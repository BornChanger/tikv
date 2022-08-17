// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains all structs related to apply batch system.
//!
//! After being started, each thread will have its own `ApplyPoller` and poll
//! using `ApplyContext`. For more information, see the documentation of
//! batch-system.

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use std::collections::VecDeque;
use std::sync::Mutex;

use batch_system::{BasicMailbox, BatchRouter, BatchSystem, HandleResult, HandlerBuilder, PollHandler, Priority};
use engine_traits::{KvEngine, PerfContextKind, RaftEngine, Snapshot, SstMetaInfo};
use raftstore::store::{fsm::{
    apply::{ControlFsm, ControlMsg},
    ApplyNotifier,
}, util::LatencyInspector, Config, RegionTask};
use slog::Logger;
use time::Duration;
use collections::HashMap;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::fsm::apply::{ApplyDelegate, Notifier};
use raftstore::store::fsm::{ApplyMetrics, ExecResult};
use tikv_util::config::{Tracker, VersionTrack};
use tikv_util::time::Instant;
use tikv_util::worker::Scheduler;

use crate::{
    fsm::{ApplyFsm, ApplyFsmDelegate},
    raft::{Apply, Peer},
    router::ApplyTask,
};

use pd_client::BucketStat;

#[derive(Debug)]
pub struct ApplyRes<S>
    where
        S: Snapshot,
{
    pub region_id: u64,
    pub apply_state: RaftApplyState,
    pub applied_term: u64,
    pub exec_res: VecDeque<ExecResult<S>>,
    pub metrics: ApplyMetrics,
    pub bucket_stat: Option<Box<BucketStat>>,
}

pub struct ApplyContext<EK>
    where
        EK: KvEngine,
{
    tag: String,
    timer: Option<Instant>,
    host: CoprocessorHost<EK>,
    importer: Arc<SstImporter>,
    region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    router: ApplyRouter<EK>,
    notifier: Box<dyn Notifier<EK>>,
    engine: EK,
    applied_batch: ApplyCallbackBatch<EK::Snapshot>,
    apply_res: Vec<ApplyRes<EK::Snapshot>>,
    exec_log_index: u64,
    exec_log_term: u64,

    kv_wb: EK::WriteBatch,
    kv_wb_last_bytes: u64,
    kv_wb_last_keys: u64,

    committed_count: usize,

    // Whether synchronize WAL is preferred.
    sync_log_hint: bool,
    // Whether to use the delete range API instead of deleting one by one.
    use_delete_range: bool,

    perf_context: EK::PerfContext,

    store_id: u64,
    /// region_id -> (peer_id, is_splitting)
    /// Used for handling race between splitting and creating new peer.
    /// An uninitialized peer can be replaced to the one from splitting iff they
    /// are exactly the same peer.
    pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,

    /// We must delete the ingested file before calling `callback` so that any
    /// ingest-request reaching this peer could see this update if leader
    /// had changed. We must also delete them after the applied-index
    /// has been persisted to kvdb because this entry may replay because of
    /// panic or power-off, which happened before `WriteBatch::write` and
    /// after `SstImporter::delete`. We shall make sure that this entry will
    /// never apply again at first, then we can delete the ssts files.
    delete_ssts: Vec<SstMetaInfo>,

    /// The priority of this Handler.
    priority: Priority,

    /// The ssts waiting to be ingested in `write_to_db`.
    pending_ssts: Vec<SstMetaInfo>,

    /// The pending inspector should be cleaned at the end of a write.
    pending_latency_inspect: Vec<LatencyInspector>,

    key_buffer: Vec<u8>,
}

impl<EK> ApplyContext<EK>
    where
        EK: KvEngine,
{
    pub fn new(
        tag: String,
        host: CoprocessorHost<EK>,
        importer: Arc<SstImporter>,
        region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
        engine: EK,
        router: ApplyRouter<EK>,
        notifier: Box<dyn Notifier<EK>>,
        cfg: &Config,
        store_id: u64,
        pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
        priority: Priority,
    ) -> ApplyContext<EK> {
        let kv_wb = engine.write_batch_with_cap(DEFAULT_APPLY_WB_SIZE);

        ApplyContext {
            tag,
            timer: None,
            host,
            importer,
            region_scheduler,
            engine: engine.clone(),
            router,
            notifier,
            kv_wb,
            applied_batch: ApplyCallbackBatch::new(),
            apply_res: vec![],
            exec_log_index: 0,
            exec_log_term: 0,
            kv_wb_last_bytes: 0,
            kv_wb_last_keys: 0,
            committed_count: 0,
            sync_log_hint: false,
            use_delete_range: cfg.use_delete_range,
            delete_ssts: vec![],
            store_id,
            pending_create_peers,
            priority,
            pending_ssts: vec![],
            pending_latency_inspect: vec![],
            key_buffer: Vec::with_capacity(1024),
        }
    }

    /// Prepares for applying entries for `delegate`.
    ///
    /// A general apply progress for a delegate is:
    /// `prepare_for` -> `commit` [-> `commit` ...] -> `finish_for`.
    /// After all delegates are handled, `write_to_db` method should be called.
    pub fn prepare_for(&mut self, delegate: &mut ApplyDelegate<EK>) {
        self.applied_batch
            .push_batch(&delegate.observe_info, delegate.region.get_id());
    }

    /// Commits all changes have done for delegate. `persistent` indicates
    /// whether write the changes into rocksdb.
    ///
    /// This call is valid only when it's between a `prepare_for` and
    /// `finish_for`.
    pub fn commit(&mut self, delegate: &mut ApplyDelegate<EK>) {
        if delegate.last_flush_applied_index < delegate.apply_state.get_applied_index() {
            delegate.write_apply_state(self.kv_wb_mut());
        }
        self.commit_opt(delegate, true);
    }

    fn commit_opt(&mut self, delegate: &mut ApplyDelegate<EK>, persistent: bool) {
        delegate.update_metrics(self);
        if persistent {
            self.write_to_db();
            self.prepare_for(delegate);
            delegate.last_flush_applied_index = delegate.apply_state.get_applied_index()
        }
        self.kv_wb_last_bytes = self.kv_wb().data_size() as u64;
        self.kv_wb_last_keys = self.kv_wb().count() as u64;
    }

    /// Writes all the changes into RocksDB.
    /// If it returns true, all pending writes are persisted in engines.
    pub fn write_to_db(&mut self) -> bool {
        let need_sync = self.sync_log_hint;
        // There may be put and delete requests after ingest request in the same fsm.
        // To guarantee the correct order, we must ingest the pending_sst first, and
        // then persist the kv write batch to engine.
        if !self.pending_ssts.is_empty() {
            let tag = self.tag.clone();
            self.importer
                .ingest(&self.pending_ssts, &self.engine)
                .unwrap_or_else(|e| {
                    panic!(
                        "{} failed to ingest ssts {:?}: {:?}",
                        tag, self.pending_ssts, e
                    );
                });
            self.pending_ssts = vec![];
        }
        if !self.kv_wb_mut().is_empty() {
            self.perf_context.start_observe();
            let mut write_opts = engine_traits::WriteOptions::new();
            write_opts.set_sync(need_sync);
            self.kv_wb().write_opt(&write_opts).unwrap_or_else(|e| {
                panic!("failed to write to engine: {:?}", e);
            });
            let trackers: Vec<_> = self
                .applied_batch
                .cb_batch
                .iter()
                .flat_map(|(cb, _)| cb.get_trackers())
                .flat_map(|trackers| trackers.iter().map(|t| t.as_tracker_token()))
                .flatten()
                .collect();
            self.perf_context.report_metrics(&trackers);
            self.sync_log_hint = false;
            let data_size = self.kv_wb().data_size();
            if data_size > APPLY_WB_SHRINK_SIZE {
                // Control the memory usage for the WriteBatch.
                self.kv_wb = self.engine.write_batch_with_cap(DEFAULT_APPLY_WB_SIZE);
            } else {
                // Clear data, reuse the WriteBatch, this can reduce memory allocations and
                // deallocations.
                self.kv_wb_mut().clear();
            }
            self.kv_wb_last_bytes = 0;
            self.kv_wb_last_keys = 0;
        }
        if !self.delete_ssts.is_empty() {
            let tag = self.tag.clone();
            for sst in self.delete_ssts.drain(..) {
                self.importer.delete(&sst.meta).unwrap_or_else(|e| {
                    panic!("{} cleanup ingested file {:?}: {:?}", tag, sst, e);
                });
            }
        }
        // Take the applied commands and their callback
        let ApplyCallbackBatch {
            cmd_batch,
            batch_max_level,
            mut cb_batch,
        } = mem::replace(&mut self.applied_batch, ApplyCallbackBatch::new());
        // Call it before invoking callback for preventing Commit is executed before
        // Prewrite is observed.
        self.host
            .on_flush_applied_cmd_batch(batch_max_level, cmd_batch, &self.engine);
        // Invoke callbacks
        let now = std::time::Instant::now();
        for (cb, resp) in cb_batch.drain(..) {
            for tracker in cb.get_trackers().iter().flat_map(|v| *v) {
                tracker.observe(now, &self.apply_time, |t| &mut t.metrics.apply_time_nanos);
            }
            cb.invoke_with_response(resp);
        }
        self.apply_time.flush();
        self.apply_wait.flush();
        need_sync
    }

    /// Finishes `Apply`s for the delegate.
    pub fn finish_for(
        &mut self,
        delegate: &mut ApplyDelegate<EK>,
        results: VecDeque<ExecResult<EK::Snapshot>>,
    ) {
        if !delegate.pending_remove {
            delegate.write_apply_state(self.kv_wb_mut());
        }
        self.commit_opt(delegate, false);
        self.apply_res.push(ApplyRes {
            region_id: delegate.region_id(),
            apply_state: delegate.apply_state.clone(),
            exec_res: results,
            metrics: delegate.metrics.clone(),
            applied_term: delegate.applied_term,
            bucket_stat: delegate.buckets.clone().map(Box::new),
        });
    }

    pub fn delta_bytes(&self) -> u64 {
        self.kv_wb().data_size() as u64 - self.kv_wb_last_bytes
    }

    pub fn delta_keys(&self) -> u64 {
        self.kv_wb().count() as u64 - self.kv_wb_last_keys
    }

    #[inline]
    pub fn kv_wb(&self) -> &EK::WriteBatch {
        &self.kv_wb
    }

    #[inline]
    pub fn kv_wb_mut(&mut self) -> &mut EK::WriteBatch {
        &mut self.kv_wb
    }

    /// Flush all pending writes to engines.
    /// If it returns true, all pending writes are persisted in engines.
    pub fn flush(&mut self) -> bool {
        // TODO: this check is too hacky, need to be more verbose and less buggy.
        let t = match self.timer.take() {
            Some(t) => t,
            None => return false,
        };

        // Write to engine
        // raftstore.sync-log = true means we need prevent data loss when power failure.
        // take raft log gc for example, we write kv WAL first, then write raft WAL,
        // if power failure happen, raft WAL may synced to disk, but kv WAL may not.
        // so we use sync-log flag here.
        let is_synced = self.write_to_db();

        if !self.apply_res.is_empty() {
            fail_point!("before_nofity_apply_res");
            let apply_res = mem::take(&mut self.apply_res);
            self.notifier.notify(apply_res);
        }

        let elapsed = t.saturating_elapsed();
        STORE_APPLY_LOG_HISTOGRAM.observe(duration_to_sec(elapsed) as f64);
        for mut inspector in std::mem::take(&mut self.pending_latency_inspect) {
            inspector.record_apply_process(elapsed);
            inspector.finish();
        }

        slow_log!(
            elapsed,
            "{} handle ready {} committed entries",
            self.tag,
            self.committed_count
        );
        self.committed_count = 0;
        is_synced
    }
}

pub struct ApplyPoller {
    apply_task_buf: Vec<ApplyTask>,
    pending_latency_inspect: Vec<LatencyInspector>,
    apply_ctx: ApplyContext,
    cfg_tracker: Tracker<Config>,
}

impl ApplyPoller {
    pub fn new(apply_ctx: ApplyContext, cfg_tracker: Tracker<Config>) -> ApplyPoller {
        Self {
            apply_task_buf: Vec::new(),
            pending_latency_inspect: Vec::new(),
            apply_ctx,
            cfg_tracker,
        }
    }

    /// Updates the internal buffer to match the latest configuration.
    fn apply_buf_capacity(&mut self) {
        let new_cap = self.messages_per_tick();
        tikv_util::set_vec_capacity(&mut self.apply_task_buf, new_cap);
    }

    #[inline]
    fn messages_per_tick(&self) -> usize {
        self.apply_ctx.cfg.messages_per_tick
    }
}

impl<EK> PollHandler<ApplyFsm<EK>, ControlFsm> for ApplyPoller
where
    EK: KvEngine,
{
    fn begin<F>(&mut self, _batch_size: usize, update_cfg: F)
    where
        for<'a> F: FnOnce(&'a batch_system::Config),
    {
        let cfg = self.cfg_tracker.any_new().map(|c| c.clone());
        if let Some(cfg) = cfg {
            let last_messages_per_tick = self.messages_per_tick();
            self.apply_ctx.cfg = cfg;
            if self.apply_ctx.cfg.messages_per_tick != last_messages_per_tick {
                self.apply_buf_capacity();
            }
            update_cfg(&self.apply_ctx.cfg.apply_batch_system);
        }
    }

    fn handle_control(&mut self, control: &mut ControlFsm) -> Option<usize> {
        control.handle_messages(&mut self.pending_latency_inspect);
        for inspector in self.pending_latency_inspect.drain(..) {
            // TODO: support apply duration.
            inspector.finish();
        }
        Some(0)
    }

    fn handle_normal(
        &mut self,
        normal: &mut impl DerefMut<Target = ApplyFsm<EK>>,
    ) -> batch_system::HandleResult {
        let received_cnt = normal.recv(&mut self.apply_task_buf);
        let handle_result = if received_cnt == self.messages_per_tick() {
            HandleResult::KeepProcessing
        } else {
            HandleResult::stop_at(0, false)
        };
        let mut delegate = ApplyFsmDelegate::new(normal, &mut self.apply_ctx);
        delegate.handle_msgs(&mut self.apply_task_buf);
        handle_result
    }

    fn end(&mut self, batch: &mut [Option<impl DerefMut<Target = ApplyFsm<EK>>>]) {
        // TODO: support memory trace
    }
}

pub struct ApplyPollerBuilder {
    cfg: Arc<VersionTrack<Config>>,
}

impl ApplyPollerBuilder {
    pub fn new(cfg: Arc<VersionTrack<Config>>) -> Self {
        Self { cfg }
    }
}

impl<EK: KvEngine> HandlerBuilder<ApplyFsm<EK>, ControlFsm> for ApplyPollerBuilder {
    type Handler = ApplyPoller;

    fn build(&mut self, priority: batch_system::Priority) -> Self::Handler {
        let apply_ctx = ApplyContext::new(self.cfg.value().clone());
        let cfg_tracker = self.cfg.clone().tracker("apply".to_string());
        ApplyPoller::new(apply_ctx, cfg_tracker)
    }
}

/// Batch system for applying logs pipeline.
pub struct ApplySystem<EK: KvEngine> {
    system: BatchSystem<ApplyFsm<EK>, ControlFsm>,
}

impl<EK: KvEngine> Deref for ApplySystem<EK> {
    type Target = BatchSystem<ApplyFsm<EK>, ControlFsm>;

    fn deref(&self) -> &BatchSystem<ApplyFsm<EK>, ControlFsm> {
        &self.system
    }
}

impl<EK: KvEngine> DerefMut for ApplySystem<EK> {
    fn deref_mut(&mut self) -> &mut BatchSystem<ApplyFsm<EK>, ControlFsm> {
        &mut self.system
    }
}

impl<EK: KvEngine> ApplySystem<EK> {
    pub fn schedule_all<'a, ER: RaftEngine>(&self, peers: impl Iterator<Item = &'a Peer<EK, ER>>) {
        let mut mailboxes = Vec::with_capacity(peers.size_hint().0);
        for peer in peers {
            let apply = Apply::new(peer);
            let (tx, fsm) = ApplyFsm::new(apply);
            mailboxes.push((
                peer.region_id(),
                BasicMailbox::new(tx, fsm, self.router().state_cnt().clone()),
            ));
        }
        self.router().register_all(mailboxes);
    }
}

pub type ApplyRouter<EK> = BatchRouter<ApplyFsm<EK>, ControlFsm>;

pub fn create_apply_batch_system<EK: KvEngine>(cfg: &Config) -> (ApplyRouter<EK>, ApplySystem<EK>) {
    let (control_tx, control_fsm) = ControlFsm::new();
    let (router, system) =
        batch_system::create_system(&cfg.apply_batch_system, control_tx, control_fsm);
    let system = ApplySystem { system };
    (router, system)
}
