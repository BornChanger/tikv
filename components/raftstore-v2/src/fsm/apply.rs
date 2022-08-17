// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use batch_system::{Fsm, Priority};
use crossbeam::channel::TryRecvError;
use fail::fail_point;
use engine_traits::{CF_RAFT, KvEngine, RaftEngineReadOnly};
use pd_client::BucketStat;
use raftstore::coprocessor::{Cmd, CmdObserveInfo, RegionState};
use raftstore::store::{entry_storage, PeerMsg, SignificantMsg, util};
use raftstore::store::fsm::apply::{ApplyResult, PendingCmdQueue};
use raftstore::store::fsm::{ApplyMetrics, CatchUpLogs, ExecResult};
use raftstore::store::metrics::PEER_ADMIN_CMD_COUNTER;
use raftstore::store::util::ConfChangeKind;
use tikv_util::{box_err, debug, error, info};
use tikv_util::mpsc::{self, LooseBoundedSender, Receiver};
use tikv_util::time::Instant;

use crate::{batch::ApplyContext, raft::Apply, router::ApplyTask};

pub struct ApplyFsm<EK: KvEngine> {
    apply: Apply<EK>,
    receiver: Receiver<ApplyTask>,
    is_stopped: bool,
}

impl<EK: KvEngine> ApplyFsm<EK> {
    pub fn new(apply: Apply<EK>) -> (LooseBoundedSender<ApplyTask>, Box<Self>) {
        let (tx, rx) = mpsc::loose_bounded(usize::MAX);
        (
            tx,
            Box::new(Self {
                apply,
                receiver: rx,
                is_stopped: false,
            }),
        )
    }

    /// Fetches messages to `apply_task_buf`. It will stop when the buffer
    /// capacity is reached or there is no more pending messages.
    ///
    /// Returns how many messages are fetched.
    pub fn recv(&mut self, apply_task_buf: &mut Vec<ApplyTask>) -> usize {
        let l = apply_task_buf.len();
        for i in l..apply_task_buf.capacity() {
            match self.receiver.try_recv() {
                Ok(msg) => apply_task_buf.push(msg),
                Err(e) => {
                    if let TryRecvError::Disconnected = e {
                        self.is_stopped = true;
                    }
                    return i - l;
                }
            }
        }
        apply_task_buf.capacity() - l
    }
}

impl<EK: KvEngine> Fsm for ApplyFsm<EK> {
    type Message = ApplyTask;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.is_stopped
    }
}

pub struct ApplyFsmDelegate<'a, EK: KvEngine> {
    fsm: &'a mut ApplyFsm<EK>,
    apply_ctx: &'a mut ApplyContext,
}

impl<'a, EK: KvEngine> ApplyFsmDelegate<'a, EK> {
    pub fn new(fsm: &'a mut ApplyFsm<EK>, apply_ctx: &'a mut ApplyContext) -> Self {
        Self { fsm, apply_ctx }
    }

    pub fn handle_msgs(&self, apply_task_buf: &mut Vec<ApplyTask>) {
        for task in apply_task_buf.drain(..) {
            // TODO: handle the tasks.
        }
    }
}

/// The apply delegate of a Region which is responsible for handling committed
/// raft log entries of a Region.
///
/// `Apply` is a term of Raft, which means executing the actual commands.
/// In Raft, once some log entries are committed, for every peer of the Raft
/// group will apply the logs one by one. For write commands, it does write or
/// delete to local engine; for admin commands, it does some meta change of the
/// Raft group.
///
/// `Delegate` is just a structure to congregate all apply related fields of a
/// Region. The apply worker receives all the apply tasks of different Regions
/// located at this store, and it will get the corresponding apply delegate to
/// handle the apply task to make the code logic more clear.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Delegate<EK>
    where
        EK: KvEngine,
{
    /// The ID of the peer.
    id: u64,
    /// The term of the Region.
    term: u64,
    /// The Region information of the peer.
    region: Region,
    /// Peer_tag, "[region region_id] peer_id".
    tag: String,

    /// If the delegate should be stopped from polling.
    /// A delegate can be stopped in conf change, merge or requested by destroy
    /// message.
    stopped: bool,
    /// The start time of the current round to execute commands.
    handle_start: Option<Instant>,
    /// Set to true when removing itself because of
    /// `ConfChangeType::RemoveNode`, and then any following committed logs
    /// in same Ready should be applied failed.
    pending_remove: bool,

    /// The commands waiting to be committed and applied
    pending_cmds: PendingCmdQueue<EK::Snapshot>,
    /// The counter of pending request snapshots. See more in `Peer`.
    pending_request_snapshot_count: Arc<AtomicUsize>,

    /// Indicates the peer is in merging, if that compact log won't be
    /// performed.
    is_merging: bool,
    /// Records the epoch version after the last merge.
    last_merge_version: u64,
    /// A temporary state that keeps track of the progress of the source peer
    /// state when CommitMerge is unable to be executed.
    wait_merge_state: Option<WaitSourceMergeState>,
    // ID of last region that reports ready.
    ready_source_region_id: u64,

    /// TiKV writes apply_state to KV RocksDB, in one write batch together with
    /// kv data.
    ///
    /// If we write it to Raft RocksDB, apply_state and kv data (Put, Delete)
    /// are in separate WAL file. When power failure, for current raft log,
    /// apply_index may synced to file, but KV data may not synced to file,
    /// so we will lose data.
    apply_state: RaftApplyState,
    /// The term of the raft log at applied index.
    applied_term: u64,
    /// The latest flushed applied index.
    last_flush_applied_index: u64,

    /// Info about cmd observer.
    observe_info: CmdObserveInfo,

    /// The local metrics, and it will be flushed periodically.
    metrics: ApplyMetrics,

    /// Priority in batch system. When applying some commands which have high
    /// latency, we decrease the priority of current fsm to reduce the
    /// impact on other normal commands.
    priority: Priority,

    /// To fetch Raft entries for applying if necessary.
    #[derivative(Debug = "ignore")]
    raft_engine: Box<dyn RaftEngineReadOnly>,

    trace: ApplyMemoryTrace,

    buckets: Option<BucketStat>,
}

impl<EK> ApplyDelegate<EK>
    where
        EK: KvEngine,
{
    fn from_registration(reg: Registration) -> ApplyDelegate<EK> {
        ApplyDelegate {
            id: reg.id,
            tag: format!("[region {}] {}", reg.region.get_id(), reg.id),
            region: reg.region,
            pending_remove: false,
            last_flush_applied_index: reg.apply_state.get_applied_index(),
            apply_state: reg.apply_state,
            applied_term: reg.applied_term,
            term: reg.term,
            stopped: false,
            handle_start: None,
            ready_source_region_id: 0,
            wait_merge_state: None,
            is_merging: reg.is_merging,
            pending_cmds: PendingCmdQueue::new(),
            metrics: Default::default(),
            last_merge_version: 0,
            pending_request_snapshot_count: reg.pending_request_snapshot_count,
            // use a default `CmdObserveInfo` because observing is disable by default
            observe_info: CmdObserveInfo::default(),
            priority: Priority::Normal,
            raft_engine: reg.raft_engine,
            trace: ApplyMemoryTrace::default(),
            buckets: None,
        }
    }

    pub fn region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Handles all the committed_entries, namely, applies the committed
    /// entries.
    fn handle_raft_committed_entries(
        &mut self,
        apply_ctx: &mut ApplyContext<EK>,
        mut committed_entries_drainer: Drain<'_, Entry>,
    ) {
        if committed_entries_drainer.len() == 0 {
            return;
        }
        apply_ctx.prepare_for(self);
        // If we send multiple ConfChange commands, only first one will be proposed
        // correctly, others will be saved as a normal entry with no data, so we
        // must re-propose these commands again.
        apply_ctx.committed_count += committed_entries_drainer.len();
        let mut results = VecDeque::new();
        while let Some(entry) = committed_entries_drainer.next() {
            if self.pending_remove {
                // This peer is about to be destroyed, skip everything.
                break;
            }

            let expect_index = self.apply_state.get_applied_index() + 1;
            if expect_index != entry.get_index() {
                panic!(
                    "{} expect index {}, but got {}, ctx {}",
                    self.tag,
                    expect_index,
                    entry.get_index(),
                    apply_ctx.tag,
                );
            }

            // NOTE: before v5.0, `EntryType::EntryConfChangeV2` entry is handled by
            // `unimplemented!()`, which can break compatibility (i.e. old version tikv
            // running on data written by new version tikv), but PD will reject old version
            // tikv join the cluster, so this should not happen.
            let res = match entry.get_entry_type() {
                EntryType::EntryNormal => self.handle_raft_entry_normal(apply_ctx, &entry),
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_raft_entry_conf_change(apply_ctx, &entry)
                }
            };

            match res {
                ApplyResult::None => {}
                ApplyResult::Res(res) => results.push_back(res),
                ApplyResult::Yield | ApplyResult::WaitMergeSource(_) => {
                    // Both cancel and merge will yield current processing.
                    apply_ctx.committed_count -= committed_entries_drainer.len() + 1;
                    let mut pending_entries =
                        Vec::with_capacity(committed_entries_drainer.len() + 1);
                    // Note that current entry is skipped when yield.
                    pending_entries.push(entry);
                    pending_entries.extend(committed_entries_drainer);
                    apply_ctx.finish_for(self, results);
                    if let ApplyResult::WaitMergeSource(logs_up_to_date) = res {
                        self.wait_merge_state = Some(WaitSourceMergeState { logs_up_to_date });
                    }
                    return;
                }
            }
        }
        apply_ctx.finish_for(self, results);

        if self.pending_remove {
            self.destroy(apply_ctx);
        }
    }

    fn update_metrics(&mut self, apply_ctx: &ApplyContext<EK>) {
        self.metrics.written_bytes += apply_ctx.delta_bytes();
        self.metrics.written_keys += apply_ctx.delta_keys();
    }

    fn write_apply_state(&self, wb: &mut EK::WriteBatch) {
        wb.put_msg_cf(
            CF_RAFT,
            &keys::apply_state_key(self.region.get_id()),
            &self.apply_state,
        )
            .unwrap_or_else(|e| {
                panic!(
                    "{} failed to save apply state to write batch, error: {:?}",
                    self.tag, e
                );
            });
    }

    fn handle_raft_entry_normal(
        &mut self,
        apply_ctx: &mut ApplyContext<EK>,
        entry: &Entry,
    ) -> ApplyResult<EK::Snapshot> {
        fail_point!(
            "yield_apply_first_region",
            self.region.get_start_key().is_empty() && !self.region.get_end_key().is_empty(),
            |_| ApplyResult::Yield
        );

        let index = entry.get_index();
        let term = entry.get_term();
        let data = entry.get_data();

        if !data.is_empty() {
            let cmd = util::parse_data_at(data, index, &self.tag);

            if apply_ctx.yield_high_latency_operation && has_high_latency_operation(&cmd) {
                self.priority = Priority::Low;
            }
            let mut has_unflushed_data =
                self.last_flush_applied_index != self.apply_state.get_applied_index();
            if has_unflushed_data && should_write_to_engine(&cmd)
                || apply_ctx.kv_wb().should_write_to_engine()
            {
                apply_ctx.commit(self);
                if let Some(start) = self.handle_start.as_ref() {
                    if start.saturating_elapsed() >= apply_ctx.yield_duration {
                        return ApplyResult::Yield;
                    }
                }
                has_unflushed_data = false;
            }
            if self.priority != apply_ctx.priority {
                if has_unflushed_data {
                    apply_ctx.commit(self);
                }
                return ApplyResult::Yield;
            }

            return self.process_raft_cmd(apply_ctx, index, term, cmd);
        }

        // we should observe empty cmd, aka leader change,
        // read index during confchange, or other situations.
        apply_ctx.host.on_empty_cmd(&self.region, index, term);

        self.apply_state.set_applied_index(index);
        self.applied_term = term;
        assert!(term > 0);

        // 1. When a peer become leader, it will send an empty entry.
        // 2. When a leader tries to read index during transferring leader,
        //    it will also propose an empty entry. But that entry will not contain
        //    any associated callback. So no need to clear callback.
        while let Some(mut cmd) = self.pending_cmds.pop_normal(u64::MAX, term - 1) {
            if let Some(cb) = cmd.cb.take() {
                apply_ctx
                    .applied_batch
                    .push_cb(cb, cmd_resp::err_resp(Error::StaleCommand, term));
            }
        }
        ApplyResult::None
    }

    fn handle_raft_entry_conf_change(
        &mut self,
        apply_ctx: &mut ApplyContext<EK>,
        entry: &Entry,
    ) -> ApplyResult<EK::Snapshot> {
        // Although conf change can't yield in normal case, it is convenient to
        // simulate yield before applying a conf change log.
        fail_point!("yield_apply_conf_change_3", self.id() == 3, |_| {
            ApplyResult::Yield
        });
        let (index, term) = (entry.get_index(), entry.get_term());
        let conf_change: ConfChangeV2 = match entry.get_entry_type() {
            EntryType::EntryConfChange => {
                let conf_change: ConfChange =
                    util::parse_data_at(entry.get_data(), index, &self.tag);
                conf_change.into_v2()
            }
            EntryType::EntryConfChangeV2 => util::parse_data_at(entry.get_data(), index, &self.tag),
            _ => unreachable!(),
        };
        let cmd = util::parse_data_at(conf_change.get_context(), index, &self.tag);
        match self.process_raft_cmd(apply_ctx, index, term, cmd) {
            ApplyResult::None => {
                // If failed, tell Raft that the `ConfChange` was aborted.
                ApplyResult::Res(ExecResult::ChangePeer(Default::default()))
            }
            ApplyResult::Res(mut res) => {
                if let ExecResult::ChangePeer(ref mut cp) = res {
                    cp.conf_change = conf_change;
                } else {
                    panic!(
                        "{} unexpected result {:?} for conf change {:?} at {}",
                        self.tag, res, conf_change, index
                    );
                }
                ApplyResult::Res(res)
            }
            ApplyResult::Yield | ApplyResult::WaitMergeSource(_) => unreachable!(),
        }
    }

    fn find_pending(
        &mut self,
        index: u64,
        term: u64,
        is_conf_change: bool,
    ) -> Option<Callback<EK::Snapshot>> {
        let (region_id, peer_id) = (self.region_id(), self.id());
        if is_conf_change {
            if let Some(mut cmd) = self.pending_cmds.take_conf_change() {
                if cmd.index == index && cmd.term == term {
                    return Some(cmd.cb.take().unwrap());
                } else {
                    notify_stale_command(region_id, peer_id, self.term, cmd);
                }
            }
            return None;
        }
        while let Some(mut head) = self.pending_cmds.pop_normal(index, term) {
            if head.term == term {
                if head.index == index {
                    return Some(head.cb.take().unwrap());
                } else {
                    panic!(
                        "{} unexpected callback at term {}, found index {}, expected {}",
                        self.tag, term, head.index, index
                    );
                }
            } else {
                // Because of the lack of original RaftCmdRequest, we skip calling
                // coprocessor here.
                notify_stale_command(region_id, peer_id, self.term, head);
            }
        }
        None
    }

    fn process_raft_cmd(
        &mut self,
        apply_ctx: &mut ApplyContext<EK>,
        index: u64,
        term: u64,
        cmd: RaftCmdRequest,
    ) -> ApplyResult<EK::Snapshot> {
        if index == 0 {
            panic!(
                "{} processing raft command needs a none zero index",
                self.tag
            );
        }

        // Set sync log hint if the cmd requires so.
        apply_ctx.sync_log_hint |= should_sync_log(&cmd);

        apply_ctx.host.pre_apply(&self.region, &cmd);
        let (mut resp, exec_result, should_write) =
            self.apply_raft_cmd(apply_ctx, index, term, &cmd);
        if let ApplyResult::WaitMergeSource(_) = exec_result {
            return exec_result;
        }

        debug!(
            "applied command";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "index" => index
        );

        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        cmd_resp::bind_term(&mut resp, self.term);
        let cmd_cb = self.find_pending(index, term, is_conf_change_cmd(&cmd));
        let cmd = Cmd::new(index, term, cmd, resp);
        apply_ctx
            .applied_batch
            .push(cmd_cb, cmd, &self.observe_info, self.region_id());
        if should_write {
            debug!("persist data and apply state"; "region_id" => self.region_id(), "peer_id" => self.id(), "state" => ?self.apply_state);
            apply_ctx.commit(self);
        }
        exec_result
    }

    /// Applies raft command.
    ///
    /// An apply operation can fail in the following situations:
    ///   - it encounters an error that will occur on all stores, it can
    /// continue applying next entry safely, like epoch not match for
    /// example;
    ///   - it encounters an error that may not occur on all stores, in this
    ///     case we should try to apply the entry again or panic. Considering
    ///     that this usually due to disk operation fail, which is rare, so just
    ///     panic is ok.
    fn apply_raft_cmd(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        index: u64,
        term: u64,
        req: &RaftCmdRequest,
    ) -> (RaftCmdResponse, ApplyResult<EK::Snapshot>, bool) {
        // if pending remove, apply should be aborted already.
        assert!(!self.pending_remove);

        // Remember if the raft cmd fails to be applied, it must have no side effects.
        // E.g. `RaftApplyState` must not be changed.

        let mut origin_epoch = None;
        let (resp, exec_result) = if ctx.host.pre_exec(&self.region, req, index, term) {
            // One of the observers want to filter execution of the command.
            let mut resp = RaftCmdResponse::default();
            if !req.get_header().get_uuid().is_empty() {
                let uuid = req.get_header().get_uuid().to_vec();
                resp.mut_header().set_uuid(uuid);
            }
            (resp, ApplyResult::None)
        } else {
            ctx.exec_log_index = index;
            ctx.exec_log_term = term;
            ctx.kv_wb_mut().set_save_point();
            let (resp, exec_result) = match self.exec_raft_cmd(ctx, req) {
                Ok(a) => {
                    ctx.kv_wb_mut().pop_save_point().unwrap();
                    if req.has_admin_request() {
                        origin_epoch = Some(self.region.get_region_epoch().clone());
                    }
                    a
                }
                Err(e) => {
                    // clear dirty values.
                    ctx.kv_wb_mut().rollback_to_save_point().unwrap();
                    match e {
                        Error::EpochNotMatch(..) => debug!(
                            "epoch not match";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "err" => ?e
                        ),
                        _ => error!(?e;
                            "execute raft command";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                        ),
                    }
                    (cmd_resp::new_error(e), ApplyResult::None)
                }
            };
            (resp, exec_result)
        };
        if let ApplyResult::WaitMergeSource(_) = exec_result {
            return (resp, exec_result, false);
        }

        self.apply_state.set_applied_index(index);
        self.applied_term = term;

        let cmd = Cmd::new(index, term, req.clone(), resp.clone());
        let should_write = ctx.host.post_exec(
            &self.region,
            &cmd,
            &self.apply_state,
            &RegionState {
                peer_id: self.id(),
                pending_remove: self.pending_remove,
                modified_region: match exec_result {
                    ApplyResult::Res(ref e) => match e {
                        ExecResult::SplitRegion { ref derived, .. } => Some(derived.clone()),
                        ExecResult::PrepareMerge { ref region, .. } => Some(region.clone()),
                        ExecResult::CommitMerge { ref region, .. } => Some(region.clone()),
                        ExecResult::RollbackMerge { ref region, .. } => Some(region.clone()),
                        _ => None,
                    },
                    _ => None,
                },
            },
        );

        if let ApplyResult::Res(ref exec_result) = exec_result {
            match *exec_result {
                ExecResult::ChangePeer(ref cp) => {
                    self.region = cp.region.clone();
                }
                ExecResult::ComputeHash { .. }
                | ExecResult::VerifyHash { .. }
                | ExecResult::CompactLog { .. }
                | ExecResult::DeleteRange { .. }
                | ExecResult::IngestSst { .. }
                | ExecResult::TransferLeader { .. } => {}
                ExecResult::SplitRegion { ref derived, .. } => {
                    self.region = derived.clone();
                    self.metrics.size_diff_hint = 0;
                    self.metrics.delete_keys_hint = 0;
                }
                ExecResult::PrepareMerge { ref region, .. } => {
                    self.region = region.clone();
                    self.is_merging = true;
                }
                ExecResult::CommitMerge { ref region, .. } => {
                    self.region = region.clone();
                    self.last_merge_version = region.get_region_epoch().get_version();
                }
                ExecResult::RollbackMerge { ref region, .. } => {
                    self.region = region.clone();
                    self.is_merging = false;
                }
            }
        }
        if let Some(epoch) = origin_epoch {
            let cmd_type = req.get_admin_request().get_cmd_type();
            let epoch_state = admin_cmd_epoch_lookup(cmd_type);
            // The change-epoch behavior **MUST BE** equal to the settings in
            // `admin_cmd_epoch_lookup`
            if (epoch_state.change_ver
                && epoch.get_version() == self.region.get_region_epoch().get_version())
                || (epoch_state.change_conf_ver
                && epoch.get_conf_ver() == self.region.get_region_epoch().get_conf_ver())
            {
                panic!(
                    "{} apply admin cmd {:?} but epoch change is not expected, epoch state {:?}, before {:?}, after {:?}",
                    self.tag,
                    req,
                    epoch_state,
                    epoch,
                    self.region.get_region_epoch()
                );
            }
        }

        (resp, exec_result, should_write)
    }

    fn destroy(&mut self, apply_ctx: &mut ApplyContext<EK>) {
        self.stopped = true;
        apply_ctx.router.close(self.region_id());
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_region_removed(self.region.get_id(), self.id, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_region_removed(self.region.get_id(), self.id, cmd);
        }

        let mut event = TraceEvent::default();
        if let Some(e) = self.trace.reset(ApplyMemoryTrace::default()) {
            event = event + e;
        }
        MEMTRACE_APPLYS.trace(event);
    }

    fn clear_all_commands_as_stale(&mut self) {
        let (region_id, peer_id) = (self.region_id(), self.id());
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_stale_command(region_id, peer_id, self.term, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_stale_command(region_id, peer_id, self.term, cmd);
        }
    }

    fn clear_all_commands_silently(&mut self) {
        for mut cmd in self.pending_cmds.normals.drain(..) {
            cmd.cb.take();
        }
        if let Some(mut cmd) = self.pending_cmds.conf_change.take() {
            cmd.cb.take();
        }
    }
}

impl<EK> ApplyDelegate<EK>
    where
        EK: KvEngine,
{
    // Only errors that will also occur on all other stores should be returned.
    fn exec_raft_cmd(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, ApplyResult<EK::Snapshot>)> {
        // Include region for epoch not match after merge may cause key not in range.
        let include_region =
            req.get_header().get_region_epoch().get_version() >= self.last_merge_version;
        check_region_epoch(req, &self.region, include_region)?;
        if req.has_admin_request() {
            self.exec_admin_cmd(ctx, req)
        } else {
            self.exec_write_cmd(ctx, req)
        }
    }

    fn exec_admin_cmd(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, ApplyResult<EK::Snapshot>)> {
        let request = req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        if cmd_type != AdminCmdType::CompactLog && cmd_type != AdminCmdType::CommitMerge {
            info!(
                "execute admin command";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "term" => ctx.exec_log_term,
                "index" => ctx.exec_log_index,
                "command" => ?request,
            );
        }

        let (mut response, exec_result) = match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::ChangePeerV2 => self.exec_change_peer_v2(ctx, request),
            AdminCmdType::Split => self.exec_split(ctx, request),
            AdminCmdType::BatchSplit => self.exec_batch_split(ctx, request),
            AdminCmdType::CompactLog => self.exec_compact_log(request),
            AdminCmdType::TransferLeader => self.exec_transfer_leader(request, ctx.exec_log_term),
            AdminCmdType::ComputeHash => self.exec_compute_hash(ctx, request),
            AdminCmdType::VerifyHash => self.exec_verify_hash(ctx, request),
            // TODO: is it backward compatible to add new cmd_type?
            AdminCmdType::PrepareMerge => self.exec_prepare_merge(ctx, request),
            AdminCmdType::CommitMerge => self.exec_commit_merge(ctx, request),
            AdminCmdType::RollbackMerge => self.exec_rollback_merge(ctx, request),
            AdminCmdType::InvalidAdmin => Err(box_err!("unsupported admin command type")),
        }?;
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::default();
        if !req.get_header().get_uuid().is_empty() {
            let uuid = req.get_header().get_uuid().to_vec();
            resp.mut_header().set_uuid(uuid);
        }
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    fn exec_write_cmd(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!(
            "on_apply_write_cmd",
            cfg!(release) || self.id() == 3,
            |_| {
                unimplemented!();
            }
        );

        let requests = req.get_requests();

        let mut ranges = vec![];
        let mut ssts = vec![];
        for req in requests {
            let cmd_type = req.get_cmd_type();
            match cmd_type {
                CmdType::Put => self.handle_put(ctx, req),
                CmdType::Delete => self.handle_delete(ctx, req),
                CmdType::DeleteRange => {
                    self.handle_delete_range(&ctx.engine, req, &mut ranges, ctx.use_delete_range)
                }
                CmdType::IngestSst => self.handle_ingest_sst(ctx, req, &mut ssts),
                // Readonly commands are handled in raftstore directly.
                // Don't panic here in case there are old entries need to be applied.
                // It's also safe to skip them here, because a restart must have happened,
                // hence there is no callback to be called.
                CmdType::Snap | CmdType::Get => {
                    warn!(
                        "skip readonly command";
                        "region_id" => self.region_id(),
                        "peer_id" => self.id(),
                        "command" => ?req,
                    );
                    continue;
                }
                CmdType::Prewrite | CmdType::Invalid | CmdType::ReadIndex => {
                    Err(box_err!("invalid cmd type, message maybe corrupted"))
                }
            }?;
        }

        let mut resp = RaftCmdResponse::default();
        if !req.get_header().get_uuid().is_empty() {
            let uuid = req.get_header().get_uuid().to_vec();
            resp.mut_header().set_uuid(uuid);
        }

        assert!(ranges.is_empty() || ssts.is_empty());
        let exec_res = if !ranges.is_empty() {
            ApplyResult::Res(ExecResult::DeleteRange { ranges })
        } else if !ssts.is_empty() {
            #[cfg(feature = "failpoints")]
            {
                let mut dont_delete_ingested_sst_fp = || {
                    fail_point!("dont_delete_ingested_sst", |_| {
                        ssts.clear();
                    });
                };
                dont_delete_ingested_sst_fp();
            }
            ctx.delete_ssts.append(&mut ssts.clone());
            ApplyResult::Res(ExecResult::IngestSst { ssts })
        } else {
            ApplyResult::None
        };

        Ok((resp, exec_res))
    }
}

// Write commands related.
impl<EK> ApplyDelegate<EK>
    where
        EK: KvEngine,
{
    fn handle_put(&mut self, ctx: &mut ApplyContext<EK>, req: &Request) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.put.inc();
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, &self.region)?;
        if let Some(s) = self.buckets.as_mut() {
            s.write_key(key, value.len() as u64);
        }

        keys::data_key_with_buffer(key, &mut ctx.key_buffer);
        let key = ctx.key_buffer.as_slice();

        self.metrics.size_diff_hint += key.len() as i64;
        self.metrics.size_diff_hint += value.len() as i64;
        if !req.get_put().get_cf().is_empty() {
            let cf = req.get_put().get_cf();
            // TODO: don't allow write preseved cfs.
            if cf == CF_LOCK {
                self.metrics.lock_cf_written_bytes += key.len() as u64;
                self.metrics.lock_cf_written_bytes += value.len() as u64;
            }
            // TODO: check whether cf exists or not.
            ctx.kv_wb.put_cf(cf, key, value).unwrap_or_else(|e| {
                panic!(
                    "{} failed to write ({}, {}) to cf {}: {:?}",
                    self.tag,
                    log_wrappers::Value::key(key),
                    log_wrappers::Value::value(value),
                    cf,
                    e
                )
            });
        } else {
            ctx.kv_wb.put(key, value).unwrap_or_else(|e| {
                panic!(
                    "{} failed to write ({}, {}): {:?}",
                    self.tag,
                    log_wrappers::Value::key(key),
                    log_wrappers::Value::value(value),
                    e
                );
            });
        }
        Ok(())
    }

    fn handle_delete(&mut self, ctx: &mut ApplyContext<EK>, req: &Request) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.delete.inc();
        let key = req.get_delete().get_key();
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, &self.region)?;
        if let Some(s) = self.buckets.as_mut() {
            s.write_key(key, 0);
        }

        keys::data_key_with_buffer(key, &mut ctx.key_buffer);
        let key = ctx.key_buffer.as_slice();

        // since size_diff_hint is not accurate, so we just skip calculate the value
        // size.
        self.metrics.size_diff_hint -= key.len() as i64;
        if !req.get_delete().get_cf().is_empty() {
            let cf = req.get_delete().get_cf();
            // TODO: check whether cf exists or not.
            ctx.kv_wb.delete_cf(cf, key).unwrap_or_else(|e| {
                panic!(
                    "{} failed to delete {}: {}",
                    self.tag,
                    log_wrappers::Value::key(key),
                    e
                )
            });

            if cf == CF_LOCK {
                // delete is a kind of write for RocksDB.
                self.metrics.lock_cf_written_bytes += key.len() as u64;
            } else {
                self.metrics.delete_keys_hint += 1;
            }
        } else {
            ctx.kv_wb.delete(key).unwrap_or_else(|e| {
                panic!(
                    "{} failed to delete {}: {}",
                    self.tag,
                    log_wrappers::Value::key(key),
                    e
                )
            });
            self.metrics.delete_keys_hint += 1;
        }

        Ok(())
    }

    fn handle_delete_range(
        &mut self,
        engine: &EK,
        req: &Request,
        ranges: &mut Vec<Range>,
        use_delete_range: bool,
    ) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.delete_range.inc();
        let s_key = req.get_delete_range().get_start_key();
        let e_key = req.get_delete_range().get_end_key();
        let notify_only = req.get_delete_range().get_notify_only();
        if !e_key.is_empty() && s_key >= e_key {
            return Err(box_err!(
                "invalid delete range command, start_key: {:?}, end_key: {:?}",
                s_key,
                e_key
            ));
        }
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(s_key, &self.region)?;
        let end_key = keys::data_end_key(e_key);
        let region_end_key = keys::data_end_key(self.region.get_end_key());
        if end_key > region_end_key {
            return Err(Error::KeyNotInRegion(e_key.to_vec(), self.region.clone()));
        }

        let mut cf = req.get_delete_range().get_cf();
        if cf.is_empty() {
            cf = CF_DEFAULT;
        }
        if !ALL_CFS.iter().any(|x| *x == cf) {
            return Err(box_err!("invalid delete range command, cf: {:?}", cf));
        }

        let start_key = keys::data_key(s_key);
        // Use delete_files_in_range to drop as many sst files as possible, this
        // is a way to reclaim disk space quickly after drop a table/index.
        if !notify_only {
            let range = vec![EngineRange::new(&start_key, &end_key)];
            let fail_f = |e: engine_traits::Error, strategy: DeleteStrategy| {
                panic!(
                    "{} failed to delete {:?} in ranges [{}, {}): {:?}",
                    self.tag,
                    strategy,
                    &log_wrappers::Value::key(&start_key),
                    &log_wrappers::Value::key(&end_key),
                    e
                )
            };
            engine
                .delete_ranges_cf(cf, DeleteStrategy::DeleteFiles, &range)
                .unwrap_or_else(|e| fail_f(e, DeleteStrategy::DeleteFiles));

            let strategy = if use_delete_range {
                DeleteStrategy::DeleteByRange
            } else {
                DeleteStrategy::DeleteByKey
            };
            // Delete all remaining keys.
            engine
                .delete_ranges_cf(cf, strategy.clone(), &range)
                .unwrap_or_else(move |e| fail_f(e, strategy));
            engine
                .delete_ranges_cf(cf, DeleteStrategy::DeleteBlobs, &range)
                .unwrap_or_else(move |e| fail_f(e, DeleteStrategy::DeleteBlobs));
        }

        // TODO: Should this be executed when `notify_only` is set?
        ranges.push(Range::new(cf.to_owned(), start_key, end_key));

        Ok(())
    }

    fn handle_ingest_sst(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &Request,
        ssts: &mut Vec<SstMetaInfo>,
    ) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.ingest_sst.inc();
        let sst = req.get_ingest_sst().get_sst();

        if let Err(e) = check_sst_for_ingestion(sst, &self.region) {
            error!(?e;
                 "ingest fail";
                 "region_id" => self.region_id(),
                 "peer_id" => self.id(),
                 "sst" => ?sst,
                 "region" => ?&self.region,
            );
            // This file is not valid, we can delete it here.
            let _ = ctx.importer.delete(sst);
            return Err(e);
        }

        match ctx.importer.validate(sst) {
            Ok(meta_info) => {
                ctx.pending_ssts.push(meta_info.clone());
                ssts.push(meta_info)
            }
            Err(e) => {
                // If this failed, it means that the file is corrupted or something
                // is wrong with the engine, but we can do nothing about that.
                panic!("{} ingest {:?}: {:?}", self.tag, sst, e);
            }
        };

        Ok(())
    }
}

// Admin commands related.
impl<EK> ApplyDelegate<EK>
    where
        EK: KvEngine,
{
    fn exec_change_peer(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        assert!(request.has_change_peer());
        let request = request.get_change_peer();
        let peer = request.get_peer();
        let store_id = peer.get_store_id();
        let change_type = request.get_change_type();
        let mut region = self.region.clone();

        fail_point!(
            "apply_on_conf_change_1_3_1",
            (self.id == 1 || self.id == 3) && self.region_id() == 1,
            |_| panic!("should not use return")
        );
        fail_point!(
            "apply_on_conf_change_3_1",
            self.id == 3 && self.region_id() == 1,
            |_| panic!("should not use return")
        );
        fail_point!(
            "apply_on_conf_change_all_1",
            self.region_id() == 1,
            |_| panic!("should not use return")
        );
        info!(
            "exec ConfChange";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "type" => util::conf_change_type_str(change_type),
            "epoch" => ?region.get_region_epoch(),
        );

        // TODO: we should need more check, like peer validation, duplicated id, etc.
        let conf_ver = region.get_region_epoch().get_conf_ver() + 1;
        region.mut_region_epoch().set_conf_ver(conf_ver);

        match change_type {
            ConfChangeType::AddNode => {
                let add_ndoe_fp = || {
                    fail_point!(
                        "apply_on_add_node_1_2",
                        self.id == 2 && self.region_id() == 1,
                        |_| {}
                    )
                };
                add_ndoe_fp();

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_peer", "all"])
                    .inc();

                let mut exists = false;
                if let Some(p) = util::find_peer_mut(&mut region, store_id) {
                    exists = true;
                    if !is_learner(p) || p.get_id() != peer.get_id() {
                        error!(
                            "can't add duplicated peer";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "peer" => ?peer,
                            "region" => ?&self.region
                        );
                        return Err(box_err!(
                            "can't add duplicated peer {:?} to region {:?}",
                            peer,
                            self.region
                        ));
                    } else {
                        p.set_role(PeerRole::Voter);
                    }
                }
                if !exists {
                    // TODO: Do we allow adding peer in same node?
                    region.mut_peers().push(peer.clone());
                }

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_peer", "success"])
                    .inc();
                info!(
                    "add peer successfully";
                    "region_id" => self.region_id(),
                    "peer_id" => self.id(),
                    "peer" => ?peer,
                    "region" => ?&self.region
                );
            }
            ConfChangeType::RemoveNode => {
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["remove_peer", "all"])
                    .inc();

                if let Some(p) = util::remove_peer(&mut region, store_id) {
                    // Considering `is_learner` flag in `Peer` here is by design.
                    if &p != peer {
                        error!(
                            "ignore remove unmatched peer";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "expect_peer" => ?peer,
                            "get_peeer" => ?p
                        );
                        return Err(box_err!(
                            "remove unmatched peer: expect: {:?}, get {:?}, ignore",
                            peer,
                            p
                        ));
                    }
                    if self.id == peer.get_id() {
                        // Remove ourself, we will destroy all region data later.
                        // So we need not to apply following logs.
                        self.stopped = true;
                        self.pending_remove = true;
                    }
                } else {
                    error!(
                        "remove missing peer";
                        "region_id" => self.region_id(),
                        "peer_id" => self.id(),
                        "peer" => ?peer,
                        "region" => ?&self.region
                    );
                    return Err(box_err!(
                        "remove missing peer {:?} from region {:?}",
                        peer,
                        self.region
                    ));
                }

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["remove_peer", "success"])
                    .inc();
                info!(
                    "remove peer successfully";
                    "region_id" => self.region_id(),
                    "peer_id" => self.id(),
                    "peer" => ?peer,
                    "region" => ?&self.region
                );
            }
            ConfChangeType::AddLearnerNode => {
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_learner", "all"])
                    .inc();

                if util::find_peer(&region, store_id).is_some() {
                    error!(
                        "can't add duplicated learner";
                        "region_id" => self.region_id(),
                        "peer_id" => self.id(),
                        "peer" => ?peer,
                        "region" => ?&self.region
                    );
                    return Err(box_err!(
                        "can't add duplicated learner {:?} to region {:?}",
                        peer,
                        self.region
                    ));
                }
                region.mut_peers().push(peer.clone());

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_learner", "success"])
                    .inc();
                info!(
                    "add learner successfully";
                    "region_id" => self.region_id(),
                    "peer_id" => self.id(),
                    "peer" => ?peer,
                    "region" => ?&self.region
                );
            }
        }

        let state = if self.pending_remove {
            PeerState::Tombstone
        } else {
            PeerState::Normal
        };
        if let Err(e) = write_peer_state(ctx.kv_wb_mut(), &region, state, None) {
            panic!("{} failed to update region state: {:?}", self.tag, e);
        }

        let mut resp = AdminResponse::default();
        resp.mut_change_peer().set_region(region.clone());

        Ok((
            resp,
            ApplyResult::Res(ExecResult::ChangePeer(ChangePeer {
                index: ctx.exec_log_index,
                conf_change: Default::default(),
                changes: vec![request.clone()],
                region,
            })),
        ))
    }

    fn exec_change_peer_v2(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        assert!(request.has_change_peer_v2());
        let changes = request.get_change_peer_v2().get_change_peers().to_vec();

        info!(
            "exec ConfChangeV2";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "kind" => ?ConfChangeKind::confchange_kind(changes.len()),
            "epoch" => ?self.region.get_region_epoch(),
        );

        let region = match ConfChangeKind::confchange_kind(changes.len()) {
            ConfChangeKind::LeaveJoint => self.apply_leave_joint()?,
            kind => self.apply_conf_change(kind, changes.as_slice())?,
        };

        let state = if self.pending_remove {
            PeerState::Tombstone
        } else {
            PeerState::Normal
        };

        if let Err(e) = write_peer_state(ctx.kv_wb_mut(), &region, state, None) {
            panic!("{} failed to update region state: {:?}", self.tag, e);
        }

        let mut resp = AdminResponse::default();
        resp.mut_change_peer().set_region(region.clone());
        Ok((
            resp,
            ApplyResult::Res(ExecResult::ChangePeer(ChangePeer {
                index: ctx.exec_log_index,
                conf_change: Default::default(),
                changes,
                region,
            })),
        ))
    }

    fn apply_conf_change(
        &mut self,
        kind: ConfChangeKind,
        changes: &[ChangePeerRequest],
    ) -> Result<Region> {
        let mut region = self.region.clone();
        for cp in changes.iter() {
            let (change_type, peer) = (cp.get_change_type(), cp.get_peer());
            let store_id = peer.get_store_id();

            confchange_cmd_metric::inc_all(change_type);

            if let Some(exist_peer) = util::find_peer(&region, store_id) {
                let r = exist_peer.get_role();
                if r == PeerRole::IncomingVoter || r == PeerRole::DemotingVoter {
                    panic!(
                        "{} can't apply confchange because configuration is still in joint state, confchange: {:?}, region: {:?}",
                        self.tag, cp, self.region
                    );
                }
            }
            match (util::find_peer_mut(&mut region, store_id), change_type) {
                (None, ConfChangeType::AddNode) => {
                    let mut peer = peer.clone();
                    match kind {
                        ConfChangeKind::Simple => peer.set_role(PeerRole::Voter),
                        ConfChangeKind::EnterJoint => peer.set_role(PeerRole::IncomingVoter),
                        _ => unreachable!(),
                    }
                    region.mut_peers().push(peer);
                }
                (None, ConfChangeType::AddLearnerNode) => {
                    let mut peer = peer.clone();
                    peer.set_role(PeerRole::Learner);
                    region.mut_peers().push(peer);
                }
                (None, ConfChangeType::RemoveNode) => {
                    error!(
                        "remove missing peer";
                        "region_id" => self.region_id(),
                        "peer_id" => self.id(),
                        "peer" => ?peer,
                        "region" => ?&self.region,
                    );
                    return Err(box_err!(
                        "remove missing peer {:?} from region {:?}",
                        peer,
                        self.region
                    ));
                }
                // Add node
                (Some(exist_peer), ConfChangeType::AddNode)
                | (Some(exist_peer), ConfChangeType::AddLearnerNode) => {
                    let (role, exist_id, incoming_id) =
                        (exist_peer.get_role(), exist_peer.get_id(), peer.get_id());

                    if exist_id != incoming_id // Add peer with different id to the same store
                        // The peer is already the requested role
                        || (role, change_type) == (PeerRole::Voter, ConfChangeType::AddNode)
                        || (role, change_type) == (PeerRole::Learner, ConfChangeType::AddLearnerNode)
                    {
                        error!(
                            "can't add duplicated peer";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "peer" => ?peer,
                            "exist peer" => ?exist_peer,
                            "confchnage type" => ?change_type,
                            "region" => ?&self.region
                        );
                        return Err(box_err!(
                            "can't add duplicated peer {:?} to region {:?}, duplicated with exist peer {:?}",
                            peer,
                            self.region,
                            exist_peer
                        ));
                    }
                    match (role, change_type) {
                        (PeerRole::Voter, ConfChangeType::AddLearnerNode) => match kind {
                            ConfChangeKind::Simple => exist_peer.set_role(PeerRole::Learner),
                            ConfChangeKind::EnterJoint => {
                                exist_peer.set_role(PeerRole::DemotingVoter)
                            }
                            _ => unreachable!(),
                        },
                        (PeerRole::Learner, ConfChangeType::AddNode) => match kind {
                            ConfChangeKind::Simple => exist_peer.set_role(PeerRole::Voter),
                            ConfChangeKind::EnterJoint => {
                                exist_peer.set_role(PeerRole::IncomingVoter)
                            }
                            _ => unreachable!(),
                        },
                        _ => unreachable!(),
                    }
                }
                // Remove node
                (Some(exist_peer), ConfChangeType::RemoveNode) => {
                    if kind == ConfChangeKind::EnterJoint
                        && exist_peer.get_role() == PeerRole::Voter
                    {
                        error!(
                            "can't remove voter directly";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "peer" => ?peer,
                            "region" => ?&self.region
                        );
                        return Err(box_err!(
                            "can not remove voter {:?} directly from region {:?}",
                            peer,
                            self.region
                        ));
                    }
                    match util::remove_peer(&mut region, store_id) {
                        Some(p) => {
                            if &p != peer {
                                error!(
                                    "ignore remove unmatched peer";
                                    "region_id" => self.region_id(),
                                    "peer_id" => self.id(),
                                    "expect_peer" => ?peer,
                                    "get_peeer" => ?p
                                );
                                return Err(box_err!(
                                    "remove unmatched peer: expect: {:?}, get {:?}, ignore",
                                    peer,
                                    p
                                ));
                            }
                            if self.id == peer.get_id() {
                                // Remove ourself, we will destroy all region data later.
                                // So we need not to apply following logs.
                                self.stopped = true;
                                self.pending_remove = true;
                            }
                        }
                        None => unreachable!(),
                    }
                }
            }
            confchange_cmd_metric::inc_success(change_type);
        }
        let conf_ver = region.get_region_epoch().get_conf_ver() + changes.len() as u64;
        region.mut_region_epoch().set_conf_ver(conf_ver);
        info!(
            "conf change successfully";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "changes" => ?changes,
            "original region" => ?&self.region,
            "current region" => ?&region,
        );
        Ok(region)
    }

    fn apply_leave_joint(&self) -> Result<Region> {
        let mut region = self.region.clone();
        let mut change_num = 0;
        for peer in region.mut_peers().iter_mut() {
            match peer.get_role() {
                PeerRole::IncomingVoter => peer.set_role(PeerRole::Voter),
                PeerRole::DemotingVoter => peer.set_role(PeerRole::Learner),
                _ => continue,
            }
            change_num += 1;
        }
        if change_num == 0 {
            panic!(
                "{} can't leave a non-joint config, region: {:?}",
                self.tag, self.region
            );
        }
        let conf_ver = region.get_region_epoch().get_conf_ver() + change_num;
        region.mut_region_epoch().set_conf_ver(conf_ver);
        info!(
            "leave joint state successfully";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "region" => ?&region,
        );
        Ok(region)
    }

    fn exec_split(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        info!(
            "split is deprecated, redirect to use batch split";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
        );
        let split = req.get_split().to_owned();
        let mut admin_req = AdminRequest::default();
        admin_req
            .mut_splits()
            .set_right_derive(split.get_right_derive());
        admin_req.mut_splits().mut_requests().push(split);
        // This method is executed only when there are unapplied entries after being
        // restarted. So there will be no callback, it's OK to return a response
        // that does not matched with its request.
        self.exec_batch_split(ctx, &admin_req)
    }

    fn exec_batch_split(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!("apply_before_split");
        fail_point!(
            "apply_before_split_1_3",
            self.id == 3 && self.region_id() == 1,
            |_| { unreachable!() }
        );

        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();

        let split_reqs = req.get_splits();
        let right_derive = split_reqs.get_right_derive();
        if split_reqs.get_requests().is_empty() {
            return Err(box_err!("missing split requests"));
        }
        let mut derived = self.region.clone();
        let new_region_cnt = split_reqs.get_requests().len();
        let mut regions = Vec::with_capacity(new_region_cnt + 1);
        let mut keys: VecDeque<Vec<u8>> = VecDeque::with_capacity(new_region_cnt + 1);
        for req in split_reqs.get_requests() {
            let split_key = req.get_split_key();
            if split_key.is_empty() {
                return Err(box_err!("missing split key"));
            }
            if split_key
                <= keys
                .back()
                .map_or_else(|| derived.get_start_key(), Vec::as_slice)
            {
                return Err(box_err!("invalid split request: {:?}", split_reqs));
            }
            if req.get_new_peer_ids().len() != derived.get_peers().len() {
                return Err(box_err!(
                    "invalid new peer id count, need {:?}, but got {:?}",
                    derived.get_peers(),
                    req.get_new_peer_ids()
                ));
            }
            keys.push_back(split_key.to_vec());
        }

        util::check_key_in_region(keys.back().unwrap(), &self.region)?;

        info!(
            "split region";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "region" => ?derived,
            "keys" => %KeysInfoFormatter(keys.iter()),
        );
        let new_version = derived.get_region_epoch().get_version() + new_region_cnt as u64;
        derived.mut_region_epoch().set_version(new_version);
        // Note that the split requests only contain ids for new regions, so we need
        // to handle new regions and old region separately.
        if right_derive {
            // So the range of new regions is [old_start_key, split_key1, ...,
            // last_split_key].
            keys.push_front(derived.get_start_key().to_vec());
        } else {
            // So the range of new regions is [split_key1, ..., last_split_key,
            // old_end_key].
            keys.push_back(derived.get_end_key().to_vec());
            derived.set_end_key(keys.front().unwrap().to_vec());
            regions.push(derived.clone());
        }

        let mut new_split_regions: HashMap<u64, NewSplitPeer> = HashMap::default();
        for req in split_reqs.get_requests() {
            let mut new_region = Region::default();
            new_region.set_id(req.get_new_region_id());
            new_region.set_region_epoch(derived.get_region_epoch().to_owned());
            new_region.set_start_key(keys.pop_front().unwrap());
            new_region.set_end_key(keys.front().unwrap().to_vec());
            new_region.set_peers(derived.get_peers().to_vec().into());
            for (peer, peer_id) in new_region
                .mut_peers()
                .iter_mut()
                .zip(req.get_new_peer_ids())
            {
                peer.set_id(*peer_id);
            }
            new_split_regions.insert(
                new_region.get_id(),
                NewSplitPeer {
                    peer_id: util::find_peer(&new_region, ctx.store_id).unwrap().get_id(),
                    result: None,
                },
            );
            regions.push(new_region);
        }

        if right_derive {
            derived.set_start_key(keys.pop_front().unwrap());
            regions.push(derived.clone());
        }

        let mut replace_regions = HashSet::default();
        {
            let mut pending_create_peers = ctx.pending_create_peers.lock().unwrap();
            for (region_id, new_split_peer) in new_split_regions.iter_mut() {
                match pending_create_peers.entry(*region_id) {
                    HashMapEntry::Occupied(mut v) => {
                        if *v.get() != (new_split_peer.peer_id, false) {
                            new_split_peer.result =
                                Some(format!("status {:?} is not expected", v.get()));
                        } else {
                            replace_regions.insert(*region_id);
                            v.insert((new_split_peer.peer_id, true));
                        }
                    }
                    HashMapEntry::Vacant(v) => {
                        v.insert((new_split_peer.peer_id, true));
                    }
                }
            }
        }

        fail_point!(
            "on_handle_apply_split_2_after_mem_check",
            self.id() == 2,
            |_| unimplemented!()
        );

        // region_id -> peer_id
        let mut already_exist_regions = Vec::new();
        for (region_id, new_split_peer) in new_split_regions.iter_mut() {
            let region_state_key = keys::region_state_key(*region_id);
            match ctx
                .engine
                .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
            {
                Ok(None) => (),
                Ok(Some(state)) => {
                    if replace_regions.get(region_id).is_some() {
                        // It's marked replaced, then further destroy will skip cleanup, so there
                        // should be no region local state.
                        panic!(
                            "{} failed to replace region {} peer {} because state {:?} alread exist in kv engine",
                            self.tag, region_id, new_split_peer.peer_id, state
                        )
                    }
                    already_exist_regions.push((*region_id, new_split_peer.peer_id));
                    new_split_peer.result = Some(format!("state {:?} exist in kv engine", state));
                }
                e => panic!(
                    "{} failed to get regions state of {}: {:?}",
                    self.tag, region_id, e
                ),
            }
        }

        if !already_exist_regions.is_empty() {
            let mut pending_create_peers = ctx.pending_create_peers.lock().unwrap();
            for (region_id, peer_id) in &already_exist_regions {
                assert_eq!(
                    pending_create_peers.remove(region_id),
                    Some((*peer_id, true))
                );
            }
        }

        let kv_wb_mut = ctx.kv_wb_mut();
        for new_region in &regions {
            if new_region.get_id() == derived.get_id() {
                continue;
            }
            let new_split_peer = new_split_regions.get(&new_region.get_id()).unwrap();
            if let Some(ref r) = new_split_peer.result {
                warn!(
                    "new region from splitting already exists";
                    "new_region_id" => new_region.get_id(),
                    "new_peer_id" => new_split_peer.peer_id,
                    "reason" => r,
                    "region_id" => self.region_id(),
                    "peer_id" => self.id(),
                );
                continue;
            }
            write_peer_state(kv_wb_mut, new_region, PeerState::Normal, None)
                .and_then(|_| write_initial_apply_state(kv_wb_mut, new_region.get_id()))
                .unwrap_or_else(|e| {
                    panic!(
                        "{} fails to save split region {:?}: {:?}",
                        self.tag, new_region, e
                    )
                });
        }
        write_peer_state(kv_wb_mut, &derived, PeerState::Normal, None).unwrap_or_else(|e| {
            panic!("{} fails to update region {:?}: {:?}", self.tag, derived, e)
        });
        let mut resp = AdminResponse::default();
        resp.mut_splits().set_regions(regions.clone().into());
        PEER_ADMIN_CMD_COUNTER.batch_split.success.inc();

        fail_point!(
            "apply_after_split_1_3",
            self.id == 3 && self.region_id() == 1,
            |_| { unreachable!() }
        );

        Ok((
            resp,
            ApplyResult::Res(ExecResult::SplitRegion {
                regions,
                derived,
                new_split_regions,
            }),
        ))
    }

    fn exec_prepare_merge(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!("apply_before_prepare_merge");
        fail_point!(
            "apply_before_prepare_merge_2_3",
            ctx.store_id == 2 || ctx.store_id == 3,
            |_| { unreachable!() }
        );

        PEER_ADMIN_CMD_COUNTER.prepare_merge.all.inc();

        let prepare_merge = req.get_prepare_merge();
        let index = prepare_merge.get_min_index();
        let first_index = entry_storage::first_index(&self.apply_state);
        if index < first_index {
            // We filter `CompactLog` command before.
            panic!(
                "{} first index {} > min_index {}, skip pre merge",
                self.tag, first_index, index
            );
        }
        let mut region = self.region.clone();
        let region_version = region.get_region_epoch().get_version() + 1;
        region.mut_region_epoch().set_version(region_version);
        // In theory conf version should not be increased when executing prepare_merge.
        // However, we don't want to do conf change after prepare_merge is committed.
        // This can also be done by iterating all proposal to find if prepare_merge is
        // proposed before proposing conf change, but it make things complicated.
        // Another way is make conf change also check region version, but this is not
        // backward compatible.
        let conf_version = region.get_region_epoch().get_conf_ver() + 1;
        region.mut_region_epoch().set_conf_ver(conf_version);
        let mut merging_state = MergeState::default();
        merging_state.set_min_index(index);
        merging_state.set_target(prepare_merge.get_target().to_owned());
        merging_state.set_commit(ctx.exec_log_index);
        write_peer_state(
            ctx.kv_wb_mut(),
            &region,
            PeerState::Merging,
            Some(merging_state.clone()),
        )
            .unwrap_or_else(|e| {
                panic!(
                    "{} failed to save merging state {:?} for region {:?}: {:?}",
                    self.tag, merging_state, region, e
                )
            });
        fail_point!("apply_after_prepare_merge");
        PEER_ADMIN_CMD_COUNTER.prepare_merge.success.inc();

        Ok((
            AdminResponse::default(),
            ApplyResult::Res(ExecResult::PrepareMerge {
                region,
                state: merging_state,
            }),
        ))
    }

    // The target peer should send missing log entries to the source peer.
    //
    // So, the merge process order would be:
    // - `exec_commit_merge` in target apply fsm and send `CatchUpLogs` to source
    //   peer fsm
    // - `on_catch_up_logs_for_merge` in source peer fsm
    // - if the source peer has already executed the corresponding
    //   `on_ready_prepare_merge`, set pending_remove and jump to step 6
    // - ... (raft append and apply logs)
    // - `on_ready_prepare_merge` in source peer fsm and set pending_remove (means
    //   source region has finished applying all logs)
    // - `logs_up_to_date_for_merge` in source apply fsm (destroy its apply fsm and
    //   send Noop to trigger the target apply fsm)
    // - resume `exec_commit_merge` in target apply fsm
    // - `on_ready_commit_merge` in target peer fsm and send `MergeResult` to source
    //   peer fsm
    // - `on_merge_result` in source peer fsm (destroy itself)
    fn exec_commit_merge(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        {
            fail_point!("apply_before_commit_merge");
            let apply_before_commit_merge = || {
                fail_point!(
                    "apply_before_commit_merge_except_1_4",
                    self.region_id() == 1 && self.id != 4,
                    |_| {}
                );
            };
            apply_before_commit_merge();
        }

        PEER_ADMIN_CMD_COUNTER.commit_merge.all.inc();

        let merge = req.get_commit_merge();
        let source_region = merge.get_source();
        let source_region_id = source_region.get_id();

        // No matter whether the source peer has applied to the required index,
        // it's a race to write apply state in both source delegate and target
        // delegate. So asking the source delegate to stop first.
        if self.ready_source_region_id != source_region_id {
            if self.ready_source_region_id != 0 {
                panic!(
                    "{} unexpected ready source region {}, expecting {}",
                    self.tag, self.ready_source_region_id, source_region_id
                );
            }
            info!(
                "asking delegate to stop";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "source_region_id" => source_region_id
            );
            fail_point!("before_handle_catch_up_logs_for_merge");
            // Sends message to the source peer fsm and pause `exec_commit_merge` process
            let logs_up_to_date = Arc::new(AtomicU64::new(0));
            let msg = SignificantMsg::CatchUpLogs(CatchUpLogs {
                target_region_id: self.region_id(),
                merge: merge.to_owned(),
                logs_up_to_date: logs_up_to_date.clone(),
            });
            ctx.notifier
                .notify_one(source_region_id, PeerMsg::SignificantMsg(msg));
            return Ok((
                AdminResponse::default(),
                ApplyResult::WaitMergeSource(logs_up_to_date),
            ));
        }

        info!(
            "execute CommitMerge";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "commit" => merge.get_commit(),
            "entries" => merge.get_entries().len(),
            "term" => ctx.exec_log_term,
            "index" => ctx.exec_log_index,
            "source_region" => ?source_region
        );

        self.ready_source_region_id = 0;

        let region_state_key = keys::region_state_key(source_region_id);
        let state: RegionLocalState = match ctx.engine.get_msg_cf(CF_RAFT, &region_state_key) {
            Ok(Some(s)) => s,
            e => panic!(
                "{} failed to get regions state of {:?}: {:?}",
                self.tag, source_region, e
            ),
        };
        if state.get_state() != PeerState::Merging {
            panic!(
                "{} unexpected state of merging region {:?}",
                self.tag, state
            );
        }
        let exist_region = state.get_region().to_owned();
        if *source_region != exist_region {
            panic!(
                "{} source_region {:?} not match exist region {:?}",
                self.tag, source_region, exist_region
            );
        }
        let mut region = self.region.clone();
        // Use a max value so that pd can ensure overlapped region has a priority.
        let version = cmp::max(
            source_region.get_region_epoch().get_version(),
            region.get_region_epoch().get_version(),
        ) + 1;
        region.mut_region_epoch().set_version(version);
        if keys::enc_end_key(&region) == keys::enc_start_key(source_region) {
            region.set_end_key(source_region.get_end_key().to_vec());
        } else {
            region.set_start_key(source_region.get_start_key().to_vec());
        }
        let kv_wb_mut = ctx.kv_wb_mut();
        write_peer_state(kv_wb_mut, &region, PeerState::Normal, None)
            .and_then(|_| {
                // TODO: maybe all information needs to be filled?
                let mut merging_state = MergeState::default();
                merging_state.set_target(self.region.clone());
                write_peer_state(
                    kv_wb_mut,
                    source_region,
                    PeerState::Tombstone,
                    Some(merging_state),
                )
            })
            .unwrap_or_else(|e| {
                panic!(
                    "{} failed to save merge region {:?}: {:?}",
                    self.tag, region, e
                )
            });

        PEER_ADMIN_CMD_COUNTER.commit_merge.success.inc();

        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::CommitMerge {
                index: ctx.exec_log_index,
                region,
                source: source_region.to_owned(),
            }),
        ))
    }

    fn exec_rollback_merge(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!("apply_before_rollback_merge");

        PEER_ADMIN_CMD_COUNTER.rollback_merge.all.inc();
        let region_state_key = keys::region_state_key(self.region_id());
        let state: RegionLocalState = match ctx.engine.get_msg_cf(CF_RAFT, &region_state_key) {
            Ok(Some(s)) => s,
            e => panic!("{} failed to get regions state: {:?}", self.tag, e),
        };
        assert_eq!(state.get_state(), PeerState::Merging, "{}", self.tag);
        let rollback = req.get_rollback_merge();
        assert_eq!(
            state.get_merge_state().get_commit(),
            rollback.get_commit(),
            "{}",
            self.tag
        );
        let mut region = self.region.clone();
        let version = region.get_region_epoch().get_version();
        // Update version to avoid duplicated rollback requests.
        region.mut_region_epoch().set_version(version + 1);
        write_peer_state(ctx.kv_wb_mut(), &region, PeerState::Normal, None).unwrap_or_else(|e| {
            panic!(
                "{} failed to rollback merge {:?}: {:?}",
                self.tag, rollback, e
            )
        });

        PEER_ADMIN_CMD_COUNTER.rollback_merge.success.inc();
        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::RollbackMerge {
                region,
                commit: rollback.get_commit(),
            }),
        ))
    }

    fn exec_compact_log(
        &mut self,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        PEER_ADMIN_CMD_COUNTER.compact.all.inc();

        let compact_index = req.get_compact_log().get_compact_index();
        let resp = AdminResponse::default();
        let first_index = entry_storage::first_index(&self.apply_state);
        if compact_index <= first_index {
            debug!(
                "compact index <= first index, no need to compact";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "compact_index" => compact_index,
                "first_index" => first_index,
            );
            return Ok((resp, ApplyResult::None));
        }
        if self.is_merging {
            info!(
                "in merging mode, skip compact";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "compact_index" => compact_index
            );
            return Ok((resp, ApplyResult::None));
        }

        let compact_term = req.get_compact_log().get_compact_term();
        // TODO: add unit tests to cover all the message integrity checks.
        if compact_term == 0 {
            info!(
                "compact term missing, skip";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "command" => ?req.get_compact_log()
            );
            // old format compact log command, safe to ignore.
            return Err(box_err!(
                "command format is outdated, please upgrade leader"
            ));
        }

        // compact failure is safe to be omitted, no need to assert.
        compact_raft_log(
            &self.tag,
            &mut self.apply_state,
            compact_index,
            compact_term,
        )?;

        PEER_ADMIN_CMD_COUNTER.compact.success.inc();

        Ok((
            resp,
            ApplyResult::Res(ExecResult::CompactLog {
                state: self.apply_state.get_truncated_state().clone(),
                first_index,
            }),
        ))
    }

    fn exec_transfer_leader(
        &mut self,
        req: &AdminRequest,
        term: u64,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        PEER_ADMIN_CMD_COUNTER.transfer_leader.all.inc();
        let resp = AdminResponse::default();

        let peer = req.get_transfer_leader().get_peer();
        // Only execute TransferLeader if the expected new leader is self.
        if peer.get_id() == self.id {
            Ok((resp, ApplyResult::Res(ExecResult::TransferLeader { term })))
        } else {
            Ok((resp, ApplyResult::None))
        }
    }

    fn exec_compute_hash(
        &self,
        ctx: &ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::ComputeHash {
                region: self.region.clone(),
                index: ctx.exec_log_index,
                context: req.get_compute_hash().get_context().to_vec(),
                // This snapshot may be held for a long time, which may cause too many
                // open files in rocksdb.
                // TODO: figure out another way to do consistency check without snapshot
                // or short life snapshot.
                snap: ctx.engine.snapshot(),
            }),
        ))
    }

    fn exec_verify_hash(
        &self,
        _: &ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        let verify_req = req.get_verify_hash();
        let index = verify_req.get_index();
        let context = verify_req.get_context().to_vec();
        let hash = verify_req.get_hash().to_vec();
        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::VerifyHash {
                index,
                context,
                hash,
            }),
        ))
    }

    fn update_memory_trace(&mut self, event: &mut TraceEvent) {
        let pending_cmds = self.pending_cmds.heap_size();

        let task = ApplyMemoryTrace {
            pending_cmds,
            0,
        };
        if let Some(e) = self.trace.reset(task) {
            *event = *event + e;
        }
    }
}
