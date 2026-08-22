#include "raft_node_impl.h"

using namespace rollingraft;

constexpr size_t kSnapshotChunkSize = 64 * 1024;  // 64KB chunks

void RaftNode::RaftNodeImpl::StartSnapshotCheckTimerLocked() {
  // PRECONDITION: group_->snapshot_mtx_ is held by caller
  if (group_->snapshot_check_timer_enabled_) {
    return;  // Already running
  }

  auto cfg = infra_->runtime_config_->Get();
  group_->snapshot_check_deadline_ =
      timer_->Now() + std::chrono::milliseconds(cfg.snapshot_check_interval_ms);
  group_->snapshot_check_timer_enabled_ = true;

  LOG_INFO("Node {} started auto-snapshot check (every {}ms)", group_->server_id_,
           cfg.snapshot_check_interval_ms);
}

void RaftNode::RaftNodeImpl::StopSnapshotCheckTimerLocked() {
  // PRECONDITION: group_->snapshot_mtx_ is held by caller
  group_->snapshot_check_timer_enabled_ = false;
}

void RaftNode::RaftNodeImpl::CheckSnapshotTimeoutLocked() {
  {
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    if (!group_->snapshot_check_timer_enabled_ ||
        timer_->Now() < group_->snapshot_check_deadline_) {
      return;
    }

    auto cfg = infra_->runtime_config_->Get();
    group_->snapshot_check_deadline_ += std::chrono::milliseconds(cfg.snapshot_check_interval_ms);
  }

  // Snapshot I/O runs outside the manager locks (two-phase); this call takes
  // the locks itself only for the cheap threshold check and the apply.
  RunAutoSnapshotIfNeeded();
}

// Lock-free: true when log growth since the last snapshot exceeds the
// configured thresholds. PRECONDITION: election+replication+snapshot locks held.
bool RaftNode::RaftNodeImpl::ShouldTriggerSnapshotLocked() {
  auto [last_index, last_term] = group_->log_.GetLastLogInfo();
  (void)last_term;
  if (last_index <= group_->last_snapshot_index_) {
    // Empty or fully-compacted log: nothing new to snapshot. Guards the
    // unsigned subtraction below (0 - last_snapshot_index_ would wrap).
    return false;
  }
  Index entries_since_snapshot = last_index - group_->last_snapshot_index_;

  auto [entry_count, byte_size] = group_->log_.GetLogStats();
  (void)entry_count;

  auto cfg = infra_->runtime_config_->Get();
  if (entries_since_snapshot >= cfg.snapshot_threshold_entries) {
    return true;
  }
  if (byte_size >= cfg.snapshot_threshold_bytes) {
    return true;
  }
  return false;
}

// No locks held: create the snapshot from the state machine and stream it to
// the persister. The state machine must be safe to call concurrently with
// Apply (it already is for Query). Returns the snapshot's meta via out params.
Status RaftNode::RaftNodeImpl::CreateAndPersistSnapshot(const std::string& trigger,
                                                        Index& out_index, Term& out_term) {
  // Create snapshot
  auto snapshot = group_->state_machine_->CreateSnapshot();
  if (!snapshot) {
    LOG_ERROR("Node {} failed to create {}-snapshot", group_->server_id_, trigger);
    return Status::Error("Failed to create snapshot");
  }

  // Get snapshot metadata
  auto meta = snapshot->GetMeta();
  Index snapshot_index = meta.last_included_index_;
  Term snapshot_term = meta.last_included_term_;

  // Stream snapshot data to persister to avoid OOM.
  constexpr size_t kReadChunkSize = 64 * 1024;  // 64KB chunks
  std::vector<uint8_t> buffer(kReadChunkSize);

  auto cfg = infra_->runtime_config_->Get();
  const size_t kMaxSnapshotSize = cfg.max_snapshot_size_bytes > 0
                                      ? cfg.max_snapshot_size_bytes
                                      : 100 * 1024 * 1024;  // 100MB default

  // Pre-scan to check total size (memory-free)
  uint64_t check_offset = 0;
  size_t total_size = 0;
  while (true) {
    size_t bytes_read = snapshot->Read(check_offset, buffer.data(), kReadChunkSize);
    if (bytes_read == 0) {
      break;
    }
    total_size += bytes_read;
    if (total_size > kMaxSnapshotSize) {
      LOG_ERROR(
          "Node {} {}-snapshot exceeds max size ({} > {} bytes). "
          "Increase max_snapshot_size_bytes or implement streaming persister.",
          group_->server_id_, trigger, total_size, kMaxSnapshotSize);
      return Status::Error("Snapshot exceeds max size");
    }
    check_offset += bytes_read;
  }

  // Persist snapshot using streaming interface. Serialized against the
  // receive-side persist (same persister, same chunk keys); a snapshot
  // superseded while we were creating it must not be written last — that
  // would regress the durable snapshot behind the already-truncated log.
  if (persister_ && total_size > 0) {
    std::lock_guard<std::mutex> io_lock(snapshot_io_mtx_);
    {
      std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
      std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
      if (snapshot_index <= group_->last_snapshot_index_) {
        LOG_WARN("Node {} skipping {}-snapshot persist: index {} <= last snapshot {}",
                 group_->server_id_, trigger, snapshot_index, group_->last_snapshot_index_);
        return Status::Error("Snapshot superseded while creating");
      }
    }

    uint64_t stream_offset = 0;
    auto chunk_provider = [&](std::string& chunk) -> bool {
      size_t bytes_read = snapshot->Read(stream_offset, buffer.data(), kReadChunkSize);
      if (bytes_read == 0) {
        return false;
      }
      chunk.assign(reinterpret_cast<char*>(buffer.data()), bytes_read);
      stream_offset += bytes_read;
      return true;
    };

    auto status = persister_->SaveSnapshotStream(chunk_provider, snapshot_index, snapshot_term);
    if (!status.ok()) {
      LOG_ERROR("Node {} failed to persist {}-snapshot: {}", group_->server_id_, trigger,
                status.ToString());
      return status;
    }
  }

  out_index = snapshot_index;
  out_term = snapshot_term;
  if (metrics_) {
    auto labels = group_->metrics_node_label_;
    labels["trigger"] = trigger;
    metrics_->GetCounter("raft_snapshots_created_total", labels).Increment();
  }
  return Status::OK();
}

// Apply a persisted snapshot to in-memory state. PRECONDITION:
// election+replication+snapshot locks held. Returns an error if the node
// stepped down or the snapshot became stale while I/O ran without locks.
Status RaftNode::RaftNodeImpl::ApplySnapshotLocked(Index snapshot_index, Term snapshot_term,
                                                   const std::string& trigger) {
  if (!IsRunning() || group_->role_ != RaftNodeRole::LEADER) {
    LOG_WARN("Node {} skipping {}-snapshot apply: no longer leader", group_->server_id_, trigger);
    if (!IsRunning()) {
      return Status::Error("Node stopped while creating snapshot");
    }
    return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
  }
  if (snapshot_index <= group_->last_snapshot_index_) {
    LOG_WARN("Node {} skipping {}-snapshot apply: index {} <= last snapshot {}", group_->server_id_,
             trigger, snapshot_index, group_->last_snapshot_index_);
    return Status::Error("Snapshot superseded while creating");
  }
  if (group_->log_.GetLastLogInfo().first > snapshot_index) {
    // The log grew past the snapshot index during the unlocked creation
    // window. SetStartIndex below clears ALL entries, so applying now would
    // drop committed entries and reuse their indices. Skip — the next tick
    // creates a fresh snapshot that covers them.
    LOG_WARN("Node {} skipping {}-snapshot apply: log advanced past snapshot index {}",
             group_->server_id_, trigger, snapshot_index);
    return Status::Error("Log advanced while creating snapshot");
  }

  Index entries_since_snapshot = snapshot_index - group_->last_snapshot_index_;

  // Truncate log - entries before snapshot_index are now covered by snapshot
  group_->log_.SetStartIndex(snapshot_index + 1);
  group_->last_snapshot_index_ = snapshot_index;
  group_->last_snapshot_term_ = snapshot_term;

  // Schedule async truncation of persisted log after releasing locks.
  // TruncatePrefix I/O can be slow; performing it asynchronously prevents
  // blocking the Raft event loop while holding manager locks.
  if (log_persister_) {
    auto cfg = infra_->runtime_config_->Get();
    uint64_t compact_before = 1;
    if (snapshot_index + 1 > cfg.log_retention_entries) {
      compact_before = snapshot_index + 1 - cfg.log_retention_entries;
    }
    NodeId my_id = group_->server_id_;
    log_persister_->TruncatePrefixAsync(compact_before, [my_id](Status status) {
      if (!status.ok()) {
        LOG_WARN("Node {} async truncate failed: {}", my_id, status.ToString());
      }
    });
  }

  if (metrics_) {
    auto labels = group_->metrics_node_label_;
    labels["trigger"] = trigger;
    metrics_->GetCounter("raft_log_compactions_total", labels).Increment();
    metrics_->GetCounter("raft_log_entries_compacted_total", group_->metrics_node_label_)
        .Increment(entries_since_snapshot);
  }

  LOG_INFO("Node {} {}-snapshot applied at index {} term {}", group_->server_id_, trigger,
           snapshot_index, snapshot_term);
  return Status::OK();
}

// Two-phase auto-snapshot entry: no locks held on entry. Checks thresholds
// under locks, creates+persists the snapshot without locks, then reapplies.
Status RaftNode::RaftNodeImpl::RunAutoSnapshotIfNeeded() {
  bool expected = false;
  if (!snapshot_in_progress_.compare_exchange_strong(expected, true)) {
    return Status::OK();  // Another snapshot is mid-flight
  }
  struct Guard {
    std::atomic<bool>& flag;
    ~Guard() { flag.store(false, std::memory_order_release); }
  } guard{snapshot_in_progress_};

  // Phase 1 (locked, fast): role + threshold check.
  bool should_trigger = false;
  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    if (!IsRunning() || group_->role_ != RaftNodeRole::LEADER) {
      return Status::OK();
    }
    should_trigger = ShouldTriggerSnapshotLocked();
  }
  if (!should_trigger) {
    return Status::OK();
  }

  LOG_INFO("Node {} triggering auto-snapshot", group_->server_id_);

  // Phase 2 (no locks): user state machine + disk I/O.
  Index snapshot_index = 0;
  Term snapshot_term = 0;
  auto status = CreateAndPersistSnapshot("auto", snapshot_index, snapshot_term);
  if (!status.ok()) {
    return status;
  }

  // Phase 3 (locked, fast): apply.
  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    return ApplySnapshotLocked(snapshot_index, snapshot_term, "auto");
  }
}

Status RaftNode::RaftNodeImpl::TriggerSnapshot() {
  // Two-phase manual snapshot: validate under locks, do the I/O without them.
  bool expected = false;
  if (!snapshot_in_progress_.compare_exchange_strong(expected, true)) {
    return Status::Error("A snapshot is already in progress");
  }
  struct Guard {
    std::atomic<bool>& flag;
    ~Guard() { flag.store(false, std::memory_order_release); }
  } guard{snapshot_in_progress_};

  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    if (!IsRunning()) {
      return Status::Error("Node not running");
    }
    if (group_->role_ != RaftNodeRole::LEADER) {
      return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
    }
  }

  Index snapshot_index = 0;
  Term snapshot_term = 0;
  auto status = CreateAndPersistSnapshot("manual", snapshot_index, snapshot_term);
  if (!status.ok()) {
    return status;
  }

  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    return ApplySnapshotLocked(snapshot_index, snapshot_term, "manual");
  }
}

// ========== Election Handling ==========

void RaftNode::RaftNodeImpl::SendInstallSnapshotToPeerLocked(NodeId peer_id) {
  // PRECONDITION: caller holds group_->election_mtx_, group_->replication_mtx_, and
  // group_->snapshot_mtx_
  auto& state = group_->snapshot_sends_[peer_id];

  // Already in progress? Skip
  if (state.in_progress) {
    LOG_DEBUG("Node {}: snapshot send to {} already in progress", group_->server_id_, peer_id);
    return;
  }

  // Snapshot already prepared: start sending its first chunk.
  if (state.snapshot) {
    if (metrics_) {
      auto labels = group_->metrics_node_label_;
      labels["peer_id"] = std::to_string(peer_id);
      metrics_->GetCounter("raft_snapshot_sends_started_total", labels).Increment();
    }
    state.in_progress = true;
    LOG_INFO("Node {}: starting snapshot send to {}: index={}, term={}, size=?", group_->server_id_,
             peer_id, state.last_included_index, state.last_included_term);
    SendNextSnapshotChunkLocked(peer_id);
    return;
  }

  // No snapshot prepared: create it WITHOUT the manager locks (user state
  // machine can be slow), then relock and send. Weak-guarded timer so a
  // destroyed group is a no-op.
  //
  // The pending flag is set BEFORE the timer is armed so concurrent
  // heartbeat ticks do not queue a second full CreateSnapshot while the
  // first is still being made, and the shared in-flight token serializes
  // creation against the auto/manual paths — the StateMachine contract does
  // not cover concurrent CreateSnapshot calls.
  bool expected = false;
  if (!snapshot_in_progress_.compare_exchange_strong(expected, true)) {
    return;  // Another snapshot creation is mid-flight; the next tick retries
  }
  state.in_progress = true;  // Pending: preparation scheduled, not yet sent
  LOG_INFO("Node {}: preparing snapshot for {}", group_->server_id_, peer_id);
  infra_->timer_->SetTimeout(std::chrono::milliseconds(0),
                             [weak_self = weak_from_this(), peer_id]() {
                               auto self = weak_self.lock();
                               if (!self) {
                                 return;
                               }
                               self->PrepareSnapshotForPeer(peer_id);
                             });
}

// No locks held: create a snapshot for the given peer and, if it is still
// needed, install it into the send state and dispatch the first chunk.
void RaftNode::RaftNodeImpl::PrepareSnapshotForPeer(NodeId peer_id) {
  struct InFlightGuard {
    std::atomic<bool>& flag;
    ~InFlightGuard() { flag.store(false, std::memory_order_release); }
  } in_flight_guard{snapshot_in_progress_};

  auto snapshot = group_->state_machine_->CreateSnapshot();
  if (!snapshot) {
    LOG_ERROR("Node {}: failed to create snapshot for {}", group_->server_id_, peer_id);
    return;
  }
  auto meta = snapshot->GetMeta();

  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);

    auto it = group_->snapshot_sends_.find(peer_id);
    if (it == group_->snapshot_sends_.end()) {
      return;  // Peer removed meanwhile; its pending flag went with it
    }
    auto& state = it->second;

    // The world may have changed while we were unlocked.
    if (!IsRunning() || group_->role_ != RaftNodeRole::LEADER) {
      state.in_progress = false;  // Clear the pending flag so a later tick can retry
      return;
    }
    if (state.snapshot) {
      return;  // A send is already running (defensive; scheduling is serialized)
    }
    // Peer may no longer need a snapshot (caught up meanwhile).
    auto nit = group_->next_index_.find(peer_id);
    if (nit == group_->next_index_.end() || nit->second >= group_->log_.GetFirstIndex()) {
      state.in_progress = false;  // Nothing to send; allow a fresh prepare if it falls behind again
      return;
    }

    state.snapshot = std::move(snapshot);
    state.offset = 0;
    state.last_included_index = meta.last_included_index_;
    state.last_included_term = meta.last_included_term_;
    state.in_progress = true;

    if (metrics_) {
      auto labels = group_->metrics_node_label_;
      labels["peer_id"] = std::to_string(peer_id);
      metrics_->GetCounter("raft_snapshot_sends_started_total", labels).Increment();
    }
    LOG_INFO("Node {}: starting snapshot send to {}: index={}, term={}, size=?", group_->server_id_,
             peer_id, state.last_included_index, state.last_included_term);
    SendNextSnapshotChunkLocked(peer_id);
  }
}

void RaftNode::RaftNodeImpl::SendNextSnapshotChunkLocked(NodeId peer_id) {
  // PRECONDITION: caller holds group_->election_mtx_ and group_->snapshot_mtx_
  auto it_state = group_->snapshot_sends_.find(peer_id);
  if (it_state == group_->snapshot_sends_.end()) {
    return;
  }

  auto& state = it_state->second;

  // Safety checks
  if (!state.snapshot || !state.in_progress) {
    LOG_ERROR("Node {}: invalid snapshot state for {}", group_->server_id_, peer_id);
    return;
  }

  // Read chunk
  std::vector<char> buffer(kSnapshotChunkSize);
  size_t bytes_read = state.snapshot->Read(state.offset, reinterpret_cast<uint8_t*>(buffer.data()),
                                           kSnapshotChunkSize);
  buffer.resize(bytes_read);
  state.last_chunk_size = bytes_read;

  // Check if this is the last chunk
  bool is_last = (bytes_read < kSnapshotChunkSize);

  // Build request
  InstallSnapshotRequest req;
  req.group_id = group_->group_id_;
  req.term_ = group_->current_term_;
  req.leader_id_ = group_->server_id_;
  req.last_included_index_ = state.last_included_index;
  req.last_included_term_ = state.last_included_term;
  req.offset_ = static_cast<uint32_t>(state.offset);
  req.data_ = std::move(buffer);
  req.done_ = is_last;
  req.correlation_id_ = next_correlation_id_.fetch_add(1, std::memory_order_relaxed);

  // Serialize
  std::string data;
  auto status = infra_->protocol_->SerializeRequest(req, data);
  if (!status.ok()) {
    LOG_ERROR("Node {}: failed to serialize InstallSnapshotRequest: {}", group_->server_id_,
              status.ToString());
    state.in_progress = false;
    return;
  }

  // Get peer address
  auto it_addr = group_->peer_map_.find(peer_id);
  if (it_addr == group_->peer_map_.end()) {
    LOG_ERROR("Node {}: peer {} not found", group_->server_id_, peer_id);
    state.in_progress = false;
    return;
  }

  if (metrics_) {
    metrics_->GetCounter("raft_snapshot_chunks_sent_total", group_->metrics_node_label_)
        .Increment();
  }
  LOG_DEBUG("Node {}: sending snapshot chunk to {}: offset={}, size={}, done={}",
            group_->server_id_, peer_id, state.offset, bytes_read, is_last);

  // Send
  infra_->network_->SendRpc(
      peer_id, it_addr->second, data, req.correlation_id_,
      std::chrono::milliseconds(infra_->runtime_config_->Get().rpc_timeout_ms),
      [weak_self = weak_from_this(), this, peer_id](const std::string& resp, bool success,
                                                    const std::string& error) {
        auto keep_alive = weak_self.lock();
        if (!keep_alive) {
          return;
        }
        // Deserialize response first (outside lock)
        InstallSnapshotResponse response;
        if (success) {
          auto status = infra_->protocol_->DeserializeResponse(resp, response);
          if (!status.ok()) {
            LOG_ERROR(
                "Node {}: failed to deserialize "
                "InstallSnapshotResponse: {}",
                group_->server_id_, status.ToString());
            success = false;
          }
        } else {
          LOG_WARN("Node {}: InstallSnapshot to {} failed: {}", group_->server_id_, peer_id, error);
        }
        HandleInstallSnapshotResponse(peer_id, response, success);
      });
}

void RaftNode::RaftNodeImpl::HandleInstallSnapshotResponse(NodeId from,
                                                           const InstallSnapshotResponse& resp,
                                                           bool rpc_success) {
  // Phase 1: Election state check under group_->election_mtx_ only.
  // Must NOT hold group_->replication_mtx_ or group_->snapshot_mtx_ here because
  // BecomeFollowerLocked acquires both internally.
  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
    if (!IsRunning()) {
      return;
    }
    if (group_->role_ != RaftNodeRole::LEADER) {
      return;
    }

    if (resp.term_ > group_->current_term_) {
      LOG_INFO(
          "Node {}: follower {} has higher term {} vs {}, reverting to "
          "Follower",
          group_->server_id_, from, resp.term_, group_->current_term_);
      BecomeFollowerLocked(resp.term_);
      return;
    }
  }

  // Phase 2: Snapshot + replication work under full hierarchy.
  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);

    auto it = group_->snapshot_sends_.find(from);
    if (it == group_->snapshot_sends_.end()) {
      return;
    }

    auto& state = it->second;
    state.in_progress = false;

    // RPC failed: retry with backoff
    if (!rpc_success) {
      LOG_WARN("Node {}: snapshot RPC to {} failed, will retry", group_->server_id_, from);
      infra_->timer_->SetTimeout(std::chrono::milliseconds(100),
                                 [weak_self = weak_from_this(), this, from]() {
                                   auto keep_alive = weak_self.lock();
                                   if (!keep_alive) {
                                     return;
                                   }
                                   std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
                                   std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
                                   if (group_->role_ == RaftNodeRole::LEADER) {
                                     SendNextSnapshotChunkLocked(from);
                                   }
                                 });
      return;
    }

    // Transfer completes when the last chunk was short. (For snapshots whose
    // size is an exact multiple of the chunk size, the sender pads with one
    // final empty chunk, so this condition always triggers.)
    if (state.last_chunk_size < kSnapshotChunkSize) {
      // Transfer complete
      LOG_INFO("Node {}: snapshot send to {} completed, updating progress to {}",
               group_->server_id_, from, state.last_included_index);

      if (metrics_) {
        auto labels = group_->metrics_node_label_;
        labels["peer_id"] = std::to_string(from);
        metrics_->GetCounter("raft_snapshot_sends_completed_total", labels).Increment();
      }
      group_->match_index_[from] = state.last_included_index;
      group_->next_index_[from] = state.last_included_index + 1;
      SetPeerReplicationLagMetricLocked(from);

      // Clean up
      group_->snapshot_sends_.erase(it);

      // Try to commit (snapshot doesn't increase commit directly,
      // but we may be able to commit entries after the snapshot)
      TryCommitLocked();
      return;
    }

    // More chunks to send
    state.offset += state.last_chunk_size;
    state.in_progress = true;
    SendNextSnapshotChunkLocked(from);
  }
}
