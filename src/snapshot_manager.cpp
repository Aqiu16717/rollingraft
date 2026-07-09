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
      std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg.snapshot_check_interval_ms);
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
        std::chrono::steady_clock::now() < group_->snapshot_check_deadline_) {
      return;
    }

    auto cfg = infra_->runtime_config_->Get();
    group_->snapshot_check_deadline_ += std::chrono::milliseconds(cfg.snapshot_check_interval_ms);
  }

  MaybeTriggerAutoSnapshotLocked();
}

// Core snapshot logic. PRECONDITION: group_->election_mtx_, group_->replication_mtx_,
// group_->snapshot_mtx_ are held by caller.
void RaftNode::RaftNodeImpl::DoSnapshotLocked(const std::string& trigger) {
  auto [last_index, last_term] = group_->log_.GetLastLogInfo();
  (void)last_term;
  Index entries_since_snapshot = last_index - group_->last_snapshot_index_;

  if (metrics_) {
    auto labels = group_->metrics_node_label_;
    labels["trigger"] = trigger;
    metrics_->GetCounter("raft_snapshots_created_total", labels).Increment();
  }
  LOG_INFO("Node {} triggering {}-snapshot ({} entries since last)", group_->server_id_, trigger,
           entries_since_snapshot);

  // Create snapshot
  auto snapshot = group_->state_machine_->CreateSnapshot();
  if (!snapshot) {
    LOG_ERROR("Node {} failed to create {}-snapshot", group_->server_id_, trigger);
    return;
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
    if (bytes_read == 0) break;
    total_size += bytes_read;
    if (total_size > kMaxSnapshotSize) {
      LOG_ERROR(
          "Node {} {}-snapshot exceeds max size ({} > {} bytes). "
          "Increase max_snapshot_size_bytes or implement streaming persister.",
          group_->server_id_, trigger, total_size, kMaxSnapshotSize);
      return;
    }
    check_offset += bytes_read;
  }

  // Persist snapshot using streaming interface
  if (persister_ && total_size > 0) {
    uint64_t stream_offset = 0;
    auto chunk_provider = [&](std::string& chunk) -> bool {
      size_t bytes_read = snapshot->Read(stream_offset, buffer.data(), kReadChunkSize);
      if (bytes_read == 0) return false;
      chunk.assign(reinterpret_cast<char*>(buffer.data()), bytes_read);
      stream_offset += bytes_read;
      return true;
    };

    auto status = persister_->SaveSnapshotStream(chunk_provider, snapshot_index, snapshot_term);
    if (!status.ok()) {
      LOG_ERROR("Node {} failed to persist {}-snapshot: {}", group_->server_id_, trigger,
                status.ToString());
      return;
    }
  }

  // Truncate log - entries before snapshot_index are now covered by snapshot
  group_->log_.SetStartIndex(snapshot_index + 1);
  group_->last_snapshot_index_ = snapshot_index;

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

  LOG_INFO(
      "Node {} {}-snapshot completed at index {} term {} ({} bytes, "
      "{} entries truncated)",
      group_->server_id_, trigger, snapshot_index, snapshot_term, total_size,
      entries_since_snapshot);
}

void RaftNode::RaftNodeImpl::MaybeTriggerAutoSnapshotLocked() {
  // Bridge pattern: acquire group_->election_mtx_ -> group_->replication_mtx_ ->
  // group_->snapshot_mtx_ Auto-snapshot reads group_->role_ (election), group_->log_ stats
  // (replication), and updates group_->last_snapshot_index_ (snapshot).
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
  std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);

  if (group_->role_ != RaftNodeRole::LEADER) {
    return;  // Only leader triggers auto-snapshot
  }

  auto [last_index, last_term] = group_->log_.GetLastLogInfo();
  (void)last_term;

  // Calculate entries since last snapshot
  Index entries_since_snapshot = last_index - group_->last_snapshot_index_;

  // Get byte size for logging
  auto [entry_count, byte_size] = group_->log_.GetLogStats();
  (void)entry_count;

  bool should_trigger = false;

  auto cfg = infra_->runtime_config_->Get();

  // Check entry count threshold
  if (entries_since_snapshot >= cfg.snapshot_threshold_entries) {
    should_trigger = true;
  }

  // Check byte size threshold
  if (!should_trigger && byte_size >= cfg.snapshot_threshold_bytes) {
    should_trigger = true;
  }

  if (!should_trigger) {
    return;
  }

  DoSnapshotLocked("auto");
}

Status RaftNode::RaftNodeImpl::TriggerSnapshot() {
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
  std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (group_->role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
  }

  DoSnapshotLocked("manual");
  return Status::OK();
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

  // Create new snapshot if needed
  if (!state.snapshot) {
    LOG_INFO("Node {}: creating snapshot for {}", group_->server_id_, peer_id);
    state.snapshot = group_->state_machine_->CreateSnapshot();
    if (!state.snapshot) {
      LOG_ERROR("Node {}: failed to create snapshot", group_->server_id_);
      return;
    }
    state.offset = 0;
    state.last_included_index = state.snapshot->GetMeta().last_included_index_;
    state.last_included_term = state.snapshot->GetMeta().last_included_term_;
  }

  if (metrics_) {
    auto labels = group_->metrics_node_label_;
    labels["peer_id"] = std::to_string(peer_id);
    metrics_->GetCounter("raft_snapshot_sends_started_total", labels).Increment();
  }
  state.in_progress = true;
  LOG_INFO("Node {}: starting snapshot send to {}: index={}, term={}, size=?", group_->server_id_,
           peer_id, state.last_included_index, state.last_included_term);

  SendNextSnapshotChunkLocked(peer_id);
}

void RaftNode::RaftNodeImpl::SendNextSnapshotChunkLocked(NodeId peer_id) {
  // PRECONDITION: caller holds group_->election_mtx_ and group_->snapshot_mtx_
  auto it_state = group_->snapshot_sends_.find(peer_id);
  if (it_state == group_->snapshot_sends_.end()) return;

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
      [this, peer_id](const std::string& resp, bool success, const std::string& error) {
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
    if (!IsRunning()) return;
    if (group_->role_ != RaftNodeRole::LEADER) return;

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
    if (it == group_->snapshot_sends_.end()) return;

    auto& state = it->second;
    state.in_progress = false;

    // RPC failed: retry with backoff
    if (!rpc_success) {
      LOG_WARN("Node {}: snapshot RPC to {} failed, will retry", group_->server_id_, from);
      infra_->timer_->SetTimeout(std::chrono::milliseconds(100), [this, from]() {
        std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
        std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
        if (group_->role_ == RaftNodeRole::LEADER) {
          SendNextSnapshotChunkLocked(from);
        }
      });
      return;
    }

    // Check if we're done
    if (state.offset + state.last_chunk_size >= state.snapshot->GetMeta().last_included_index_) {
      // Actually we need to track total size, not index. Let 'done' flag drive
      // this. But we don't store total size. Use the done flag from last send.
      // Simpler: check if last chunk was smaller than chunk size
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
    }

    // More chunks to send
    state.offset += state.last_chunk_size;
    state.in_progress = true;
    SendNextSnapshotChunkLocked(from);
  }
}
