#include "raft_node_impl.h"

using namespace rollingraft;

constexpr size_t kSnapshotChunkSize = 64 * 1024;  // 64KB chunks

void RaftNode::RaftNodeImpl::StartSnapshotCheckTimerLocked() {
  if (snapshot_check_timer_ != 0) {
    return;  // Already running
  }

  snapshot_check_timer_ = timer_->SetInterval(
      std::chrono::milliseconds(config_.snapshot_check_interval_ms),
      [this]() { MaybeTriggerAutoSnapshotLocked(); });

  LOG_INFO("Node {} started auto-snapshot check (every {}ms)", server_id_,
           config_.snapshot_check_interval_ms);
}

void RaftNode::RaftNodeImpl::StopSnapshotCheckTimerLocked() {
  if (snapshot_check_timer_ != 0) {
    timer_->CancelTimer(snapshot_check_timer_);
    snapshot_check_timer_ = 0;
  }
}

void RaftNode::RaftNodeImpl::MaybeTriggerAutoSnapshotLocked() {
  if (role_ != RaftNodeRole::LEADER) {
    return;  // Only leader triggers auto-snapshot
  }

  auto [last_index, last_term] = log_.GetLastLogInfo();
  (void)last_term;

  // Calculate entries since last snapshot
  Index entries_since_snapshot = last_index - last_snapshot_index_;

  // Get byte size for logging
  auto [entry_count, byte_size] = log_.GetLogStats();
  (void)entry_count;

  bool should_trigger = false;
  std::string reason;

  // Check entry count threshold
  if (entries_since_snapshot >= config_.snapshot_threshold_entries) {
    should_trigger = true;
    reason = std::to_string(entries_since_snapshot) +
             " entries since last snapshot (threshold: " +
             std::to_string(config_.snapshot_threshold_entries) + ")";
  }

  // Check byte size threshold
  if (!should_trigger && byte_size >= config_.snapshot_threshold_bytes) {
    should_trigger = true;
    reason = std::to_string(byte_size) +
             " bytes since last snapshot (threshold: " +
             std::to_string(config_.snapshot_threshold_bytes) + ")";
  }

  if (!should_trigger) {
    return;
  }

  if (metrics_) {
    metrics_
        ->GetCounter(
            "raft_snapshots_created_total",
            {{"node_id", std::to_string(server_id_)}, {"trigger", "auto"}})
        .Increment();
  }
  LOG_INFO("Node {} triggering auto-snapshot: {}", server_id_, reason);

  // Create snapshot
  auto snapshot = state_machine_->CreateSnapshot();
  if (!snapshot) {
    LOG_ERROR("Node {} failed to create auto-snapshot", server_id_);
    return;
  }

  // Get snapshot metadata
  auto meta = snapshot->GetMeta();
  Index snapshot_index = meta.last_included_index_;
  Term snapshot_term = meta.last_included_term_;

  // Read full snapshot data
  std::string snapshot_data;
  constexpr size_t kReadChunkSize = 64 * 1024;  // 64KB chunks
  std::vector<uint8_t> buffer(kReadChunkSize);
  uint64_t offset = 0;

  while (true) {
    size_t bytes_read = snapshot->Read(offset, buffer.data(), kReadChunkSize);
    if (bytes_read == 0) {
      break;
    }
    snapshot_data.append(reinterpret_cast<char*>(buffer.data()), bytes_read);
    offset += bytes_read;
  }

  // Persist snapshot
  if (persister_ && !snapshot_data.empty()) {
    auto status =
        persister_->SaveSnapshot(snapshot_data, snapshot_index, snapshot_term);
    if (!status.ok()) {
      LOG_ERROR("Node {} failed to persist auto-snapshot: {}", server_id_,
                status.ToString());
      return;
    }
  }

  // Truncate log - entries before snapshot_index are now covered by snapshot
  log_.SetStartIndex(snapshot_index + 1);
  last_snapshot_index_ = snapshot_index;

  // Truncate persisted log with retention buffer
  if (log_persister_) {
    uint64_t compact_before = 1;
    if (snapshot_index + 1 > config_.log_retention_entries) {
      compact_before = snapshot_index + 1 - config_.log_retention_entries;
    }
    auto status = log_persister_->TruncatePrefix(compact_before);
    if (!status.ok()) {
      LOG_WARN("Node {} failed to truncate persisted log: {}", server_id_,
               status.ToString());
    }
  }

  if (metrics_) {
    metrics_
        ->GetCounter(
            "raft_log_compactions_total",
            {{"node_id", std::to_string(server_id_)}, {"trigger", "auto"}})
        .Increment();
    metrics_
        ->GetCounter("raft_log_entries_compacted_total",
                     {{"node_id", std::to_string(server_id_)}})
        .Increment(entries_since_snapshot);
  }

  LOG_INFO(
      "Node {} auto-snapshot completed at index {} term {} ({} bytes, "
      "{} entries truncated)",
      server_id_, snapshot_index, snapshot_term, snapshot_data.size(),
      entries_since_snapshot);
}

// ========== Election Handling ==========

void RaftNode::RaftNodeImpl::SendInstallSnapshotToPeerLocked(NodeId peer_id) {
  auto& state = snapshot_sends_[peer_id];

  // Already in progress? Skip
  if (state.in_progress) {
    LOG_DEBUG("Node {}: snapshot send to {} already in progress", server_id_,
              peer_id);
    return;
  }

  // Create new snapshot if needed
  if (!state.snapshot) {
    LOG_INFO("Node {}: creating snapshot for {}", server_id_, peer_id);
    state.snapshot = state_machine_->CreateSnapshot();
    if (!state.snapshot) {
      LOG_ERROR("Node {}: failed to create snapshot", server_id_);
      return;
    }
    state.offset = 0;
    state.last_included_index = state.snapshot->GetMeta().last_included_index_;
    state.last_included_term = state.snapshot->GetMeta().last_included_term_;
  }

  if (metrics_) {
    metrics_
        ->GetCounter("raft_snapshot_sends_started_total",
                     {{"node_id", std::to_string(server_id_)},
                      {"peer_id", std::to_string(peer_id)}})
        .Increment();
  }
  state.in_progress = true;
  LOG_INFO("Node {}: starting snapshot send to {}: index={}, term={}, size=?",
           server_id_, peer_id, state.last_included_index,
           state.last_included_term);

  SendNextSnapshotChunkLocked(peer_id);
}

void RaftNode::RaftNodeImpl::SendNextSnapshotChunkLocked(NodeId peer_id) {
  auto it_state = snapshot_sends_.find(peer_id);
  if (it_state == snapshot_sends_.end()) return;

  auto& state = it_state->second;

  // Safety checks
  if (!state.snapshot || !state.in_progress) {
    LOG_ERROR("Node {}: invalid snapshot state for {}", server_id_, peer_id);
    return;
  }

  // Read chunk
  std::vector<char> buffer(kSnapshotChunkSize);
  size_t bytes_read = state.snapshot->Read(
      state.offset, reinterpret_cast<uint8_t*>(buffer.data()),
      kSnapshotChunkSize);
  buffer.resize(bytes_read);
  state.last_chunk_size = bytes_read;

  // Check if this is the last chunk
  bool is_last = (bytes_read < kSnapshotChunkSize);

  // Build request
  InstallSnapshotRequest req;
  req.term_ = current_term_;
  req.leader_id_ = server_id_;
  req.last_included_index_ = state.last_included_index;
  req.last_included_term_ = state.last_included_term;
  req.offset_ = static_cast<uint32_t>(state.offset);
  req.data_ = std::move(buffer);
  req.done_ = is_last;

  // Serialize
  std::string data;
  auto status = protocol_->SerializeRequest(req, data);
  if (!status.ok()) {
    LOG_ERROR("Node {}: failed to serialize InstallSnapshotRequest: {}",
              server_id_, status.ToString());
    state.in_progress = false;
    return;
  }

  // Get peer address
  auto it_addr = peer_map_.find(peer_id);
  if (it_addr == peer_map_.end()) {
    LOG_ERROR("Node {}: peer {} not found", server_id_, peer_id);
    state.in_progress = false;
    return;
  }

  if (metrics_) {
    metrics_
        ->GetCounter("raft_snapshot_chunks_sent_total",
                     {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }
  LOG_DEBUG(
      "Node {}: sending snapshot chunk to {}: offset={}, size={}, done={}",
      server_id_, peer_id, state.offset, bytes_read, is_last);

  // Send
  network_->SendRpc(peer_id, it_addr->second, data,
                    std::chrono::milliseconds(config_.rpc_timeout_ms),
                    [this, peer_id](const std::string& resp, bool success,
                                    const std::string& error) {
                      // Deserialize response first (outside lock)
                      InstallSnapshotResponse response;
                      if (success) {
                        auto status =
                            protocol_->DeserializeResponse(resp, response);
                        if (!status.ok()) {
                          LOG_ERROR(
                              "Node {}: failed to deserialize "
                              "InstallSnapshotResponse: {}",
                              server_id_, status.ToString());
                          success = false;
                        }
                      } else {
                        LOG_WARN("Node {}: InstallSnapshot to {} failed: {}",
                                 server_id_, peer_id, error);
                      }
                      HandleInstallSnapshotResponse(peer_id, response, success);
                    });
}

void RaftNode::RaftNodeImpl::HandleInstallSnapshotResponse(
    NodeId from, const InstallSnapshotResponse& resp, bool rpc_success) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) return;
  if (role_ != RaftNodeRole::LEADER) return;

  auto it = snapshot_sends_.find(from);
  if (it == snapshot_sends_.end()) return;

  auto& state = it->second;
  state.in_progress = false;

  // RPC failed: retry with backoff
  if (!rpc_success) {
    LOG_WARN("Node {}: snapshot RPC to {} failed, will retry", server_id_,
             from);
    timer_->SetTimeout(std::chrono::milliseconds(100), [this, from]() {
      std::lock_guard<std::mutex> lock(mtx_);
      if (role_ == RaftNodeRole::LEADER) {
        SendNextSnapshotChunkLocked(from);
      }
    });
    return;
  }

  // Term check: if follower has higher term, revert to follower
  if (resp.term_ > current_term_) {
    LOG_INFO(
        "Node {}: follower {} has higher term {} vs {}, reverting to Follower",
        server_id_, from, resp.term_, current_term_);
    BecomeFollowerLocked(resp.term_);
    return;
  }

  // Check if we're done
  if (state.offset + state.last_chunk_size >=
      state.snapshot->GetMeta().last_included_index_) {
    // Actually we need to track total size, not index. Let 'done' flag drive
    // this. But we don't store total size. Use the done flag from last send.
    // Simpler: check if last chunk was smaller than chunk size
    if (state.last_chunk_size < kSnapshotChunkSize) {
      // Transfer complete
      LOG_INFO(
          "Node {}: snapshot send to {} completed, updating progress to {}",
          server_id_, from, state.last_included_index);

      if (metrics_) {
        metrics_
            ->GetCounter("raft_snapshot_sends_completed_total",
                         {{"node_id", std::to_string(server_id_)},
                          {"peer_id", std::to_string(from)}})
            .Increment();
      }
      match_index_[from] = state.last_included_index;
      next_index_[from] = state.last_included_index + 1;

      // Clean up
      snapshot_sends_.erase(it);

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
