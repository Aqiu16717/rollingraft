#include "raft_node_impl.h"

using namespace rollingraft;

void RaftNode::RaftNodeImpl::StartHeartbeatTimerLocked() {
  heartbeat_timer_ = timer_->SetInterval(
      std::chrono::milliseconds(config_.heartbeat_interval_ms),
      [this]() { OnHeartbeatTimeout(); });
}

void RaftNode::RaftNodeImpl::StopHeartbeatTimerLocked() {
  if (heartbeat_timer_ != 0) {
    timer_->CancelTimer(heartbeat_timer_);
    heartbeat_timer_ = 0;
  }
}

void RaftNode::RaftNodeImpl::OnHeartbeatTimeout() {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) return;
  if (role_ != RaftNodeRole::LEADER) return;

  BroadcastAppendEntriesLocked();
}

void RaftNode::RaftNodeImpl::BroadcastAppendEntriesLocked() {
  for (const auto& [peer_id, addr] : peer_map_) {
    (void)addr;
    SendAppendEntriesToPeerLocked(peer_id);
  }
}

void RaftNode::RaftNodeImpl::SendAppendEntriesToPeerLocked(NodeId peer_id) {
  auto it = next_index_.find(peer_id);
  if (it == next_index_.end()) return;

  Index next_idx = it->second;

  // Check if we need to send snapshot instead
  Index first_log_index = log_.GetFirstIndex();
  if (next_idx < first_log_index) {
    LOG_INFO(
        "Node {}: next_idx {} < first_log_index {}, sending snapshot to {}",
        server_id_, next_idx, first_log_index, peer_id);
    SendInstallSnapshotToPeerLocked(peer_id);
    return;
  }

  AppendEntriesRequest req;
  req.term_ = current_term_;
  req.leader_id_ = server_id_;
  req.prev_log_index_ = next_idx - 1;
  req.prev_log_term_ = GetLogTermLocked(req.prev_log_index_);
  req.leader_commit_ = commit_index_;

  // Get log entries — only send entries that have been durably flushed
  auto [last_index, _] = log_.GetLastLogInfo();
  Index effective_last = last_index;
  if (log_persister_) {
    effective_last = std::min(last_index, flushed_index_);
  }
  if (next_idx <= effective_last) {
    Index end =
        std::min(next_idx + config_.max_entries_per_append, effective_last + 1);
    req.entries_ = log_.GetEntries(next_idx, end);
  }

  // Serialize and send
  std::string data;
  auto status = protocol_->SerializeRequest(req, data);
  if (!status.ok()) {
    LOG_ERROR("Failed to serialize AppendEntriesRequest: {}",
              status.ToString());
    return;
  }

  auto it_addr = peer_map_.find(peer_id);
  if (it_addr == peer_map_.end()) return;

  if (metrics_) {
    metrics_->GetCounter("raft_appendentries_sent_total",
                         {{"node_id", std::to_string(server_id_)}, {"peer_id", std::to_string(peer_id)}})
        .Increment();
  }

  network_->SendRpc(
      peer_id, it_addr->second, data,
      std::chrono::milliseconds(config_.rpc_timeout_ms),
      [this, peer_id](const std::string& resp, bool success,
                      const std::string& error) {
        if (!success) {
          LOG_INFO("AppendEntries to {} failed: {}, will retry", peer_id,
                   error);
          // Trigger retry with backoff
          ScheduleAppendEntriesRetry(peer_id);
          return;
        }

        AppendEntriesResponse response;
        auto status = protocol_->DeserializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize AppendEntriesResponse: {}",
                    status.ToString());
          // Also retry on deserialization failure
          ScheduleAppendEntriesRetry(peer_id);
          return;
        }
        // Reset retry state on successful response
        retry_state_.erase(peer_id);
        HandleAppendEntriesResponse(peer_id, response);
      });
}

void RaftNode::RaftNodeImpl::ScheduleAppendEntriesRetry(NodeId peer_id) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning() || role_ != RaftNodeRole::LEADER) return;

  auto& retry = retry_state_[peer_id];
  retry.attempts++;

  if (retry.attempts > static_cast<int>(config_.max_retry_attempts)) {
    LOG_WARN("Node {}: max retry attempts ({}) reached for peer {}", server_id_,
             config_.max_retry_attempts, peer_id);
    retry_state_.erase(peer_id);
    return;
  }

  // Exponential backoff: delay = base * 2^attempts, capped at max
  uint32_t delay = config_.base_retry_delay_ms * (1u << retry.attempts);
  delay = std::min(delay, config_.max_retry_delay_ms);

  if (metrics_) {
    metrics_->GetCounter("raft_appendentries_retries_total",
                         {{"node_id", std::to_string(server_id_)}, {"peer_id", std::to_string(peer_id)}})
        .Increment();
  }
  LOG_INFO("Node {}: scheduling AppendEntries retry {} to peer {} in {}ms",
           server_id_, retry.attempts, peer_id, delay);

  timer_->SetTimeout(std::chrono::milliseconds(delay), [this, peer_id]() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (role_ == RaftNodeRole::LEADER) {
      SendAppendEntriesToPeerLocked(peer_id);
    }
  });
}

void RaftNode::RaftNodeImpl::HandleAppendEntriesResponse(
    NodeId from, const AppendEntriesResponse& resp) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) return;
  if (role_ != RaftNodeRole::LEADER) return;

  // If response term is higher, revert to Follower
  if (resp.term_ > current_term_) {
    BecomeFollowerLocked(resp.term_);
    return;
  }

  if (resp.success_) {
    if (metrics_) {
      metrics_->GetCounter("raft_appendentries_success_total",
                           {{"node_id", std::to_string(server_id_)}, {"peer_id", std::to_string(from)}})
          .Increment();
    }
    // Update progress
    Index new_match = next_index_[from] - 1 + resp.entries_count_;
    match_index_[from] = std::max(match_index_[from], new_match);
    next_index_[from] = match_index_[from] + 1;

    // Reset retry state on success
    retry_state_.erase(from);

    // Try to commit
    TryCommitLocked();
  } else {
    if (metrics_) {
      metrics_->GetCounter("raft_appendentries_failure_total",
                           {{"node_id", std::to_string(server_id_)}, {"peer_id", std::to_string(from)}})
          .Increment();
    }
    // Log mismatch, back off
    if (resp.conflict_index_ > 0) {
      next_index_[from] = resp.conflict_index_;
    } else {
      next_index_[from] = std::max<Index>(1, next_index_[from] - 1);
    }

    // Use exponential backoff retry for log mismatch too
    ScheduleAppendEntriesRetry(from);
  }
}

void RaftNode::RaftNodeImpl::TryCommitLocked() {
  auto [last_index, _] = log_.GetLastLogInfo();

  for (Index index = last_index; index > commit_index_; --index) {
    // Only commit entries from current term
    if (GetLogTermLocked(index) != current_term_) {
      break;
    }

    // Count logs replicated to majority.
    // The leader only counts itself if the entry is durably persisted.
    int count = 0;
    if (!log_persister_ || index <= flushed_index_) {
      count = 1;  // Self
    }
    for (const auto& [peer_id, match] : match_index_) {
      (void)peer_id;
      if (match >= index) ++count;
    }

    if (static_cast<size_t>(count) > (peer_addrs_.size() + 1) / 2) {
      commit_index_ = index;
      if (metrics_) {
        metrics_->GetCounter("raft_commits_total", {{"node_id", std::to_string(server_id_)}})
            .Increment();
        metrics_->GetGauge("raft_commit_index", {{"node_id", std::to_string(server_id_)}})
            .Set(static_cast<double>(commit_index_));
      }
      LOG_INFO("Node {} commit index advanced to {}", server_id_,
               commit_index_);
      ApplyCommittedLocked();
      break;
    }
  }
}

