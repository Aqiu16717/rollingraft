#include "raft_node_impl.h"

using namespace rollingraft;

void RaftNode::RaftNodeImpl::StartHeartbeatTimerLocked() {
  // PRECONDITION: replication_mtx_ is held by caller
  auto cfg = runtime_config_->Get();
  heartbeat_timer_ = timer_->SetInterval(
      std::chrono::milliseconds(cfg.heartbeat_interval_ms),
      [this]() { OnHeartbeatTimeout(); });
}

void RaftNode::RaftNodeImpl::StopHeartbeatTimerLocked() {
  // PRECONDITION: replication_mtx_ is held by caller
  if (heartbeat_timer_ != 0) {
    timer_->CancelTimer(heartbeat_timer_);
    heartbeat_timer_ = 0;
  }
}

void RaftNode::RaftNodeImpl::OnHeartbeatTimeout() {
  // Bridge pattern: acquire election_mtx_ first, then replication_mtx_
  // to safely access both role_ (election) and next_index_ (replication).
  std::lock_guard<std::mutex> lock_e(election_mtx_);
  std::lock_guard<std::mutex> lock_r(replication_mtx_);

  if (!IsRunning()) return;
  if (role_ != RaftNodeRole::LEADER) return;

  // CheckQuorum: verify we still have majority acks before sending
  // next round of heartbeats.
  if (check_quorum_enabled_) {
    CheckQuorumLocked();
  }

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
    // Bridge: replication_mtx_ -> snapshot_mtx_ per lock hierarchy
    std::lock_guard<std::mutex> lock_s(snapshot_mtx_);
    SendInstallSnapshotToPeerLocked(peer_id);
    return;
  }

  AppendEntriesRequest req;
  req.term_ = current_term_;
  req.leader_id_ = server_id_;
  req.prev_log_index_ = next_idx - 1;
  req.prev_log_term_ = GetLogTermLocked(req.prev_log_index_);
  req.leader_commit_ = commit_index_;
  req.correlation_id_ =
      next_correlation_id_.fetch_add(1, std::memory_order_relaxed);

  // Get log entries — only send entries that have been durably flushed
  auto [last_index, _] = log_.GetLastLogInfo();
  Index effective_last = last_index;
  if (log_persister_) {
    effective_last = std::min(last_index, flushed_index_);
  }
  if (next_idx <= effective_last) {
    auto cfg = runtime_config_->Get();
    Index end =
        std::min(next_idx + cfg.max_entries_per_append, effective_last + 1);
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

  // Backpressure: limit in-flight AppendEntries per peer to prevent
  // memory unbounded growth when a follower is slow or partitioned.
  // Heartbeats (empty entries) bypass backpressure to ensure liveness.
  constexpr size_t kMaxPendingAppends = 3;
  bool is_heartbeat = req.entries_.empty();
  if (!is_heartbeat && pending_appends_[peer_id] >= kMaxPendingAppends) {
    LOG_DEBUG("Node {}: backpressure on peer {}, pending={}", server_id_,
              peer_id, pending_appends_[peer_id]);
    return;
  }
  if (!is_heartbeat) {
    pending_appends_[peer_id]++;
  }

  if (metrics_) {
    metrics_
        ->GetCounter("raft_appendentries_sent_total",
                     {{"node_id", std::to_string(server_id_)},
                      {"peer_id", std::to_string(peer_id)}})
        .Increment();
  }

  {
    auto cfg = runtime_config_->Get();
    network_->SendRpc(
        peer_id, it_addr->second, data, req.correlation_id_,
        std::chrono::milliseconds(cfg.rpc_timeout_ms),
      [this, peer_id, is_heartbeat](const std::string& resp, bool success,
                      const std::string& error) {
        // Decrement backpressure counter only for non-heartbeat AppendEntries.
        if (!is_heartbeat) {
          pending_appends_[peer_id]--;
        }

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
}

void RaftNode::RaftNodeImpl::ScheduleAppendEntriesRetry(NodeId peer_id) {
  // Bridge pattern: election_mtx_ first, then replication_mtx_
  std::lock_guard<std::mutex> lock_e(election_mtx_);
  std::lock_guard<std::mutex> lock_r(replication_mtx_);
  ScheduleAppendEntriesRetryLocked(peer_id);
}

void RaftNode::RaftNodeImpl::ScheduleAppendEntriesRetryLocked(NodeId peer_id) {
  // Precondition: caller holds election_mtx_ + replication_mtx_
  if (!IsRunning() || role_ != RaftNodeRole::LEADER) return;

  auto& retry = retry_state_[peer_id];
  retry.attempts++;

  auto cfg = runtime_config_->Get();

  if (retry.attempts > static_cast<int>(cfg.max_retry_attempts)) {
    LOG_WARN("Node {}: max retry attempts ({}) reached for peer {}", server_id_,
             cfg.max_retry_attempts, peer_id);
    retry_state_.erase(peer_id);
    return;
  }

  // Exponential backoff: delay = base * 2^attempts, capped at max
  uint32_t delay = cfg.base_retry_delay_ms * (1u << retry.attempts);
  delay = std::min(delay, cfg.max_retry_delay_ms);

  if (metrics_) {
    metrics_
        ->GetCounter("raft_appendentries_retries_total",
                     {{"node_id", std::to_string(server_id_)},
                      {"peer_id", std::to_string(peer_id)}})
        .Increment();
  }
  LOG_INFO("Node {}: scheduling AppendEntries retry {} to peer {} in {}ms",
           server_id_, retry.attempts, peer_id, delay);

  timer_->SetTimeout(std::chrono::milliseconds(delay), [this, peer_id]() {
    std::lock_guard<std::mutex> lock_e(election_mtx_);
    std::lock_guard<std::mutex> lock_r(replication_mtx_);
    if (role_ == RaftNodeRole::LEADER) {
      SendAppendEntriesToPeerLocked(peer_id);
    }
  });
}

void RaftNode::RaftNodeImpl::HandleAppendEntriesResponse(
    NodeId from, const AppendEntriesResponse& resp) {
  // Bridge pattern: election_mtx_ first, then replication_mtx_
  // HandleAppendEntriesResponse may trigger leader step-down (election state)
  // and updates match_index_/next_index_ (replication state).
  std::lock_guard<std::mutex> lock_e(election_mtx_);
  std::unique_lock<std::mutex> lock_r(replication_mtx_);

  if (!IsRunning()) return;
  if (role_ != RaftNodeRole::LEADER) return;

  // If response term is higher, revert to Follower
  if (resp.term_ > current_term_) {
    // Drop replication_mtx_ before calling BecomeFollowerLocked,
    // which acquires replication_mtx_ + snapshot_mtx_ internally.
    // election_mtx_ remains held, so the transition is serialized.
    lock_r.unlock();
    BecomeFollowerLocked(resp.term_);
    return;
  }

  if (resp.success_) {
    if (metrics_) {
      metrics_
          ->GetCounter("raft_appendentries_success_total",
                       {{"node_id", std::to_string(server_id_)},
                        {"peer_id", std::to_string(from)}})
          .Increment();
    }
    // Update progress
    Index new_match = next_index_[from] - 1 + resp.entries_count_;
    match_index_[from] = std::max(match_index_[from], new_match);
    next_index_[from] = match_index_[from] + 1;

    // Reset retry state on success
    retry_state_.erase(from);

    // CheckQuorum: track successful AppendEntries acks for quorum detection
    if (check_quorum_enabled_) {
      quorum_acks_[from] = std::chrono::steady_clock::now();
    }

    // Try to commit
    TryCommitLocked();
  } else {
    if (metrics_) {
      metrics_
          ->GetCounter("raft_appendentries_failure_total",
                       {{"node_id", std::to_string(server_id_)},
                        {"peer_id", std::to_string(from)}})
          .Increment();
    }
    // Log mismatch, back off
    if (resp.conflict_index_ > 0) {
      next_index_[from] = resp.conflict_index_;
    } else {
      next_index_[from] = std::max<Index>(1, next_index_[from] - 1);
    }

    // Use exponential backoff retry for log mismatch too
    ScheduleAppendEntriesRetryLocked(from);
  }
}

void RaftNode::RaftNodeImpl::CheckQuorumLocked() {
  // PRECONDITION: election_mtx_ is held by caller
  if (!IsRunning() || role_ != RaftNodeRole::LEADER) return;

  auto cfg = runtime_config_->Get();
  auto now = std::chrono::steady_clock::now();

  // Count how many nodes have acked within election_timeout
  int ack_count = 1;  // Leader counts itself
  for (const auto& [peer_id, ack_time] : quorum_acks_) {
    (void)peer_id;
    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - ack_time)
            .count();
    if (elapsed >= 0 &&
        static_cast<uint32_t>(elapsed) < cfg.election_timeout_ms) {
      ++ack_count;
    }
  }

  uint32_t majority;
  {
    std::shared_lock<std::shared_mutex> lock_m(membership_mtx_);
    majority = cluster_config_.GetMajority();
  }

  if (static_cast<uint32_t>(ack_count) < majority) {
    LOG_WARN(
        "Node {} lost quorum (acks={}/{}), stepping down from leadership",
        server_id_, ack_count, majority);
    if (metrics_) {
      metrics_
          ->GetCounter("raft_checkquorum_stepdown_total",
                       {{"node_id", std::to_string(server_id_)}})
          .Increment();
    }
    BecomeFollowerLocked(current_term_);
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
    // Joint consensus: need both old and new majorities.
    int old_count = 0;
    int new_count = 0;
    bool leader_in_new = false;
    bool leader_in_old = false;

    {
      std::shared_lock<std::shared_mutex> lock_m(membership_mtx_);
      leader_in_new = cluster_config_.Contains(server_id_);
      leader_in_old =
          cluster_config_.is_joint &&
          std::find(cluster_config_.old_nodes.begin(),
                    cluster_config_.old_nodes.end(),
                    server_id_) != cluster_config_.old_nodes.end();
    }

    if (!log_persister_ || index <= flushed_index_) {
      if (leader_in_new) ++new_count;
      if (leader_in_old) ++old_count;
    }

    for (const auto& [peer_id, match] : match_index_) {
      if (match >= index) {
        bool peer_in_new = false;
        bool peer_in_old = false;
        {
          std::shared_lock<std::shared_mutex> lock_m(membership_mtx_);
          peer_in_new = cluster_config_.Contains(peer_id);
          peer_in_old =
              cluster_config_.is_joint &&
              std::find(cluster_config_.old_nodes.begin(),
                        cluster_config_.old_nodes.end(),
                        peer_id) != cluster_config_.old_nodes.end();
        }
        if (peer_in_new) ++new_count;
        if (peer_in_old) ++old_count;
      }
    }

    bool can_commit = false;
    {
      std::shared_lock<std::shared_mutex> lock_m(membership_mtx_);
      can_commit = cluster_config_.JointMajoritySatisfied(old_count, new_count);
    }

    if (can_commit) {
      commit_index_ = index;
      if (metrics_) {
        metrics_
            ->GetCounter("raft_commits_total",
                         {{"node_id", std::to_string(server_id_)}})
            .Increment();
        metrics_
            ->GetGauge("raft_commit_index",
                       {{"node_id", std::to_string(server_id_)}})
            .Set(static_cast<double>(commit_index_));
      }
      LOG_INFO("Node {} commit index advanced to {}", server_id_,
               commit_index_);
      ApplyCommittedLocked();
      break;
    }
  }
}
