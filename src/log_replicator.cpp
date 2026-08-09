#include "raft_node_impl.h"

using namespace rollingraft;

void RaftNode::RaftNodeImpl::StartHeartbeatTimerLocked(uint32_t interval_ms) {
  // PRECONDITION: group_->replication_mtx_ is held by caller
  // If interval_ms == 0, use the runtime-configured heartbeat interval.
  uint32_t interval = interval_ms;
  if (interval == 0) {
    auto cfg = infra_->runtime_config_->Get();
    interval = cfg.heartbeat_interval_ms;
  }
  group_->heartbeat_deadline_ = timer_->Now() + std::chrono::milliseconds(interval);
  group_->heartbeat_timer_enabled_ = true;
}

void RaftNode::RaftNodeImpl::StopHeartbeatTimerLocked() {
  // PRECONDITION: group_->replication_mtx_ is held by caller
  group_->heartbeat_timer_enabled_ = false;
}

void RaftNode::RaftNodeImpl::CheckHeartbeatTimeoutLocked() {
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);

  if (!group_->heartbeat_timer_enabled_ || timer_->Now() < group_->heartbeat_deadline_) {
    return;
  }

  // Re-schedule before running the handler so that a long handler does not
  // delay the next tick.  Use quiesced interval when in quiesced mode.
  auto cfg = infra_->runtime_config_->Get();
  uint32_t interval = cfg.heartbeat_interval_ms;
  if (group_->quiesced_.load(std::memory_order_acquire)) {
    interval = group_->config_.quiesced_heartbeat_interval_ms;
  }
  group_->heartbeat_deadline_ += std::chrono::milliseconds(interval);

  OnHeartbeatTimeoutLocked();
}

void RaftNode::RaftNodeImpl::OnHeartbeatTimeout() {
  // Bridge pattern: acquire group_->election_mtx_ first, then group_->replication_mtx_
  // to safely access both group_->role_ (election) and group_->next_index_ (replication).
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
  OnHeartbeatTimeoutLocked();
}

void RaftNode::RaftNodeImpl::OnHeartbeatTimeoutLocked() {
  if (!IsRunning()) {
    return;
  }
  if (group_->role_ != RaftNodeRole::LEADER) {
    return;
  }

  // Quiesced mode: if idle for too long, enter quiesced state and skip
  // empty heartbeats to reduce network/CPU overhead.
  if (ShouldEnterQuiescedLocked()) {
    EnterQuiescedLocked();
  }
  if (group_->quiesced_.load(std::memory_order_acquire)) {
    // In quiesced mode, only send heartbeats if there are pending reads
    // that need heartbeat acks. Otherwise skip entirely.
    bool has_pending_reads = false;
    {
      std::lock_guard<std::mutex> lock_a(group_->applier_mtx_);
      has_pending_reads = !group_->pending_reads_.empty();
    }
    if (!has_pending_reads) {
      return;
    }
  }

  // CheckQuorum: verify we still have majority acks before sending
  // next round of heartbeats.
  if (group_->check_quorum_enabled_) {
    CheckQuorumLocked();
  }

  if (group_->role_ != RaftNodeRole::LEADER) {
    return;
  }

  BroadcastAppendEntriesLocked();
  MaybeAutoPromoteLearnersLocked();
  MaybeRemoveDeadNodesLocked();
}

void RaftNode::RaftNodeImpl::MaybeRemoveDeadNodesLocked() {
  // PRECONDITION: group_->election_mtx_ and group_->replication_mtx_ are held by caller
  if (!IsRunning() || group_->role_ != RaftNodeRole::LEADER) {
    return;
  }
  if (!group_->config_.auto_remove_dead_nodes) {
    return;
  }

  auto now = std::chrono::steady_clock::now();

  // Collect dead nodes (copy to avoid modifying during iteration)
  std::vector<NodeId> dead_nodes;
  {
    std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
    for (NodeId peer_id : group_->cluster_config_.nodes) {
      if (peer_id == group_->server_id_) {
        continue;  // Skip self
      }

      auto it = group_->last_contact_time_.find(peer_id);
      if (it == group_->last_contact_time_.end()) {
        // Never contacted — treat as dead if we have been leader long enough
        dead_nodes.push_back(peer_id);
        continue;
      }

      auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second).count();
      if (elapsed >= 0 && static_cast<uint32_t>(elapsed) >= group_->config_.dead_node_timeout_ms) {
        dead_nodes.push_back(peer_id);
      }
    }
  }

  for (NodeId dead_id : dead_nodes) {
    // Safety check: ensure we still have quorum after removal
    {
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      uint32_t remaining = group_->cluster_config_.nodes.size() - 1;
      uint32_t majority_after_remove = remaining / 2 + 1;
      int active_voters = 1;  // Leader counts itself
      for (const auto& [peer_id, ack_time] : group_->quorum_acks_) {
        auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(now - ack_time).count();
        if (elapsed >= 0 &&
            static_cast<uint32_t>(elapsed) < infra_->runtime_config_->Get().election_timeout_ms &&
            group_->cluster_config_.IsVoter(peer_id) && peer_id != dead_id) {
          ++active_voters;
        }
      }
      if (static_cast<uint32_t>(active_voters) < majority_after_remove) {
        LOG_WARN(
            "Node {}: dead node {} detected but removing it would lose quorum ({} < {}), skipping",
            group_->server_id_, dead_id, active_voters, majority_after_remove);
        continue;
      }
    }

    LOG_INFO("Node {}: auto-removing dead node {} (no contact for {}ms)", group_->server_id_,
             dead_id, group_->config_.dead_node_timeout_ms);

    if (metrics_) {
      auto labels = group_->metrics_node_label_;
      labels["peer_id"] = std::to_string(dead_id);
      metrics_->GetCounter("raft_dead_nodes_detected_total", labels).Increment();
    }

    // Drop locks before calling RemoveNode (it acquires its own locks)
    // We already hold group_->election_mtx_ + group_->replication_mtx_, but RemoveNode
    // acquires them in the same order, so we need to drop first.
    // Actually RemoveNode acquires group_->election_mtx_ then group_->replication_mtx_ then
    // group_->membership_mtx_. Since we hold group_->election_mtx_ and group_->replication_mtx_,
    // calling RemoveNode would deadlock. Solution: drop our locks, call RemoveNode, then re-acquire
    // if needed. But OnHeartbeatTimeout is a timer callback — we can just return after RemoveNode.
    // However, we may have multiple dead nodes. Use a post-task approach.

    // Schedule removal asynchronously to avoid deadlock with current locks
    infra_->timer_->SetTimeout(std::chrono::milliseconds(1),
                               [weak_self = weak_from_this(), this, dead_id]() {
                                 auto keep_alive = weak_self.lock();
                                 if (!keep_alive) {
                                   return;
                                 }
                                 auto status = RemoveNode(dead_id);
                                 if (!status.ok()) {
                                   LOG_WARN("Node {}: auto-removal of dead node {} failed: {}",
                                            group_->server_id_, dead_id, status.ToString());
                                 }
                               });
  }
}

// Maximum log lag (in entries) for a learner to be auto-promoted.
void RaftNode::RaftNodeImpl::MaybeAutoPromoteLearnersLocked() {
  // PRECONDITION: group_->election_mtx_ and group_->replication_mtx_ are held by caller
  if (!IsRunning() || group_->role_ != RaftNodeRole::LEADER) {
    return;
  }

  std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
  for (NodeId learner_id : group_->cluster_config_.learners) {
    auto it = group_->match_index_.find(learner_id);
    if (it == group_->match_index_.end()) {
      continue;
    }

    // Promote when learner has caught up to all committed entries.
    if (it->second >= group_->commit_index_) {
      // Schedule promotion asynchronously to avoid lock re-entrancy.
      // PromoteLearner will check group_->pending_config_change_ internally.
      LOG_INFO("Node {} auto-promoting learner {} (match={} commit={})", group_->server_id_,
               learner_id, it->second, group_->commit_index_);
      infra_->timer_->SetTimeout(
          std::chrono::milliseconds(0), [weak_self = weak_from_this(), this, learner_id]() {
            auto keep_alive = weak_self.lock();
            if (!keep_alive) {
              return;
            }
            auto status = PromoteLearner(learner_id);
            if (!status.ok()) {
              LOG_DEBUG("Auto-promote learner {} failed: {}", learner_id, status.GetMessage());
            }
          });
    }
  }
}

void RaftNode::RaftNodeImpl::BroadcastAppendEntriesLocked() {
  for (const auto& [peer_id, addr] : group_->peer_map_) {
    (void)addr;
    // With pipeline replication, keep filling the window until
    // backpressure kicks in or there are no more entries.
    while (true) {
      Index before = group_->next_index_[peer_id];
      SendAppendEntriesToPeerLocked(peer_id);
      Index after = group_->next_index_[peer_id];
      // If group_->next_index_ didn't advance, nothing was sent (pipeline full
      // or no entries). Stop trying for this peer.
      if (after == before) {
        break;
      }
    }
  }
}

void RaftNode::RaftNodeImpl::SendAppendEntriesToPeerLocked(NodeId peer_id) {
  auto it = group_->next_index_.find(peer_id);
  if (it == group_->next_index_.end()) {
    return;
  }

  Index next_idx = it->second;

  // Check if we need to send snapshot instead
  Index first_log_index = group_->log_.GetFirstIndex();
  if (next_idx < first_log_index) {
    LOG_INFO("Node {}: next_idx {} < first_log_index {}, sending snapshot to {}",
             group_->server_id_, next_idx, first_log_index, peer_id);
    // Bridge: group_->replication_mtx_ -> group_->snapshot_mtx_ per lock hierarchy
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    SendInstallSnapshotToPeerLocked(peer_id);
    return;
  }

  AppendEntriesRequest req;
  req.group_id = group_->group_id_;
  req.term_ = group_->current_term_;
  req.leader_id_ = group_->server_id_;
  req.prev_log_index_ = next_idx - 1;
  req.prev_log_term_ = GetLogTermLocked(req.prev_log_index_);
  req.leader_commit_ = group_->commit_index_;
  req.correlation_id_ = next_correlation_id_.fetch_add(1, std::memory_order_relaxed);

  // Get log entries — in group commit mode we send entries that are
  // in the in-memory log, not just fsynced ones. The pipeline ensures
  // followers receive them promptly.
  auto [last_index, _] = group_->log_.GetLastLogInfo();
  Index effective_last = last_index;
  if (next_idx <= effective_last) {
    auto cfg = infra_->runtime_config_->Get();
    Index end = std::min(next_idx + cfg.max_entries_per_append, effective_last + 1);
    req.entries_ = group_->log_.GetEntries(next_idx, end);
  }

  bool is_heartbeat = req.entries_.empty();

  // Heartbeats bypass pipeline window to ensure liveness.
  if (!is_heartbeat) {
    auto cfg = infra_->runtime_config_->Get();
    size_t window = cfg.max_pipeline_window;
    size_t inflight_count = 0;
    auto it_inflight = group_->inflight_.find(peer_id);
    if (it_inflight != group_->inflight_.end()) {
      for (const auto& entry : it_inflight->second) {
        inflight_count += entry.count;
      }
    }
    if (inflight_count >= window) {
      LOG_DEBUG("Node {}: pipeline full on peer {}, inflight={}/{}", group_->server_id_, peer_id,
                inflight_count, window);
      return;
    }
  }

  // Serialize and send
  std::string data;
  auto status = infra_->protocol_->SerializeRequest(req, data);
  if (!status.ok()) {
    LOG_ERROR("Failed to serialize AppendEntriesRequest: {}", status.ToString());
    return;
  }

  auto it_addr = group_->peer_map_.find(peer_id);
  if (it_addr == group_->peer_map_.end()) {
    return;
  }

  if (!is_heartbeat) {
    // Track this batch in the pipeline.
    group_->inflight_[peer_id].push_back({next_idx, req.entries_.size()});
    // Advance group_->next_index_ immediately so subsequent sends continue
    // filling the pipeline without waiting for the RPC response.
    group_->next_index_[peer_id] = next_idx + req.entries_.size();
  } else {
    group_->last_heartbeat_sent_[peer_id] = std::chrono::steady_clock::now();
  }

  if (metrics_) {
    auto labels = group_->metrics_node_label_;
    labels["peer_id"] = std::to_string(peer_id);
    metrics_->GetCounter("raft_appendentries_sent_total", labels).Increment();
  }

  {
    auto cfg = infra_->runtime_config_->Get();
    infra_->network_->SendRpc(
        peer_id, it_addr->second, data, req.correlation_id_,
        std::chrono::milliseconds(cfg.rpc_timeout_ms),
        [weak_self = weak_from_this(), this, peer_id, is_heartbeat](
            const std::string& resp, bool success, const std::string& error) {
          auto keep_alive = weak_self.lock();
          if (!keep_alive) {
            return;
          }
          if (!success) {
            LOG_INFO("AppendEntries to {} failed: {}, will retry", peer_id, error);
            ScheduleAppendEntriesRetry(peer_id, is_heartbeat);
            return;
          }

          AppendEntriesResponse response;
          auto status = infra_->protocol_->DeserializeResponse(resp, response);
          if (!status.ok()) {
            LOG_ERROR("Failed to deserialize AppendEntriesResponse: {}", status.ToString());
            ScheduleAppendEntriesRetry(peer_id, is_heartbeat);
            return;
          }
          if (is_heartbeat) {
            HandleHeartbeatResponse(peer_id, response);
          } else {
            HandleAppendEntriesResponse(peer_id, response);
          }
        });
  }
}

void RaftNode::RaftNodeImpl::ScheduleAppendEntriesRetry(NodeId peer_id, bool is_heartbeat) {
  // Bridge pattern: group_->election_mtx_ first, then group_->replication_mtx_
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);

  // Network failure: pop the failed batch from pipeline and reset group_->next_index_,
  // but only for non-heartbeat requests. Heartbeats are not tracked in inflight.
  if (!is_heartbeat) {
    auto it_inflight = group_->inflight_.find(peer_id);
    if (it_inflight != group_->inflight_.end() && !it_inflight->second.empty()) {
      auto head = it_inflight->second.front();
      it_inflight->second.pop_front();
      group_->next_index_[peer_id] = head.start_index;
      it_inflight->second.clear();
    }
  }

  ScheduleAppendEntriesRetryLocked(peer_id);
}

void RaftNode::RaftNodeImpl::ScheduleAppendEntriesRetryLocked(NodeId peer_id) {
  // Precondition: caller holds group_->election_mtx_ + group_->replication_mtx_
  if (!IsRunning() || group_->role_ != RaftNodeRole::LEADER) {
    return;
  }

  auto& retry = group_->retry_state_[peer_id];
  retry.attempts++;

  auto cfg = infra_->runtime_config_->Get();

  if (retry.attempts > static_cast<int>(cfg.max_retry_attempts)) {
    LOG_WARN("Node {}: max retry attempts ({}) reached for peer {}", group_->server_id_,
             cfg.max_retry_attempts, peer_id);
    group_->retry_state_.erase(peer_id);
    return;
  }

  // Exponential backoff: delay = base * 2^attempts, capped at max
  uint32_t delay = cfg.base_retry_delay_ms * (1u << retry.attempts);
  delay = std::min(delay, cfg.max_retry_delay_ms);

  if (metrics_) {
    auto labels = group_->metrics_node_label_;
    labels["peer_id"] = std::to_string(peer_id);
    metrics_->GetCounter("raft_appendentries_retries_total", labels).Increment();
  }
  LOG_INFO("Node {}: scheduling AppendEntries retry {} to peer {} in {}ms", group_->server_id_,
           retry.attempts, peer_id, delay);

  infra_->timer_->SetTimeout(std::chrono::milliseconds(delay),
                             [weak_self = weak_from_this(), this, peer_id]() {
                               auto keep_alive = weak_self.lock();
                               if (!keep_alive) {
                                 return;
                               }
                               std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
                               std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
                               if (group_->role_ == RaftNodeRole::LEADER) {
                                 SendAppendEntriesToPeerLocked(peer_id);
                               }
                             });
}

void RaftNode::RaftNodeImpl::HandleAppendEntriesResponse(NodeId from,
                                                         const AppendEntriesResponse& resp) {
  // Bridge pattern: group_->election_mtx_ first, then group_->replication_mtx_
  // HandleAppendEntriesResponse may trigger leader step-down (election state)
  // and updates group_->match_index_/group_->next_index_ (replication state).
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::unique_lock<std::mutex> lock_r(group_->replication_mtx_);

  if (!IsRunning()) {
    return;
  }
  if (group_->role_ != RaftNodeRole::LEADER) {
    return;
  }

  // If response term is higher, revert to Follower
  if (resp.term_ > group_->current_term_) {
    // Drop group_->replication_mtx_ before calling BecomeFollowerLocked,
    // which acquires group_->replication_mtx_ + group_->snapshot_mtx_ internally.
    // group_->election_mtx_ remains held, so the transition is serialized.
    lock_r.unlock();
    BecomeFollowerLocked(resp.term_);
    return;
  }

  // Process pipeline head for this peer.
  auto it_inflight = group_->inflight_.find(from);
  if (it_inflight == group_->inflight_.end() || it_inflight->second.empty()) {
    // Late response or peer not in pipeline (e.g., heartbeat).
    // Heartbeats don't track inflight, so just handle success/failure
    // for liveness but don't touch group_->match_index_ based on stale state.
    if (!resp.success_) {
      // A stale failure must not rewind next_index_ past already-acked
      // progress (match_index_ + 1 is the floor).
      Index floor = group_->match_index_[from] + 1;
      group_->next_index_[from] = std::max({floor, group_->next_index_[from] - 1});
      ScheduleAppendEntriesRetryLocked(from);
    }
    return;
  }

  // Pop the head of the inflight queue (FIFO, TCP preserves order).
  auto head = it_inflight->second.front();
  it_inflight->second.pop_front();

  if (resp.success_) {
    if (metrics_) {
      auto labels = group_->metrics_node_label_;
      labels["peer_id"] = std::to_string(from);
      metrics_->GetCounter("raft_appendentries_success_total", labels).Increment();
    }
    // Update progress based on the actual start index of this batch.
    Index new_match = head.start_index + resp.entries_count_ - 1;
    group_->match_index_[from] = std::max(group_->match_index_[from], new_match);
    // group_->next_index_ was already advanced when sending; ensure it stays >= match+1.
    group_->next_index_[from] = std::max(group_->next_index_[from], group_->match_index_[from] + 1);
    SetPeerReplicationLagMetricLocked(from);

    // Reset retry state on success
    group_->retry_state_.erase(from);

    // Track last contact time for dead node detection
    group_->last_contact_time_[from] = std::chrono::steady_clock::now();

    // CheckQuorum: track successful AppendEntries acks for quorum detection
    if (group_->check_quorum_enabled_) {
      group_->quorum_acks_[from] = std::chrono::steady_clock::now();
    }

    // Update leader lease if we have majority voter acks
    {
      auto now = std::chrono::steady_clock::now();
      auto cfg = infra_->runtime_config_->Get();
      int ack_count = 1;  // Leader counts itself
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      for (const auto& [peer_id, ack_time] : group_->quorum_acks_) {
        auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(now - ack_time).count();
        if (elapsed >= 0 && static_cast<uint32_t>(elapsed) < cfg.election_timeout_ms &&
            group_->cluster_config_.IsVoter(peer_id)) {
          ++ack_count;
        }
      }
      if (static_cast<uint32_t>(ack_count) >= group_->cluster_config_.GetMajority()) {
        group_->leader_lease_expiry_ = now + std::chrono::milliseconds(cfg.election_timeout_ms);
      }
      UpdateLeaderLeaseMetricLocked();
    }

    // Coalescing: regular heartbeat acks also count for ReadIndex,
    // so insert acks for all pending reads from this peer.
    if (!group_->pending_reads_.empty()) {
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      std::lock_guard<std::mutex> lock_a(group_->applier_mtx_);
      for (auto& [read_id, read_req] : group_->pending_reads_) {
        read_req.acks.insert(from);
      }
    }

    // Try to commit
    TryCommitLocked();

    // Fill the pipeline if there are more entries to send.
    auto [last_index, _] = group_->log_.GetLastLogInfo();
    if (group_->next_index_[from] <= last_index) {
      SendAppendEntriesToPeerLocked(from);
    }
  } else {
    if (metrics_) {
      auto labels = group_->metrics_node_label_;
      labels["peer_id"] = std::to_string(from);
      metrics_->GetCounter("raft_appendentries_failure_total", labels).Increment();
    }
    // Log mismatch: clear all inflight entries for this peer because
    // subsequent in-flight batches are now invalid (prefix missing).
    it_inflight->second.clear();

    // Reset group_->next_index_ to retry from the conflict point.
    if (resp.conflict_index_ > 0) {
      group_->next_index_[from] = resp.conflict_index_;
      // If follower reports conflict at or before our group_->match_index_, our
      // group_->match_index_ is stale (follower truncated or diverged). Reset it.
      if (resp.conflict_index_ <= group_->match_index_[from]) {
        group_->match_index_[from] = resp.conflict_index_ - 1;
      }
    } else {
      group_->next_index_[from] = std::max<Index>(1, head.start_index - 1);
      // If we're backing up before group_->match_index_, reset group_->match_index_ too.
      if (group_->next_index_[from] <= group_->match_index_[from]) {
        group_->match_index_[from] = group_->next_index_[from] - 1;
      }
    }
    if (group_->next_index_[from] < 1) {
      group_->next_index_[from] = 1;
    }
    SetPeerReplicationLagMetricLocked(from);

    // Even on failure, the peer is alive — update contact time
    group_->last_contact_time_[from] = std::chrono::steady_clock::now();

    // Use exponential backoff retry for log mismatch too
    ScheduleAppendEntriesRetryLocked(from);
  }
}

void RaftNode::RaftNodeImpl::HandleHeartbeatResponse(NodeId from,
                                                     const AppendEntriesResponse& resp) {
  // Bridge pattern: group_->election_mtx_ first, then group_->replication_mtx_
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::unique_lock<std::mutex> lock_r(group_->replication_mtx_);

  if (!IsRunning()) {
    return;
  }
  if (group_->role_ != RaftNodeRole::LEADER) {
    return;
  }

  // If response term is higher, revert to Follower
  if (resp.term_ > group_->current_term_) {
    lock_r.unlock();
    BecomeFollowerLocked(resp.term_);
    return;
  }

  if (!resp.success_) {
    // Stale term or other rejection — schedule retry
    ScheduleAppendEntriesRetryLocked(from);
    return;
  }

  // Heartbeat ack: update quorum tracking and leader lease
  if (group_->check_quorum_enabled_) {
    group_->quorum_acks_[from] = std::chrono::steady_clock::now();
  }
  // Track last contact time for dead node detection
  group_->last_contact_time_[from] = std::chrono::steady_clock::now();

  {
    auto now = std::chrono::steady_clock::now();
    auto cfg = infra_->runtime_config_->Get();
    int ack_count = 1;  // Leader counts itself
    std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
    for (const auto& [peer_id, ack_time] : group_->quorum_acks_) {
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - ack_time).count();
      if (elapsed >= 0 && static_cast<uint32_t>(elapsed) < cfg.election_timeout_ms &&
          group_->cluster_config_.IsVoter(peer_id)) {
        ++ack_count;
      }
    }
    if (static_cast<uint32_t>(ack_count) >= group_->cluster_config_.GetMajority()) {
      group_->leader_lease_expiry_ = now + std::chrono::milliseconds(cfg.election_timeout_ms);
    }
    UpdateLeaderLeaseMetricLocked();
  }

  // Coalescing: heartbeat acks also count for ReadIndex
  if (!group_->pending_reads_.empty()) {
    std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
    std::lock_guard<std::mutex> lock_a(group_->applier_mtx_);
    for (auto& [read_id, read_req] : group_->pending_reads_) {
      read_req.acks.insert(from);
    }
  }
}

void RaftNode::RaftNodeImpl::CheckQuorumLocked() {
  // PRECONDITION: group_->election_mtx_ is held by caller
  if (!IsRunning() || group_->role_ != RaftNodeRole::LEADER) {
    return;
  }

  auto cfg = infra_->runtime_config_->Get();
  auto now = std::chrono::steady_clock::now();

  // Collect VOTERS that have acked within election_timeout. The quorum check
  // must satisfy both configs during joint consensus.
  std::set<NodeId> acked_voters{group_->server_id_};  // Leader counts itself
  for (const auto& [peer_id, ack_time] : group_->quorum_acks_) {
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - ack_time).count();
    if (elapsed >= 0 && static_cast<uint32_t>(elapsed) < cfg.election_timeout_ms) {
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      if (group_->cluster_config_.IsVoter(peer_id)) {
        acked_voters.insert(peer_id);
      }
    }
  }

  if (!ElectionQuorumSatisfiedLocked(acked_voters)) {
    LOG_WARN("Node {} lost quorum (acks={}), stepping down from leadership", group_->server_id_,
             acked_voters.size());
    if (metrics_) {
      metrics_->GetCounter("raft_checkquorum_stepdown_total", group_->metrics_node_label_)
          .Increment();
    }
    BecomeFollowerLocked(group_->current_term_);
  }
}

void RaftNode::RaftNodeImpl::TryCommitLocked() {
  auto [last_index, _] = group_->log_.GetLastLogInfo();

  // Durability gate: only entries up to flushed_index_ (locally fsynced) may
  // be committed. A "committed" entry must survive a crash of this leader —
  // committing beyond the durable watermark could acknowledge data to clients
  // that exists only in memory across the whole cluster.
  Index commit_ceiling = std::min(last_index, group_->flushed_index_);

  for (Index index = commit_ceiling; index > group_->commit_index_; --index) {
    // Only commit entries from current term
    if (GetLogTermLocked(index) != group_->current_term_) {
      break;
    }

    // Count logs replicated to majority.
    // Joint consensus: need both old and new majorities.
    int old_count = 0;
    int new_count = 0;
    bool leader_in_new = false;
    bool leader_in_old = false;

    {
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      leader_in_new = group_->cluster_config_.IsVoter(group_->server_id_);
      leader_in_old = group_->cluster_config_.is_joint &&
                      std::find(group_->cluster_config_.old_nodes.begin(),
                                group_->cluster_config_.old_nodes.end(),
                                group_->server_id_) != group_->cluster_config_.old_nodes.end();
    }

    // Group commit: leader counts itself as long as the entry is in
    // the local log (even if not yet fsynced). Durability is ensured
    // by the fact that a majority (including followers) must have the
    // entry before it is committed.
    if (leader_in_new) {
      ++new_count;
    }
    if (leader_in_old) {
      ++old_count;
    }

    for (const auto& [peer_id, match] : group_->match_index_) {
      if (match >= index) {
        bool peer_in_new = false;
        bool peer_in_old = false;
        {
          std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
          peer_in_new = group_->cluster_config_.IsVoter(peer_id);
          peer_in_old = group_->cluster_config_.is_joint &&
                        std::find(group_->cluster_config_.old_nodes.begin(),
                                  group_->cluster_config_.old_nodes.end(),
                                  peer_id) != group_->cluster_config_.old_nodes.end();
        }
        if (peer_in_new) {
          ++new_count;
        }
        if (peer_in_old) {
          ++old_count;
        }
      }
    }

    bool can_commit = false;
    {
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      can_commit = group_->cluster_config_.JointMajoritySatisfied(old_count, new_count);
    }

    if (can_commit) {
      group_->commit_index_ = index;
      if (metrics_) {
        infra_->metrics_->GetCounter("raft_commits_total", group_->metrics_node_label_).Increment();
        infra_->metrics_->GetGauge("raft_commit_index", group_->metrics_node_label_)
            .Set(static_cast<double>(group_->commit_index_));
      }
      LOG_INFO("Node {} commit index advanced to {}", group_->server_id_, group_->commit_index_);
      ApplyCommittedLocked();
      break;
    }
  }
}
