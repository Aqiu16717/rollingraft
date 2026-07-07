#include "raft_node_impl.h"

using namespace rollingraft;

void RaftNode::RaftNodeImpl::BecomeFollowerLocked(Term term) {
  RaftNodeRole old_role = group_->role_;

  group_->role_ = RaftNodeRole::FOLLOWER;
  group_->current_term_ = term;
  group_->voted_for_ = -1;
  group_->vote_count_ = 0;
  group_->leader_id_ = -1;
  group_->leader_addr_.clear();

  // Stop leader timers and clear pipeline state
  {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    StopHeartbeatTimerLocked();
    group_->inflight_.clear();
  }
  {
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    StopSnapshotCheckTimerLocked();
  }

  // Reset and start election timer
  ResetElectionTimerLocked();

  // Persist state
  if (persister_) {
    auto persist_status = persister_->SaveState({group_->current_term_, group_->voted_for_});
    if (!persist_status.ok()) {
      LOG_ERROR("Node {} failed to persist state when becoming Follower: {} — aborting",
                group_->server_id_, persist_status.GetMessage());
      std::abort();
    }
  }

  // Publish event
  if (old_role != group_->role_) {
    NodeRoleChangedEvent event;
    event.node_id = group_->server_id_;
    event.old_role = old_role;
    event.new_role = group_->role_;
    event.term = group_->current_term_;
    event.timestamp = std::chrono::steady_clock::now();
    event_bus_.Publish(event);
  }

  // Invoke callback
  if (old_role != group_->role_ && group_->role_change_callback_) {
    group_->role_change_callback_(group_->role_, group_->current_term_);
  }

  if (metrics_) {
    infra_->metrics_->GetGauge("raft_role", {{"node_id", std::to_string(group_->server_id_)}})
        .Set(static_cast<double>(RaftNodeRole::FOLLOWER));
    infra_->metrics_
        ->GetGauge("raft_current_term", {{"node_id", std::to_string(group_->server_id_)}})
        .Set(static_cast<double>(group_->current_term_));
    infra_->metrics_
        ->GetGauge("raft_leader_lease_seconds", {{"node_id", std::to_string(group_->server_id_)}})
        .Set(0.0);
  }
  UpdateLeaderLeaseMetricLocked();
  LOG_INFO("Node {} became Follower at term {}", group_->server_id_, group_->current_term_);
}

void RaftNode::RaftNodeImpl::BecomeCandidateLocked() {
  RaftNodeRole old_role = group_->role_;

  group_->role_ = RaftNodeRole::CANDIDATE;
  ++group_->current_term_;
  group_->voted_for_ = group_->server_id_;
  group_->vote_count_ = 1;  // Vote for self

  // Persist state
  if (persister_) {
    auto persist_status = persister_->SaveState({group_->current_term_, group_->voted_for_});
    if (!persist_status.ok()) {
      LOG_ERROR("Node {} failed to persist state when becoming Candidate: {} — aborting",
                group_->server_id_, persist_status.GetMessage());
      std::abort();
    }
  }

  // Publish event
  if (old_role != group_->role_) {
    NodeRoleChangedEvent event;
    event.node_id = group_->server_id_;
    event.old_role = old_role;
    event.new_role = group_->role_;
    event.term = group_->current_term_;
    event.timestamp = std::chrono::steady_clock::now();
    event_bus_.Publish(event);
  }

  // Invoke callback
  if (old_role != group_->role_ && group_->role_change_callback_) {
    group_->role_change_callback_(group_->role_, group_->current_term_);
  }

  if (metrics_) {
    infra_->metrics_
        ->GetCounter("raft_elections_total", {{"node_id", std::to_string(group_->server_id_)}})
        .Increment();
    infra_->metrics_->GetGauge("raft_role", {{"node_id", std::to_string(group_->server_id_)}})
        .Set(static_cast<double>(RaftNodeRole::CANDIDATE));
    infra_->metrics_
        ->GetGauge("raft_current_term", {{"node_id", std::to_string(group_->server_id_)}})
        .Set(static_cast<double>(group_->current_term_));
  }
  LOG_INFO("Node {} became Candidate at term {}", group_->server_id_, group_->current_term_);

  // Use committed cluster configuration for quorum calculation.
  // group_->peer_addrs_ may contain optimistically added nodes that have not
  // yet been committed via log, so it must not be used for majority.
  uint32_t majority;
  {
    std::shared_lock<std::shared_mutex> lock(group_->membership_mtx_);
    majority = group_->cluster_config_.GetMajority();
  }

  // Single-node cluster: already has majority, become leader immediately
  if (group_->vote_count_ >= majority) {
    BecomeLeaderLocked();
    return;
  }

  // Send request vote
  BroadcastRequestVoteLocked();

  // Reset election timer
  ResetElectionTimerLocked();
}

void RaftNode::RaftNodeImpl::BecomeLeaderLocked() {
  RaftNodeRole old_role = group_->role_;

  group_->role_ = RaftNodeRole::LEADER;
  group_->leader_id_ = group_->server_id_;
  group_->leader_addr_ = group_->config_.listen_addr;

  // Initialize leader state
  auto [last_index, _] = group_->log_.GetLastLogInfo();
  group_->next_index_.clear();
  group_->match_index_.clear();
  group_->retry_state_.clear();     // Reset retry state for new leadership
  group_->inflight_.clear();        // Clear pipeline state for new leadership
  group_->snapshot_sends_.clear();  // Clear snapshot send state for new leadership

  // Clear client sessions - new leader doesn't have old session state
  // Clients will retry with their next command, which will be treated as new
  size_t cleared_sessions = group_->client_sessions_.size();
  group_->client_sessions_.clear();

  // CheckQuorum: initialize quorum acks with grace period so that the
  // leader does not step down before peers have a chance to ack.
  auto now = std::chrono::steady_clock::now();
  group_->quorum_acks_.clear();
  group_->last_contact_time_.clear();
  for (const auto& [peer_id, addr] : group_->peer_map_) {
    (void)addr;
    group_->next_index_[peer_id] = last_index + 1;
    group_->match_index_[peer_id] = 0;
    SetPeerReplicationLagMetricLocked(peer_id);
    group_->quorum_acks_[peer_id] = now;
    group_->last_contact_time_[peer_id] = now;
  }

  // Stop election timer
  CancelElectionTimerLocked();

  // Start heartbeat timer
  {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    StartHeartbeatTimerLocked();
  }

  // Start auto-snapshot check timer
  {
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    StartSnapshotCheckTimerLocked();
  }

  // Publish events
  if (old_role != group_->role_) {
    NodeRoleChangedEvent role_event;
    role_event.node_id = group_->server_id_;
    role_event.old_role = old_role;
    role_event.new_role = group_->role_;
    role_event.term = group_->current_term_;
    role_event.timestamp = std::chrono::steady_clock::now();
    event_bus_.Publish(role_event);
  }

  LeaderChangedEvent leader_event;
  leader_event.node_id = group_->server_id_;
  leader_event.new_leader_id = group_->server_id_;
  leader_event.new_leader_addr = group_->config_.listen_addr;
  leader_event.term = group_->current_term_;
  leader_event.timestamp = std::chrono::steady_clock::now();
  event_bus_.Publish(leader_event);

  // Invoke callback
  if (old_role != group_->role_ && group_->role_change_callback_) {
    group_->role_change_callback_(group_->role_, group_->current_term_);
  }
  if (group_->leader_change_callback_) {
    group_->leader_change_callback_(group_->server_id_, group_->config_.listen_addr);
  }

  if (metrics_) {
    infra_->metrics_
        ->GetCounter("raft_leader_elected_total", {{"node_id", std::to_string(group_->server_id_)}})
        .Increment();
    infra_->metrics_->GetGauge("raft_role", {{"node_id", std::to_string(group_->server_id_)}})
        .Set(static_cast<double>(RaftNodeRole::LEADER));
  }
  LOG_INFO("Node {} became Leader at term {} (cleared {} client sessions)", group_->server_id_,
           group_->current_term_, cleared_sessions);

  // Send heartbeat immediately (establish authority)
  BroadcastAppendEntriesLocked();
}

void RaftNode::RaftNodeImpl::ResetElectionTimerLocked() {
  CancelElectionTimerLocked();

  // Read dynamic config snapshot (thread-safe via RuntimeConfig)
  auto cfg = infra_->runtime_config_->Get();

  uint32_t base_timeout = cfg.election_timeout_ms;
  // Quiesced mode: extend election timeout to reduce false elections
  // when the leader has paused heartbeats due to idleness.
  if (group_->quiesced_.load(std::memory_order_acquire)) {
    base_timeout = group_->config_.quiesced_election_timeout_ms;
  }

  // Random timeout [base_timeout, 2 * base_timeout)
  static thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dis(base_timeout, 2 * base_timeout);

  uint32_t timeout = dis(gen);

  group_->election_timer_ = infra_->timer_->SetTimeout(std::chrono::milliseconds(timeout),
                                                       [this]() { OnElectionTimeout(); });

  LOG_DEBUG("Node {} election timer reset to {}ms", group_->server_id_, timeout);
}

void RaftNode::RaftNodeImpl::CancelElectionTimerLocked() {
  if (group_->election_timer_ != 0) {
    infra_->timer_->CancelTimer(group_->election_timer_);
    group_->election_timer_ = 0;
  }
}

void RaftNode::RaftNodeImpl::OnStoreTick() {
  // Currently a no-op placeholder.  Future PRs can migrate group-local
  // timeouts (election, heartbeat, snapshot check) from one timer per group
  // to this single shared tick, reducing resource usage for very large
  // numbers of groups.  The tick infrastructure is in place in RaftStore.
}

void RaftNode::RaftNodeImpl::OnElectionTimeout() {
  std::lock_guard<std::mutex> lock(group_->election_mtx_);

  if (!IsRunning()) return;
  if (group_->role_ == RaftNodeRole::LEADER) return;

  if (metrics_) {
    infra_->metrics_
        ->GetCounter("raft_election_timeouts_total",
                     {{"node_id", std::to_string(group_->server_id_)}})
        .Increment();
  }

  // Quiesced mode: if we have been quiesced for multiple consecutive timeouts,
  // the leader may actually be dead. Exit quiesced and start election.
  // Otherwise, just reset the timer with extended timeout.
  if (group_->quiesced_.load(std::memory_order_acquire)) {
    ++group_->consecutive_quiesced_timeouts_;
    if (group_->consecutive_quiesced_timeouts_ >=
        group_->config_.quiesced_max_consecutive_timeouts) {
      LOG_INFO(
          "Node {} exiting quiesced mode after {} consecutive timeouts, "
          "starting election",
          group_->server_id_, group_->consecutive_quiesced_timeouts_);
      ExitQuiescedLocked();
      // Fall through to normal election handling
    } else {
      LOG_DEBUG("Node {} quiesced election timeout {}/{}, resetting timer", group_->server_id_,
                group_->consecutive_quiesced_timeouts_,
                group_->config_.quiesced_max_consecutive_timeouts);
      ResetElectionTimerLocked();
      return;
    }
  }

  if (!group_->pre_vote_enabled_) {
    // Pre-vote disabled: fall back to classic election
    LOG_INFO("Node {} election timeout at term {}, becoming Candidate", group_->server_id_,
             group_->current_term_);
    BecomeCandidateLocked();
    return;
  }

  LOG_INFO("Node {} election timeout at term {}, starting PreVote", group_->server_id_,
           group_->current_term_);

  // Pre-vote extension: before becoming candidate, ask peers if an
  // election would succeed. This prevents term inflation when a
  // partitioned node rejoins.
  group_->pre_vote_running_ = true;
  group_->pre_vote_count_ = 1;  // Vote for self
  group_->pre_vote_term_ = group_->current_term_ + 1;

  uint32_t majority;
  {
    std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
    majority = group_->cluster_config_.GetMajority();
  }

  // Single-node cluster: already has majority, skip pre-vote
  if (group_->pre_vote_count_ >= majority) {
    group_->pre_vote_running_ = false;
    BecomeCandidateLocked();
    return;
  }

  BroadcastPreVoteLocked();

  // Schedule a pre-vote timeout. If we don't get majority by then,
  // reset and wait for the next election timeout.
  auto cfg = infra_->runtime_config_->Get();
  uint32_t pre_vote_timeout = cfg.election_timeout_ms / 2;
  if (pre_vote_timeout < 10) pre_vote_timeout = 10;
  infra_->timer_->SetTimeout(std::chrono::milliseconds(pre_vote_timeout), [this]() {
    std::lock_guard<std::mutex> lock(group_->election_mtx_);
    if (group_->pre_vote_running_) {
      LOG_INFO("Node {} PreVote timed out, resetting", group_->server_id_);
      group_->pre_vote_running_ = false;
      group_->pre_vote_count_ = 0;
      // Reset election timer to wait for next timeout
      ResetElectionTimerLocked();
    }
  });
}

void RaftNode::RaftNodeImpl::BroadcastRequestVoteLocked() {
  LOG_INFO("Node {} broadcasting RequestVote at term {} to {} peers", group_->server_id_,
           group_->current_term_, group_->peer_addrs_.size());

  for (const auto& [peer_id, addr] : group_->peer_map_) {
    (void)peer_id;
    SendRequestVoteToPeerLocked(peer_id, addr);
  }
}

void RaftNode::RaftNodeImpl::SendRequestVoteToPeerLocked(NodeId peer_id, const NodeAddr& addr) {
  auto [last_index, last_term] = group_->log_.GetLastLogInfo();

  RequestVoteRequest req;
  req.group_id = group_->group_id_;
  req.term_ = group_->current_term_;
  req.candidate_id_ = group_->server_id_;
  req.last_log_index_ = last_index;
  req.last_log_term_ = last_term;
  req.correlation_id_ = next_correlation_id_.fetch_add(1, std::memory_order_relaxed);

  // Serialize request
  std::string data;
  auto status = infra_->protocol_->SerializeRequest(req, data);
  if (!status.ok()) {
    LOG_ERROR("Failed to serialize RequestVoteRequest: {}", status.ToString());
    return;
  }

  if (metrics_) {
    infra_->metrics_
        ->GetCounter("raft_requestvote_sent_total",
                     {{"node_id", std::to_string(group_->server_id_)}})
        .Increment();
  }

  Term original_term = group_->current_term_;  // Save current term for comparison

  auto cfg = infra_->runtime_config_->Get();

  infra_->network_->SendRpc(
      peer_id, addr, data, req.correlation_id_, std::chrono::milliseconds(cfg.rpc_timeout_ms),
      [this, peer_id, original_term](const std::string& resp, bool success,
                                     const std::string& error) {
        if (!success) {
          LOG_WARN("RequestVote to {} failed: {}", peer_id, error);
          return;
        }

        RequestVoteResponse response;
        auto status = infra_->protocol_->DeserializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize RequestVoteResponse: {}", status.ToString());
          return;
        }
        HandleRequestVoteResponse(peer_id, response, original_term);
      });
}

void RaftNode::RaftNodeImpl::HandleRequestVoteResponse(NodeId from, const RequestVoteResponse& resp,
                                                       Term original_term) {
  LOG_INFO("Node {} received RequestVoteResponse from {}: granted={}, term={}", group_->server_id_,
           from, resp.vote_granted_, resp.term_);

  std::lock_guard<std::mutex> lock(group_->election_mtx_);

  if (!IsRunning()) return;
  if (group_->role_ != RaftNodeRole::CANDIDATE) {
    LOG_INFO("Node {} ignoring vote response, not a candidate (role={})", group_->server_id_,
             static_cast<int>(group_->role_));
    return;
  }

  // If response term is higher, revert to Follower
  if (resp.term_ > group_->current_term_) {
    LOG_INFO("Node {} term {} < {}, reverting to Follower", group_->server_id_,
             group_->current_term_, resp.term_);
    BecomeFollowerLocked(resp.term_);
    return;
  }

  // If term has changed, ignore this response
  if (original_term != group_->current_term_) {
    return;
  }

  // Ignore stale term responses
  if (resp.term_ < group_->current_term_) {
    return;
  }

  if (resp.vote_granted_) {
    if (metrics_) {
      metrics_
          ->GetCounter("raft_votes_received_total",
                       {{"node_id", std::to_string(group_->server_id_)}, {"granted", "true"}})
          .Increment();
    }
    ++group_->vote_count_;
    uint32_t majority;
    {
      std::shared_lock<std::shared_mutex> lock(group_->membership_mtx_);
      majority = group_->cluster_config_.GetMajority();
    }
    LOG_INFO("Node {} got vote from {}, total: {}/{}", group_->server_id_, from,
             group_->vote_count_, majority);

    // Got majority votes, become Leader
    if (group_->vote_count_ >= majority) {
      BecomeLeaderLocked();
    }
  }
}

void RaftNode::RaftNodeImpl::BroadcastPreVoteLocked() {
  LOG_INFO("Node {} broadcasting PreVote at term {} to {} peers", group_->server_id_,
           group_->pre_vote_term_, group_->peer_addrs_.size());

  for (const auto& [peer_id, addr] : group_->peer_map_) {
    (void)peer_id;
    SendPreVoteToPeerLocked(peer_id, addr);
  }
}

void RaftNode::RaftNodeImpl::SendPreVoteToPeerLocked(NodeId peer_id, const NodeAddr& addr) {
  auto [last_index, last_term] = group_->log_.GetLastLogInfo();

  PreVoteRequest req;
  req.group_id = group_->group_id_;
  req.term_ = group_->pre_vote_term_;
  req.candidate_id_ = group_->server_id_;
  req.last_log_index_ = last_index;
  req.last_log_term_ = last_term;
  req.correlation_id_ = next_correlation_id_.fetch_add(1, std::memory_order_relaxed);

  std::string data;
  auto status = infra_->protocol_->SerializeRequest(req, data);
  if (!status.ok()) {
    LOG_ERROR("Failed to serialize PreVoteRequest: {}", status.ToString());
    return;
  }

  if (metrics_) {
    infra_->metrics_
        ->GetCounter("raft_prevote_sent_total", {{"node_id", std::to_string(group_->server_id_)}})
        .Increment();
  }

  Term original_pre_vote_term = group_->pre_vote_term_;
  auto cfg = infra_->runtime_config_->Get();

  infra_->network_->SendRpc(
      peer_id, addr, data, req.correlation_id_, std::chrono::milliseconds(cfg.rpc_timeout_ms),
      [this, peer_id, original_pre_vote_term](const std::string& resp, bool success,
                                              const std::string& error) {
        if (!success) {
          LOG_WARN("PreVote to {} failed: {}", peer_id, error);
          return;
        }

        PreVoteResponse response;
        auto status = infra_->protocol_->DeserializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize PreVoteResponse: {}", status.ToString());
          return;
        }
        HandlePreVoteResponse(peer_id, response, original_pre_vote_term);
      });
}

void RaftNode::RaftNodeImpl::HandlePreVoteResponse(NodeId from, const PreVoteResponse& resp,
                                                   Term original_pre_vote_term) {
  LOG_INFO("Node {} received PreVoteResponse from {}: granted={}, term={}", group_->server_id_,
           from, resp.vote_granted_, resp.term_);

  std::lock_guard<std::mutex> lock(group_->election_mtx_);

  if (!IsRunning()) return;
  if (!group_->pre_vote_running_) return;

  // If we are already a candidate or leader, pre-vote is done
  if (group_->role_ == RaftNodeRole::CANDIDATE || group_->role_ == RaftNodeRole::LEADER) {
    return;
  }

  // If our term has changed since pre-vote started, discard
  if (original_pre_vote_term != group_->pre_vote_term_) {
    return;
  }

  // If response term is higher, update term and revert to follower
  if (resp.term_ > group_->current_term_) {
    group_->pre_vote_running_ = false;
    group_->pre_vote_count_ = 0;
    BecomeFollowerLocked(resp.term_);
    return;
  }

  if (resp.vote_granted_) {
    if (metrics_) {
      metrics_
          ->GetCounter("raft_prevote_received_total",
                       {{"node_id", std::to_string(group_->server_id_)}, {"granted", "true"}})
          .Increment();
    }
    ++group_->pre_vote_count_;
    uint32_t majority;
    {
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      majority = group_->cluster_config_.GetMajority();
    }
    LOG_INFO("Node {} got PreVote from {}, total: {}/{}", group_->server_id_, from,
             group_->pre_vote_count_, majority);

    // Got majority pre-votes, become candidate
    if (group_->pre_vote_count_ >= majority) {
      group_->pre_vote_running_ = false;
      group_->pre_vote_count_ = 0;
      BecomeCandidateLocked();
    }
  }
}
