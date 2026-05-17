#include "raft_node_impl.h"

using namespace rollingraft;

void RaftNode::RaftNodeImpl::BecomeFollowerLocked(Term term) {
  RaftNodeRole old_role = role_;

  role_ = RaftNodeRole::FOLLOWER;
  current_term_ = term;
  voted_for_ = -1;
  vote_count_ = 0;
  leader_id_ = -1;
  leader_addr_.clear();

  // Stop leader timers
  {
    std::lock_guard<std::mutex> lock_r(replication_mtx_);
    StopHeartbeatTimerLocked();
  }
  {
    std::lock_guard<std::mutex> lock_s(snapshot_mtx_);
    StopSnapshotCheckTimerLocked();
  }

  // Reset and start election timer
  ResetElectionTimerLocked();

  // Persist state
  if (persister_) {
    auto persist_status = persister_->SaveState({current_term_, voted_for_});
    if (!persist_status.ok()) {
      LOG_ERROR("Node {} failed to persist state when becoming Follower: {}",
                server_id_, persist_status.GetMessage());
    }
  }

  // Invoke callback
  if (old_role != role_ && role_change_callback_) {
    role_change_callback_(role_, current_term_);
  }

  if (metrics_) {
    metrics_->GetGauge("raft_role", {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(RaftNodeRole::FOLLOWER));
    metrics_
        ->GetGauge("raft_current_term",
                   {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(current_term_));
  }
  LOG_INFO("Node {} became Follower at term {}", server_id_, current_term_);
}

void RaftNode::RaftNodeImpl::BecomeCandidateLocked() {
  RaftNodeRole old_role = role_;

  role_ = RaftNodeRole::CANDIDATE;
  ++current_term_;
  voted_for_ = server_id_;
  vote_count_ = 1;  // Vote for self

  // Persist state
  if (persister_) {
    auto persist_status = persister_->SaveState({current_term_, voted_for_});
    if (!persist_status.ok()) {
      LOG_ERROR("Node {} failed to persist state when becoming Candidate: {}",
                server_id_, persist_status.GetMessage());
    }
  }

  // Invoke callback
  if (old_role != role_ && role_change_callback_) {
    role_change_callback_(role_, current_term_);
  }

  if (metrics_) {
    metrics_
        ->GetCounter("raft_elections_total",
                     {{"node_id", std::to_string(server_id_)}})
        .Increment();
    metrics_->GetGauge("raft_role", {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(RaftNodeRole::CANDIDATE));
    metrics_
        ->GetGauge("raft_current_term",
                   {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(current_term_));
  }
  LOG_INFO("Node {} became Candidate at term {}", server_id_, current_term_);

  // Single-node cluster: already has majority, become leader immediately
  if (vote_count_ > (peer_addrs_.size() + 1) / 2) {
    BecomeLeaderLocked();
    return;
  }

  // Send request vote
  BroadcastRequestVoteLocked();

  // Reset election timer
  ResetElectionTimerLocked();
}

void RaftNode::RaftNodeImpl::BecomeLeaderLocked() {
  RaftNodeRole old_role = role_;

  role_ = RaftNodeRole::LEADER;
  leader_id_ = server_id_;
  leader_addr_ = config_.listen_addr;

  // Initialize leader state
  auto [last_index, _] = log_.GetLastLogInfo();
  next_index_.clear();
  match_index_.clear();
  retry_state_.clear();  // Reset retry state for new leadership

  // Clear client sessions - new leader doesn't have old session state
  // Clients will retry with their next command, which will be treated as new
  size_t cleared_sessions = client_sessions_.size();
  client_sessions_.clear();

  for (const auto& [peer_id, addr] : peer_map_) {
    (void)addr;
    next_index_[peer_id] = last_index + 1;
    match_index_[peer_id] = 0;
  }

  // Stop election timer
  CancelElectionTimerLocked();

  // Start heartbeat timer
  {
    std::lock_guard<std::mutex> lock_r(replication_mtx_);
    StartHeartbeatTimerLocked();
  }

  // Start auto-snapshot check timer
  {
    std::lock_guard<std::mutex> lock_s(snapshot_mtx_);
    StartSnapshotCheckTimerLocked();
  }

  // Invoke callback
  if (old_role != role_ && role_change_callback_) {
    role_change_callback_(role_, current_term_);
  }
  if (leader_change_callback_) {
    leader_change_callback_(server_id_, config_.listen_addr);
  }

  if (metrics_) {
    metrics_
        ->GetCounter("raft_leader_elected_total",
                     {{"node_id", std::to_string(server_id_)}})
        .Increment();
    metrics_->GetGauge("raft_role", {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(RaftNodeRole::LEADER));
  }
  LOG_INFO("Node {} became Leader at term {} (cleared {} client sessions)",
           server_id_, current_term_, cleared_sessions);

  // Send heartbeat immediately (establish authority)
  BroadcastAppendEntriesLocked();
}

void RaftNode::RaftNodeImpl::ResetElectionTimerLocked() {
  CancelElectionTimerLocked();

  // Read dynamic config snapshot (thread-safe via RuntimeConfig)
  auto cfg = runtime_config_->Get();

  // Random timeout [election_timeout, 2 * election_timeout)
  static thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dis(cfg.election_timeout_ms,
                                      2 * cfg.election_timeout_ms);

  uint32_t timeout = dis(gen);

  election_timer_ = timer_->SetTimeout(std::chrono::milliseconds(timeout),
                                       [this]() { OnElectionTimeout(); });

  LOG_DEBUG("Node {} election timer reset to {}ms", server_id_, timeout);
}

void RaftNode::RaftNodeImpl::CancelElectionTimerLocked() {
  if (election_timer_ != 0) {
    timer_->CancelTimer(election_timer_);
    election_timer_ = 0;
  }
}

void RaftNode::RaftNodeImpl::OnElectionTimeout() {
  std::lock_guard<std::mutex> lock(election_mtx_);

  if (!IsRunning()) return;
  if (role_ == RaftNodeRole::LEADER) return;

  LOG_INFO("Node {} election timeout at term {}, becoming Candidate",
           server_id_, current_term_);

  if (metrics_) {
    metrics_
        ->GetCounter("raft_election_timeouts_total",
                     {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }
  BecomeCandidateLocked();
}

void RaftNode::RaftNodeImpl::BroadcastRequestVoteLocked() {
  auto [last_index, last_term] = log_.GetLastLogInfo();

  RequestVoteRequest req;
  req.term_ = current_term_;
  req.candidate_id_ = server_id_;
  req.last_log_index_ = last_index;
  req.last_log_term_ = last_term;

  LOG_INFO("Node {} broadcasting RequestVote at term {} to {} peers",
           server_id_, current_term_, peer_addrs_.size());

  for (const auto& [peer_id, addr] : peer_map_) {
    (void)peer_id;
    SendRequestVoteToPeerLocked(peer_id, addr);
  }
}

void RaftNode::RaftNodeImpl::SendRequestVoteToPeerLocked(NodeId peer_id,
                                                         const NodeAddr& addr) {
  auto [last_index, last_term] = log_.GetLastLogInfo();

  RequestVoteRequest req;
  req.term_ = current_term_;
  req.candidate_id_ = server_id_;
  req.last_log_index_ = last_index;
  req.last_log_term_ = last_term;
  req.correlation_id_ =
      next_correlation_id_.fetch_add(1, std::memory_order_relaxed);

  // Serialize request
  std::string data;
  auto status = protocol_->SerializeRequest(req, data);
  if (!status.ok()) {
    LOG_ERROR("Failed to serialize RequestVoteRequest: {}", status.ToString());
    return;
  }

  if (metrics_) {
    metrics_
        ->GetCounter("raft_requestvote_sent_total",
                     {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }

  Term original_term = current_term_;  // Save current term for comparison

  auto cfg = runtime_config_->Get();

  network_->SendRpc(
      peer_id, addr, data, req.correlation_id_,
      std::chrono::milliseconds(cfg.rpc_timeout_ms),
      [this, peer_id, original_term](const std::string& resp, bool success,
                                     const std::string& error) {
        if (!success) {
          LOG_WARN("RequestVote to {} failed: {}", peer_id, error);
          return;
        }

        RequestVoteResponse response;
        auto status = protocol_->DeserializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize RequestVoteResponse: {}",
                    status.ToString());
          return;
        }
        HandleRequestVoteResponse(peer_id, response, original_term);
      });
}

void RaftNode::RaftNodeImpl::HandleRequestVoteResponse(
    NodeId from, const RequestVoteResponse& resp, Term original_term) {
  LOG_INFO("Node {} received RequestVoteResponse from {}: granted={}, term={}",
           server_id_, from, resp.vote_granted_, resp.term_);

  std::lock_guard<std::mutex> lock(election_mtx_);

  if (!IsRunning()) return;
  if (role_ != RaftNodeRole::CANDIDATE) {
    LOG_INFO("Node {} ignoring vote response, not a candidate (role={})",
             server_id_, static_cast<int>(role_));
    return;
  }

  // If response term is higher, revert to Follower
  if (resp.term_ > current_term_) {
    LOG_INFO("Node {} term {} < {}, reverting to Follower", server_id_,
             current_term_, resp.term_);
    BecomeFollowerLocked(resp.term_);
    return;
  }

  // If term has changed, ignore this response
  if (original_term != current_term_) {
    return;
  }

  // Ignore stale term responses
  if (resp.term_ < current_term_) {
    return;
  }

  if (resp.vote_granted_) {
    if (metrics_) {
      metrics_
          ->GetCounter(
              "raft_votes_received_total",
              {{"node_id", std::to_string(server_id_)}, {"granted", "true"}})
          .Increment();
    }
    ++vote_count_;
    LOG_INFO("Node {} got vote from {}, total: {}/{}", server_id_, from,
             vote_count_, peer_addrs_.size() + 1);

    // Got majority votes, become Leader
    if (vote_count_ > (peer_addrs_.size() + 1) / 2) {
      BecomeLeaderLocked();
    }
  }
}
