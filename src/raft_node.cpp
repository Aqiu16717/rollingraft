#include "rollingraft/raft_node.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <random>

#include "rollingraft/logger.h"
#include "rollingraft/network_transport.h"
#include "rollingraft/persister.h"
#include "rollingraft/protocol.h"
#include "rollingraft/raft_log.h"
#include "rollingraft/rpc.h"
#include "rollingraft/timer_service.h"
#include "rollingraft/types.h"

// Default component implementations
#include "json_protocol.h"
#include "asio_timer_service.h"

// Forward declaration for default network transport
namespace rollingraft {
std::unique_ptr<NetworkTransport> CreateDefaultNetworkTransport();
}  // namespace rollingraft

using namespace rollingraft;

// ========== Pending Proposals ==========
struct PendingProposal {
  Index index;                                         // Log index
  std::function<void(const ApplyResult&)> callback;    // Completion callback
  std::chrono::steady_clock::time_point propose_time;  // Proposal timestamp
};

// ========== RaftNode Implementation ==========
class RaftNode::RaftNodeImpl {
 public:
  RaftNodeImpl(const RaftNodeConfig& config,
               std::shared_ptr<StateMachine> state_machine,
               std::unique_ptr<NetworkTransport> network,
               std::unique_ptr<TimerService> timer,
               std::unique_ptr<Persister> persister,
               std::unique_ptr<Protocol> protocol);
  ~RaftNodeImpl();

  Status Start();
  Status Stop();

  bool IsLeader() const;
  RaftNodeRole GetRole() const;
  Term CurrentTerm() const;
  std::string GetLeaderAddr() const;

  void SetRoleChangeCallback(std::function<void(RaftNodeRole, uint64_t)> cb);
  void SetLeaderChangeCallback(std::function<void(NodeId, std::string)> cb);

  Status Propose(const std::string& command,
                 std::function<void(const ApplyResult&)> callback);
  Status ReadIndex(std::function<void()> callback);

  // RPC handlers (called by NetworkTransport)
  void HandleRequestVote(const RequestVoteRequest&, RequestVoteResponse&);
  void HandleAppendEntries(const AppendEntriesRequest&, AppendEntriesResponse&);
  void HandleInstallSnapshot(const InstallSnapshotRequest&,
                             InstallSnapshotResponse&);

 private:
  // State transitions (must hold mtx_ when calling)
  void BecomeFollowerLocked(Term term);
  void BecomeCandidateLocked();
  void BecomeLeaderLocked();

  // Timer management (must hold mtx_ when calling)
  void ResetElectionTimerLocked();
  void CancelElectionTimerLocked();
  void StartHeartbeatTimerLocked();
  void StopHeartbeatTimerLocked();

  // Election related
  void BroadcastRequestVoteLocked();
  void SendRequestVoteToPeerLocked(NodeId peer_id, const NodeAddr& addr);
  void HandleRequestVoteResponse(NodeId from, const RequestVoteResponse& resp,
                                 Term original_term);

  // Log replication related
  void BroadcastAppendEntriesLocked();
  void SendAppendEntriesToPeerLocked(NodeId peer_id);
  void HandleAppendEntriesResponse(NodeId from,
                                   const AppendEntriesResponse& resp);

  // Commit and apply
  void TryCommitLocked();
  void ApplyCommittedLocked();

  // Utility methods
  uint64_t GetLogTermLocked(uint64_t index);
  NodeId ParseNodeId(const NodeAddr& addr);

  // Timeout handlers
  void OnElectionTimeout();
  void OnHeartbeatTimeout();

  // RPC entry point
  void HandleIncomingRpc(NodeId from, const std::string& data,
                         std::string& response);

  // State check
  bool IsRunning() const { return state_ == NodeState::kRunning; }

 private:
  // ========== Node Identity ==========
  NodeId server_id_;
  std::vector<NodeAddr> peer_addrs_;
  std::unordered_map<NodeId, NodeAddr> peer_map_;

  // ========== Raft Persistent State ==========
  Term current_term_ = 0;
  NodeId voted_for_ = -1;
  RaftLog log_;

  // ========== Raft Volatile State ==========
  Index commit_index_ = 0;
  Index last_applied_ = 0;
  NodeId leader_id_ = -1;
  NodeAddr leader_addr_;
  RaftNodeRole role_ = RaftNodeRole::FOLLOWER;
  uint32_t vote_count_ = 0;

  // ========== Leader State ==========
  std::unordered_map<NodeId, Index> next_index_;
  std::unordered_map<NodeId, Index> match_index_;

  // ========== Timer State ==========
  int election_timeout_ = 0;
  TimerId election_timer_ = 0;
  TimerId heartbeat_timer_ = 0;

  // ========== Dependencies ==========
  RaftNodeConfig config_;
  std::shared_ptr<StateMachine> state_machine_;
  std::unique_ptr<NetworkTransport> network_;
  std::unique_ptr<TimerService> timer_;
  std::unique_ptr<Persister> persister_;
  std::unique_ptr<Protocol> protocol_;

  // ========== Runtime State ==========
  enum class NodeState {
    kInitialized = 0,
    kRunning = 1,
    kStopping = 2,
    kStopped = 3
  };
  std::atomic<NodeState> state_{NodeState::kInitialized};

  // ========== Thread Synchronization ==========
  mutable std::mutex mtx_;

  // ========== Pending Proposals ==========
  std::unordered_map<uint64_t, PendingProposal> pending_proposals_;

  // ========== Callbacks ==========
  std::function<void(RaftNodeRole, uint64_t)> role_change_callback_;
  std::function<void(NodeId, std::string)> leader_change_callback_;
};

// ========== Constructor/Destructor ==========

RaftNode::RaftNodeImpl::RaftNodeImpl(
    const RaftNodeConfig& config, std::shared_ptr<StateMachine> state_machine,
    std::unique_ptr<NetworkTransport> network,
    std::unique_ptr<TimerService> timer, std::unique_ptr<Persister> persister,
    std::unique_ptr<Protocol> protocol)
    : config_(config),
      state_machine_(std::move(state_machine)),
      network_(std::move(network)),
      timer_(std::move(timer)),
      persister_(std::move(persister)),
      protocol_(std::move(protocol)) {
  server_id_ = config.node_id;
  peer_addrs_ = config.peers;

  // Build peer map
  for (const auto& addr : peer_addrs_) {
    NodeId peer_id = ParseNodeId(addr);
    peer_map_[peer_id] = addr;
  }

  if (!state_machine_) {
    throw std::invalid_argument("StateMachine cannot be null");
  }
  if (!network_) {
    throw std::invalid_argument("NetworkTransport cannot be null");
  }
  if (!timer_) {
    throw std::invalid_argument("TimerService cannot be null");
  }

  LOG_INFO("RaftNodeImpl created for node {}", server_id_);
}

RaftNode::RaftNodeImpl::~RaftNodeImpl() {
  if (state_ == NodeState::kRunning) {
    Stop();
  }
}

// ========== Public Interface Implementation ==========

Status RaftNode::RaftNodeImpl::Start() {
  NodeState expected = NodeState::kInitialized;
  if (!state_.compare_exchange_strong(expected, NodeState::kRunning)) {
    return Status::Error("Already started or stopped");
  }

  LOG_INFO("Starting RaftNode {} on {}...", config_.node_id,
           config_.listen_addr);

  // 1. Initialize persistence
  if (persister_) {
    auto status = persister_->Open(config_.data_dir);
    if (!status.ok()) {
      state_ = NodeState::kInitialized;
      return status;
    }

    // Restore persistent state
    PersistentState state;
    if (persister_->LoadState(state).ok()) {
      current_term_ = state.current_term;
      voted_for_ = state.voted_for;
      LOG_INFO("Restored state: term={}, voted_for={}", current_term_,
               voted_for_);
    }
  }

  // 2. Initialize network layer
  auto handler = [this](NodeId from, const std::string& req,
                        std::string& resp) {
    HandleIncomingRpc(from, req, resp);
  };

  auto status = network_->Initialize(config_.listen_addr, handler);
  if (!status.ok()) {
    if (persister_) persister_->Close();
    state_ = NodeState::kInitialized;
    return status;
  }

  status = network_->Start();
  if (!status.ok()) {
    if (persister_) persister_->Close();
    state_ = NodeState::kInitialized;
    return status;
  }

  // 3. Start timer service
  timer_->Start();

  // 4. Enter Follower state
  {
    std::lock_guard<std::mutex> lock(mtx_);
    BecomeFollowerLocked(current_term_);
  }

  LOG_INFO("RaftNode {} started successfully", config_.node_id);
  return Status::OK();
}

Status RaftNode::RaftNodeImpl::Stop() {
  NodeState expected = NodeState::kRunning;
  if (!state_.compare_exchange_strong(expected, NodeState::kStopping)) {
    if (state_ == NodeState::kStopped) {
      return Status::OK();  // Already stopped
    }
    return Status::Error("Node not running");
  }

  LOG_INFO("Stopping RaftNode {}...", config_.node_id);

  // 1. Stop timers (with lock)
  {
    std::lock_guard<std::mutex> lock(mtx_);
    CancelElectionTimerLocked();
    StopHeartbeatTimerLocked();
  }

  // 2. Stop TimerService
  if (timer_) {
    timer_->Stop();
  }

  // 3. Stop NetworkTransport
  if (network_) {
    network_->Stop();
  }

  // 4. Close persistence
  if (persister_) {
    persister_->Close();
  }

  // 5. Clean up pending proposals
  {
    std::lock_guard<std::mutex> lock(mtx_);
    for (auto& [id, proposal] : pending_proposals_) {
      ApplyResult result;
      result.success = false;
      result.error_message = "Node stopped";
      proposal.callback(result);
    }
    pending_proposals_.clear();
  }

  state_ = NodeState::kStopped;
  LOG_INFO("RaftNode {} stopped", config_.node_id);
  return Status::OK();
}

bool RaftNode::RaftNodeImpl::IsLeader() const {
  std::lock_guard<std::mutex> lock(mtx_);
  return role_ == RaftNodeRole::LEADER;
}

RaftNodeRole RaftNode::RaftNodeImpl::GetRole() const {
  std::lock_guard<std::mutex> lock(mtx_);
  return role_;
}

Term RaftNode::RaftNodeImpl::CurrentTerm() const {
  std::lock_guard<std::mutex> lock(mtx_);
  return current_term_;
}

std::string RaftNode::RaftNodeImpl::GetLeaderAddr() const {
  std::lock_guard<std::mutex> lock(mtx_);
  return leader_addr_;
}

void RaftNode::RaftNodeImpl::SetRoleChangeCallback(
    std::function<void(RaftNodeRole, uint64_t)> cb) {
  role_change_callback_ = std::move(cb);
}

void RaftNode::RaftNodeImpl::SetLeaderChangeCallback(
    std::function<void(NodeId, std::string)> cb) {
  leader_change_callback_ = std::move(cb);
}

Status RaftNode::RaftNodeImpl::Propose(
    const std::string& command,
    std::function<void(const ApplyResult&)> callback) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  // Append to local log
  auto [index, status] = log_.Append(current_term_, command);
  if (!status.ok()) {
    return status;
  }

  // Record pending proposal
  PendingProposal proposal;
  proposal.index = index;
  proposal.callback = std::move(callback);
  proposal.propose_time = std::chrono::steady_clock::now();
  pending_proposals_[index] = std::move(proposal);

  // Trigger log replication
  BroadcastAppendEntriesLocked();

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::ReadIndex(std::function<void()> callback) {
  // TODO: implement linearizable read
  (void)callback;
  return Status::Error("Not implemented");
}

// ========== State Transitions ==========

void RaftNode::RaftNodeImpl::BecomeFollowerLocked(Term term) {
  RaftNodeRole old_role = role_;

  role_ = RaftNodeRole::FOLLOWER;
  current_term_ = term;
  voted_for_ = -1;
  vote_count_ = 0;
  leader_id_ = -1;
  leader_addr_.clear();

  // Stop leader timer
  StopHeartbeatTimerLocked();

  // Reset and start election timer
  ResetElectionTimerLocked();

  // Persist state
  if (persister_) {
    persister_->SaveState({current_term_, voted_for_});
  }

  // Invoke callback
  if (old_role != role_ && role_change_callback_) {
    role_change_callback_(role_, current_term_);
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
    persister_->SaveState({current_term_, voted_for_});
  }

  // Invoke callback
  if (old_role != role_ && role_change_callback_) {
    role_change_callback_(role_, current_term_);
  }

  LOG_INFO("Node {} became Candidate at term {}", server_id_, current_term_);

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

  for (const auto& [peer_id, addr] : peer_map_) {
    (void)addr;
    next_index_[peer_id] = last_index + 1;
    match_index_[peer_id] = 0;
  }

  // Stop election timer
  CancelElectionTimerLocked();

  // Start heartbeat timer
  StartHeartbeatTimerLocked();

  // Invoke callback
  if (old_role != role_ && role_change_callback_) {
    role_change_callback_(role_, current_term_);
  }
  if (leader_change_callback_) {
    leader_change_callback_(server_id_, config_.listen_addr);
  }

  LOG_INFO("Node {} became Leader at term {}", server_id_, current_term_);

  // Send heartbeat immediately (establish authority)
  BroadcastAppendEntriesLocked();
}

// ========== Timer Management ==========

void RaftNode::RaftNodeImpl::ResetElectionTimerLocked() {
  CancelElectionTimerLocked();

  // Random timeout [election_timeout, 2 * election_timeout)
  static thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dis(config_.election_timeout_ms,
                                      2 * config_.election_timeout_ms);

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

// ========== Election Handling ==========

void RaftNode::RaftNodeImpl::OnElectionTimeout() {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) return;
  if (role_ == RaftNodeRole::LEADER) return;

  LOG_INFO("Node {} election timeout at term {}, becoming Candidate",
           server_id_, current_term_);

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

  // Serialize request
  std::string data;
  // protocol_->SerializeRequest(req, data);  // TODO: implement serialization

  Term original_term = current_term_;  // Save current term for comparison

  network_->SendRpc(
      peer_id, addr, data, std::chrono::milliseconds(config_.rpc_timeout_ms),
      [this, peer_id, original_term](const std::string& resp, bool success,
                                     const std::string& error) {
        if (!success) {
          LOG_WARN("RequestVote to {} failed: {}", peer_id, error);
          return;
        }

        RequestVoteResponse response;
        // protocol_->DeserializeResponse(resp, response); // TODO: implement deserialization: implement deserialization
        HandleRequestVoteResponse(peer_id, response, original_term);
      });
}

void RaftNode::RaftNodeImpl::HandleRequestVoteResponse(
    NodeId from, const RequestVoteResponse& resp, Term original_term) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) return;
  if (role_ != RaftNodeRole::CANDIDATE) return;

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
    ++vote_count_;
    LOG_INFO("Node {} got vote from {}, total: {}/{}", server_id_, from,
             vote_count_, peer_addrs_.size() + 1);

    // Got majority votes, become Leader
    if (vote_count_ > (peer_addrs_.size() + 1) / 2) {
      BecomeLeaderLocked();
    }
  }
}

// ========== Log Replication ==========

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

  AppendEntriesRequest req;
  req.term_ = current_term_;
  req.leader_id_ = server_id_;
  req.prev_log_index_ = next_idx - 1;
  req.prev_log_term_ = GetLogTermLocked(req.prev_log_index_);
  req.leader_commit_ = commit_index_;

  // Get log entries
  auto [last_index, _] = log_.GetLastLogInfo();
  if (next_idx <= last_index) {
    Index end =
        std::min(next_idx + config_.max_entries_per_append, last_index + 1);
    req.entries_ = log_.GetEntries(next_idx, end);
  }

  // Serialize and send
  std::string data;
  // protocol_->SerializeRequest(req, data); // TODO: implement serialization

  auto it_addr = peer_map_.find(peer_id);
  if (it_addr == peer_map_.end()) return;

  network_->SendRpc(peer_id, it_addr->second, data,
                    std::chrono::milliseconds(config_.rpc_timeout_ms),
                    [this, peer_id](const std::string& resp, bool success,
                                    const std::string& error) {
                      if (!success) {
                        LOG_WARN("AppendEntries to {} failed: {}", peer_id,
                                 error);
                        return;
                      }

                      AppendEntriesResponse response;
                      // protocol_->DeserializeResponse(resp, response); // TODO
                      HandleAppendEntriesResponse(peer_id, response);
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
    // Update progress
    Index new_match = next_index_[from] - 1 + resp.entries_count_;
    match_index_[from] = std::max(match_index_[from], new_match);
    next_index_[from] = match_index_[from] + 1;

    // Try to commit
    TryCommitLocked();
  } else {
    // Log mismatch, back off
    if (resp.conflict_index_ > 0) {
      next_index_[from] = resp.conflict_index_;
    } else {
      next_index_[from] = std::max<Index>(1, next_index_[from] - 1);
    }

    // Retry with delay
    timer_->SetTimeout(std::chrono::milliseconds(10), [this, from]() {
      std::lock_guard<std::mutex> lock(mtx_);
      if (role_ == RaftNodeRole::LEADER) {
        SendAppendEntriesToPeerLocked(from);
      }
    });
  }
}

// ========== Commit and Apply ==========

void RaftNode::RaftNodeImpl::TryCommitLocked() {
  auto [last_index, _] = log_.GetLastLogInfo();

  for (Index index = last_index; index > commit_index_; --index) {
    // Only commit entries from current term
    if (GetLogTermLocked(index) != current_term_) {
      break;
    }

    // Count logs replicated to majority
    int count = 1;  // Self
    for (const auto& [peer_id, match] : match_index_) {
      (void)peer_id;
      if (match >= index) ++count;
    }

    if (count > (peer_addrs_.size() + 1) / 2) {
      commit_index_ = index;
      LOG_INFO("Node {} commit index advanced to {}", server_id_,
               commit_index_);
      ApplyCommittedLocked();
      break;
    }
  }
}

void RaftNode::RaftNodeImpl::ApplyCommittedLocked() {
  while (last_applied_ < commit_index_) {
    ++last_applied_;

    auto entry_opt = log_.GetEntry(last_applied_);
    if (!entry_opt) {
      LOG_ERROR("Node {} failed to get log entry {}", server_id_,
                last_applied_);
      continue;
    }

    const auto& entry = *entry_opt;

    // Apply to StateMachine
    auto result = state_machine_->Apply(
        std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(entry.data_.data()),
            entry.data_.size()),
        last_applied_);

    // Callback to waiting users
    auto it = pending_proposals_.find(last_applied_);
    if (it != pending_proposals_.end()) {
      it->second.callback(result);
      pending_proposals_.erase(it);
    }
  }
}

// ========== RPC Handling ==========

void RaftNode::RaftNodeImpl::HandleIncomingRpc(NodeId from,
                                               const std::string& data,
                                               std::string& response) {
  (void)from;
  (void)data;
  (void)response;
  // TODO: dispatch to specific handler based on message type
}

void RaftNode::RaftNodeImpl::HandleRequestVote(const RequestVoteRequest& req,
                                               RequestVoteResponse& resp) {
  std::lock_guard<std::mutex> lock(mtx_);

  resp.term_ = current_term_;
  resp.vote_granted_ = false;

  // If request term is higher, revert to Follower
  if (req.term_ > current_term_) {
    BecomeFollowerLocked(req.term_);
    resp.term_ = current_term_;
  }

  // Reject stale term requests
  if (req.term_ < current_term_) {
    LOG_DEBUG("Node {} reject vote: req.term {} < {}", server_id_, req.term_,
              current_term_);
    return;
  }

  // Check if log is at least as up-to-date
  auto [last_index, last_term] = log_.GetLastLogInfo();

  bool log_is_up_to_date =
      (req.last_log_term_ > last_term) ||
      (req.last_log_term_ == last_term && req.last_log_index_ >= last_index);

  if (!log_is_up_to_date) {
    LOG_DEBUG("Node {} reject vote: candidate log not up-to-date", server_id_);
    return;
  }

  // Check if already voted
  if (voted_for_ == -1 || voted_for_ == req.candidate_id_) {
    voted_for_ = req.candidate_id_;
    resp.vote_granted_ = true;

    // Reset election timer
    ResetElectionTimerLocked();

    // Persist state
    if (persister_) {
      persister_->SaveState({current_term_, voted_for_});
    }

    LOG_INFO("Node {} voted for {} at term {}", server_id_, req.candidate_id_,
             current_term_);
  }
}

void RaftNode::RaftNodeImpl::HandleAppendEntries(
    const AppendEntriesRequest& req, AppendEntriesResponse& resp) {
  std::lock_guard<std::mutex> lock(mtx_);

  resp.term_ = current_term_;
  resp.success_ = false;
  resp.conflict_index_ = 0;
  resp.entries_count_ = 0;

  // If leader term is higher, revert to Follower
  if (req.term_ > current_term_) {
    BecomeFollowerLocked(req.term_);
    resp.term_ = current_term_;
  }

  // Reject stale term leader
  if (req.term_ < current_term_) {
    LOG_DEBUG("Node {} reject AppendEntries: req.term {} < {}", server_id_,
              req.term_, current_term_);
    return;
  }

  // Update leader info
  leader_id_ = req.leader_id_;
  auto it = peer_map_.find(leader_id_);
  if (it != peer_map_.end()) {
    leader_addr_ = it->second;
  }

  // Reset election timer
  ResetElectionTimerLocked();

  // Check if prev_log matches
  if (req.prev_log_index_ > 0) {
    Term prev_term = GetLogTermLocked(req.prev_log_index_);
    if (prev_term != req.prev_log_term_) {
      LOG_DEBUG("Node {} log mismatch at index {}: local={}, remote={}",
                server_id_, req.prev_log_index_, prev_term, req.prev_log_term_);
      resp.conflict_index_ = req.prev_log_index_;
      return;
    }
  }

  // Append log entries
  if (!req.entries_.empty()) {
    // Check for conflicts and truncate
    for (const auto& entry : req.entries_) {
      Term existing_term = GetLogTermLocked(entry.index_);
      if (existing_term != 0 && existing_term != entry.term_) {
        LOG_INFO("Node {} truncating log from index {}", server_id_,
                 entry.index_);
        log_.TruncateSuffix(entry.index_);
        break;
      }
    }

    // Append new entries
    for (const auto& entry : req.entries_) {
      auto [idx, status] = log_.Append(entry.term_, entry.data_);
      (void)idx;
      if (!status.ok()) {
        LOG_ERROR("Node {} failed to append entry: {}", server_id_,
                  status.ToString());
        return;
      }
    }
    resp.entries_count_ = req.entries_.size();
    LOG_INFO("Node {} appended {} entries", server_id_, req.entries_.size());
  }

  // Update commit_index
  if (req.leader_commit_ > commit_index_) {
    auto [last_index, _] = log_.GetLastLogInfo();
    commit_index_ = std::min(req.leader_commit_, last_index);
    ApplyCommittedLocked();
  }

  resp.success_ = true;
}

void RaftNode::RaftNodeImpl::HandleInstallSnapshot(
    const InstallSnapshotRequest& req, InstallSnapshotResponse& resp) {
  (void)req;
  (void)resp;
  // TODO: implement snapshot handling
}

// ========== Utility Methods ==========

uint64_t RaftNode::RaftNodeImpl::GetLogTermLocked(uint64_t index) {
  if (index == 0) return 0;
  return log_.GetLogTerm(index);
}

NodeId RaftNode::RaftNodeImpl::ParseNodeId(const NodeAddr& addr) {
  // Simple parsing: extract port number as ID from address
  // In production, should use configured node_id mapping
  auto pos = addr.find(':');
  if (pos == std::string::npos) return -1;
  try {
    return static_cast<NodeId>(std::stoi(addr.substr(pos + 1)));
  } catch (...) {
    return -1;
  }
}

// ========== RaftNode Public Interface ==========

RaftNode::RaftNode(const RaftNodeConfig& config,
                   std::shared_ptr<StateMachine> sm)
    : raft_node_impl_(std::make_unique<RaftNodeImpl>(
          config, sm,
          config.network_factory ? config.network_factory()
                                 : CreateDefaultNetworkTransport(),
          config.timer_factory ? config.timer_factory()
                              : TimerService::CreateDefault(),
          config.persister_factory ? config.persister_factory() : nullptr,
          config.protocol_factory ? config.protocol_factory()
                                 : std::make_unique<JsonProtocol>())) {}

RaftNode::~RaftNode() = default;

Status RaftNode::Start() { return raft_node_impl_->Start(); }

Status RaftNode::Stop() { return raft_node_impl_->Stop(); }

bool RaftNode::IsLeader() const { return raft_node_impl_->IsLeader(); }

RaftNodeRole RaftNode::GetRole() const { return raft_node_impl_->GetRole(); }

Term RaftNode::CurrentTerm() const { return raft_node_impl_->CurrentTerm(); }

NodeAddr RaftNode::GetLeaderAddr() const {
  return raft_node_impl_->GetLeaderAddr();
}
