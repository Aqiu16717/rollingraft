#include "rollingraft/raft_node.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <random>
#include <set>

#include "rollingraft/log_persister.h"
#include "rollingraft/logger.h"
#include "rollingraft/network_transport.h"
#include "rollingraft/persister.h"
#include "rollingraft/protocol.h"
#include "rollingraft/raft_log.h"
#include "rollingraft/rpc.h"
#include "rollingraft/state_machine.h"
#include "rollingraft/timer_service.h"
#include "rollingraft/types.h"

#include "rollingraft/metrics.h"
#include "metrics_http_server.h"

// Default component implementations
#include "asio_timer_service.h"
#include "json_protocol.h"
#include "nlohmann/json.hpp"

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

// ========== Client Session (for idempotency) ==========
struct ClientSession {
  uint64_t last_seq;                                  // Last processed seq
  std::string last_response;                          // Cached response
  Index last_index;                                   // Log index
  Term last_term;                                     // Term when executed
  std::chrono::steady_clock::time_point last_active;  // For cleanup
};

// ========== Snapshot Transfer State (Leader side) ==========
struct SnapshotSendState {
  std::shared_ptr<Snapshot> snapshot;  // Snapshot handle
  uint64_t offset = 0;                 // Current offset
  Index last_included_index = 0;       // Snapshot metadata
  Term last_included_term = 0;         // Snapshot metadata
  bool in_progress = false;            // Transfer in progress
  size_t last_chunk_size = 0;          // For progress tracking
};

// ========== Pending ReadIndex Request ==========
struct PendingReadIndex {
  Index read_index;                // The commit index to wait for
  std::function<void()> callback;  // Completion callback
  std::chrono::steady_clock::time_point start_time;  // Request timestamp
  std::set<NodeId> acks;                             // Nodes that acknowledged
  bool heartbeats_sent = false;  // Whether heartbeats were sent
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
  ApplyResult ProposeAndWaitLocked(const std::string& command);
  Status ReadIndex(std::function<void()> callback);

  // Membership change (only for leader)
  Status AddNode(NodeId id, const NodeAddr& addr);
  Status RemoveNode(NodeId id);
  ClusterConfig GetConfig() const;

  // RPC handlers (called by NetworkTransport)
  void HandleRequestVote(const RequestVoteRequest&, RequestVoteResponse&);
  void HandleAppendEntries(const AppendEntriesRequest&, AppendEntriesResponse&);
  void HandleInstallSnapshot(const InstallSnapshotRequest&,
                             InstallSnapshotResponse&);
  void HandleClientRequest(const ClientRequest&, ClientResponse&);

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
  void StartSnapshotCheckTimerLocked();
  void StopSnapshotCheckTimerLocked();

  // Snapshot related
  void MaybeTriggerAutoSnapshotLocked();

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
  void ScheduleAppendEntriesRetry(NodeId peer_id);  // With exponential backoff

  // ReadIndex related
  void BroadcastReadIndexHeartbeatsLocked(uint64_t read_id);
  void HandleReadIndexAckLocked(NodeId from, uint64_t read_id);
  void ProcessPendingReadsLocked();

  // Snapshot related
  void SendInstallSnapshotToPeerLocked(NodeId peer_id);
  void SendNextSnapshotChunkLocked(NodeId peer_id);
  void HandleInstallSnapshotResponse(NodeId from,
                                     const InstallSnapshotResponse& resp,
                                     bool rpc_success);

  // Commit and apply
  void TryCommitLocked();
  void ApplyCommittedLocked();

  // Utility methods
  uint64_t GetLogTermLocked(uint64_t index);
  NodeId ParseNodeId(const NodeAddr& addr);

  // Membership change
  void ApplyConfigChangeLocked(const std::string& cmd);

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
  Index flushed_index_ = 0;  // Highest log index durably persisted
  NodeId leader_id_ = -1;
  NodeAddr leader_addr_;
  RaftNodeRole role_ = RaftNodeRole::FOLLOWER;
  uint32_t vote_count_ = 0;

  // ========== Leader State ==========
  std::unordered_map<NodeId, Index> next_index_;
  std::unordered_map<NodeId, Index> match_index_;
  std::unordered_map<uint64_t, ClientSession> client_sessions_;  // Idempotency

  // Retry tracking for AppendEntries
  struct RetryState {
    int attempts = 0;
    std::chrono::steady_clock::time_point last_retry;
  };
  std::unordered_map<NodeId, RetryState> retry_state_;

  // ========== Cluster Config ==========
  ClusterConfig cluster_config_;
  mutable std::mutex config_mutex_;  // Protects cluster_config_

  // ========== Timer State ==========
  TimerId election_timer_ = 0;
  TimerId heartbeat_timer_ = 0;
  TimerId snapshot_check_timer_ = 0;

  // ========== Snapshot State ==========
  Index last_snapshot_index_ = 0;  // For auto-snapshot trigger

  // ========== Dependencies ==========
  RaftNodeConfig config_;
  std::shared_ptr<StateMachine> state_machine_;
  std::unique_ptr<NetworkTransport> network_;
  std::unique_ptr<TimerService> timer_;
  std::unique_ptr<Persister> persister_;
  std::unique_ptr<LogPersister> log_persister_;
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

  // ========== Pending ReadIndex Requests ==========
  std::unordered_map<uint64_t, PendingReadIndex> pending_reads_;
  uint64_t next_read_id_ = 1;

  // ========== Snapshot Transfer State ==========
  std::unordered_map<NodeId, SnapshotSendState> snapshot_sends_;  // Leader side
  std::string snapshot_temp_data_;  // Follower side (chunk buffer)

  // ========== Metrics ==========
  std::unique_ptr<MetricsRegistry> metrics_;
  std::unique_ptr<MetricsHttpServer> metrics_server_;

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

  // Initialize metrics if enabled
  if (config.metrics_enabled) {
    metrics_ = std::make_unique<MetricsRegistry>();
  }

  // Initialize cluster config from peers
  cluster_config_.nodes.push_back(server_id_);
  for (const auto& addr : peer_addrs_) {
    NodeId peer_id = ParseNodeId(addr);
    if (peer_id >= 0) {
      cluster_config_.nodes.push_back(peer_id);
    }
  }
  cluster_config_.version = 1;

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

    // Initialize and start LogPersister
    LogPersistenceConfig log_config;
    log_config.batch_size = config_.max_entries_per_append;
    log_config.batch_interval_ms = config_.heartbeat_interval_ms / 2;
    log_persister_ =
        std::make_unique<LogPersister>(std::move(persister_), log_config);
    log_persister_->Start();

    // Restore log entries from disk
    auto restored_entries = log_persister_->Restore(log_.GetFirstIndex());
    for (const auto& entry : restored_entries) {
      log_.AppendLogEntry(entry);
    }

    // All restored entries are already durably persisted
    flushed_index_ = log_.LastLogIndex();
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

  // 4. Start metrics HTTP server
  if (metrics_ && !config_.metrics_addr.empty()) {
    metrics_server_ = std::make_unique<MetricsHttpServer>(
        config_.metrics_addr, metrics_.get());
    metrics_server_->Start();
  }

  // 5. Enter Follower state
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

  // 1. Stop metrics server
  if (metrics_server_) {
    metrics_server_->Stop();
    metrics_server_.reset();
  }

  // 2. Stop timers (with lock)
  {
    std::lock_guard<std::mutex> lock(mtx_);
    CancelElectionTimerLocked();
    StopHeartbeatTimerLocked();
  }

  // 3. Stop TimerService
  if (timer_) {
    timer_->Stop();
  }

  // 4. Stop NetworkTransport
  if (network_) {
    network_->Stop();
  }

  // 5. Stop LogPersister (flushes remaining entries)
  if (log_persister_) {
    log_persister_->Stop();
  }

  // 6. Clean up pending proposals
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
    if (metrics_) {
      metrics_->GetCounter("raft_propose_total", {{"node_id", std::to_string(server_id_)}, {"result", "rejected_not_leader"}})
          .Increment();
    }
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  // Append to local log
  auto [index, status] = log_.Append(current_term_, command);
  if (!status.ok()) {
    return status;
  }

  // Persist log entry (async with callback)
  if (log_persister_) {
    auto entry_opt = log_.GetEntry(index);
    if (entry_opt) {
      log_persister_->Append(
          *entry_opt, [this, index](Status s) {
            if (!s.ok()) {
              LOG_WARN("Node {} log persistence failed for index {}: {}",
                       server_id_, index, s.ToString());
              return;
            }
            std::lock_guard<std::mutex> lock(mtx_);
            if (!IsRunning()) {
              return;
            }
            if (role_ != RaftNodeRole::LEADER) {
              return;
            }
            if (index > flushed_index_) {
              flushed_index_ = index;
            }
            // Retry commit now that this entry is durable
            TryCommitLocked();
            // Replicate to followers
            BroadcastAppendEntriesLocked();
          });
    }
  } else {
    // No persistence configured (test path) — treat as immediately flushed
    flushed_index_ = std::max(flushed_index_, index);
  }

  // Record pending proposal
  PendingProposal proposal;
  proposal.index = index;
  proposal.callback = std::move(callback);
  proposal.propose_time = std::chrono::steady_clock::now();
  pending_proposals_[index] = std::move(proposal);

  if (metrics_) {
    metrics_->GetCounter("raft_propose_total", {{"node_id", std::to_string(server_id_)}, {"result", "accepted"}})
        .Increment();
  }

  // Trigger log replication only if no persister (otherwise callback triggers it)
  if (!log_persister_) {
    BroadcastAppendEntriesLocked();
  }

  return Status::OK();
}

ApplyResult RaftNode::RaftNodeImpl::ProposeAndWaitLocked(
    const std::string& command) {
  // Use promise/future for synchronous wait
  std::promise<ApplyResult> promise;
  auto future = promise.get_future();

  // Create callback that will set the promise value
  auto callback = [&promise](const ApplyResult& result) {
    promise.set_value(result);
  };

  // Append to local log
  auto [index, status] = log_.Append(current_term_, command);
  if (!status.ok()) {
    ApplyResult error_result;
    error_result.success = false;
    error_result.error_message = status.GetMessage();
    return error_result;
  }

  // Persist log entry synchronously before replication
  if (log_persister_) {
    auto entry_opt = log_.GetEntry(index);
    if (entry_opt) {
      auto flush_status = log_persister_->AppendSync(*entry_opt);
      if (!flush_status.ok()) {
        ApplyResult error_result;
        error_result.success = false;
        error_result.error_message = flush_status.GetMessage();
        return error_result;
      }
      flushed_index_ = std::max(flushed_index_, index);
    }
  } else {
    flushed_index_ = std::max(flushed_index_, index);
  }

  // Record pending proposal
  PendingProposal proposal;
  proposal.index = index;
  proposal.callback = std::move(callback);
  proposal.propose_time = std::chrono::steady_clock::now();
  pending_proposals_[index] = std::move(proposal);

  // Trigger log replication
  BroadcastAppendEntriesLocked();

  // Unlock mutex while waiting to allow other threads to make progress
  mtx_.unlock();

  // Wait for commit and apply with timeout
  auto wait_status = future.wait_for(std::chrono::seconds(5));

  mtx_.lock();

  if (wait_status == std::future_status::timeout) {
    if (metrics_) {
      metrics_->GetCounter("raft_propose_total", {{"node_id", std::to_string(server_id_)}, {"result", "timeout"}})
          .Increment();
    }
    // Remove pending proposal on timeout
    pending_proposals_.erase(index);
    ApplyResult timeout_result;
    timeout_result.success = false;
    timeout_result.error_message = "Command execution timeout";
    return timeout_result;
  }

  return future.get();
}

Status RaftNode::RaftNodeImpl::ReadIndex(std::function<void()> callback) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  // Create pending read request
  uint64_t read_id = next_read_id_++;
  PendingReadIndex read_req;
  read_req.read_index = commit_index_;
  read_req.callback = std::move(callback);
  read_req.start_time = std::chrono::steady_clock::now();
  read_req.acks.insert(server_id_);  // Leader acknowledges itself

  pending_reads_[read_id] = std::move(read_req);

  LOG_INFO("Node {} ReadIndex request {} at commit_index {}", server_id_,
           read_id, commit_index_);

  if (metrics_) {
    metrics_->GetCounter("raft_readindex_total", {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }

  // Send heartbeats to confirm leadership
  BroadcastReadIndexHeartbeatsLocked(read_id);

  return Status::OK();
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

  // Stop leader timers
  StopHeartbeatTimerLocked();
  StopSnapshotCheckTimerLocked();

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

  if (metrics_) {
    metrics_->GetGauge("raft_role", {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(RaftNodeRole::FOLLOWER));
    metrics_->GetGauge("raft_current_term", {{"node_id", std::to_string(server_id_)}})
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
    persister_->SaveState({current_term_, voted_for_});
  }

  // Invoke callback
  if (old_role != role_ && role_change_callback_) {
    role_change_callback_(role_, current_term_);
  }

  if (metrics_) {
    metrics_->GetCounter("raft_elections_total", {{"node_id", std::to_string(server_id_)}})
        .Increment();
    metrics_->GetGauge("raft_role", {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(RaftNodeRole::CANDIDATE));
    metrics_->GetGauge("raft_current_term", {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(current_term_));
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
  StartHeartbeatTimerLocked();

  // Start auto-snapshot check timer
  StartSnapshotCheckTimerLocked();

  // Invoke callback
  if (old_role != role_ && role_change_callback_) {
    role_change_callback_(role_, current_term_);
  }
  if (leader_change_callback_) {
    leader_change_callback_(server_id_, config_.listen_addr);
  }

  if (metrics_) {
    metrics_->GetCounter("raft_leader_elected_total", {{"node_id", std::to_string(server_id_)}})
        .Increment();
    metrics_->GetGauge("raft_role", {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(RaftNodeRole::LEADER));
  }
  LOG_INFO("Node {} became Leader at term {} (cleared {} client sessions)",
           server_id_, current_term_, cleared_sessions);

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
    metrics_->GetCounter("raft_snapshots_created_total",
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
    auto status = persister_->SaveSnapshot(snapshot_data, snapshot_index,
                                           snapshot_term);
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
    metrics_->GetCounter("raft_log_compactions_total",
                         {{"node_id", std::to_string(server_id_)}, {"trigger", "auto"}})
        .Increment();
    metrics_->GetCounter("raft_log_entries_compacted_total",
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

void RaftNode::RaftNodeImpl::OnElectionTimeout() {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) return;
  if (role_ == RaftNodeRole::LEADER) return;

  LOG_INFO("Node {} election timeout at term {}, becoming Candidate",
           server_id_, current_term_);

  if (metrics_) {
    metrics_->GetCounter("raft_election_timeouts_total", {{"node_id", std::to_string(server_id_)}})
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

  // Serialize request
  std::string data;
  auto status = protocol_->SerializeRequest(req, data);
  if (!status.ok()) {
    LOG_ERROR("Failed to serialize RequestVoteRequest: {}", status.ToString());
    return;
  }

  if (metrics_) {
    metrics_->GetCounter("raft_requestvote_sent_total", {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }

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

  std::lock_guard<std::mutex> lock(mtx_);

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
      metrics_->GetCounter("raft_votes_received_total", {{"node_id", std::to_string(server_id_)}, {"granted", "true"}})
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

// ========== Snapshot Replication ==========

constexpr size_t kSnapshotChunkSize = 64 * 1024;  // 64KB chunks

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
    metrics_->GetCounter("raft_snapshot_sends_started_total",
                         {{"node_id", std::to_string(server_id_)}, {"peer_id", std::to_string(peer_id)}})
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
    metrics_->GetCounter("raft_snapshot_chunks_sent_total",
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
        metrics_->GetCounter("raft_snapshot_sends_completed_total",
                             {{"node_id", std::to_string(server_id_)}, {"peer_id", std::to_string(from)}})
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

// ========== Commit and Apply ==========

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

    // Check if this is a config change command
    if (entry.data_.find("CONFIG_CHANGE:") == 0) {
      ApplyConfigChangeLocked(entry.data_);

      // Still need to callback for proposals
      auto it = pending_proposals_.find(last_applied_);
      if (it != pending_proposals_.end()) {
        ApplyResult result;
        result.success = true;
        result.applied_index = last_applied_;
        it->second.callback(result);
        pending_proposals_.erase(it);
      }
      continue;
    }

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

  if (metrics_) {
    metrics_->GetGauge("raft_applied_index", {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(last_applied_));
  }

  // Check if any pending reads can be completed
  ProcessPendingReadsLocked();
}

// ========== RPC Handling ==========

void RaftNode::RaftNodeImpl::HandleIncomingRpc(NodeId /*from*/,
                                               const std::string& data,
                                               std::string& response) {
  // First, peek at the message type to dispatch to the correct handler
  // We need to deserialize based on the type field in the JSON
  try {
    // Parse just enough to get the type
    auto j = nlohmann::json::parse(data);
    if (!j.contains("type")) {
      LOG_ERROR("Received RPC without type field");
      return;
    }

    int type_id = j["type"];
    auto message_type = static_cast<RaftMessageType>(type_id);

    switch (message_type) {
      case RaftMessageType::KRequestVoteRequest: {
        RequestVoteRequest req;
        auto status = protocol_->DeserializeRequest(data, req);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize RequestVoteRequest: {}",
                    status.ToString());
          return;
        }
        RequestVoteResponse resp;
        HandleRequestVote(req, resp);
        status = protocol_->SerializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to serialize RequestVoteResponse: {}",
                    status.ToString());
        }
        break;
      }

      case RaftMessageType::KAppendEntriesRequest: {
        AppendEntriesRequest req;
        auto status = protocol_->DeserializeRequest(data, req);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize AppendEntriesRequest: {}",
                    status.ToString());
          return;
        }
        AppendEntriesResponse resp;
        HandleAppendEntries(req, resp);
        status = protocol_->SerializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to serialize AppendEntriesResponse: {}",
                    status.ToString());
        }
        break;
      }

      case RaftMessageType::KInstallSnapshotRequest: {
        InstallSnapshotRequest req;
        auto status = protocol_->DeserializeRequest(data, req);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize InstallSnapshotRequest: {}",
                    status.ToString());
          return;
        }
        InstallSnapshotResponse resp;
        HandleInstallSnapshot(req, resp);
        status = protocol_->SerializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to serialize InstallSnapshotResponse: {}",
                    status.ToString());
        }
        break;
      }

      case RaftMessageType::KClientRequest: {
        ClientRequest req;
        auto status = protocol_->DeserializeRequest(data, req);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize ClientRequest: {}",
                    status.ToString());
          return;
        }
        ClientResponse resp;
        HandleClientRequest(req, resp);
        status = protocol_->SerializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to serialize ClientResponse: {}",
                    status.ToString());
        }
        break;
      }

      default:
        LOG_ERROR("Unknown message type: {}", type_id);
        break;
    }

  } catch (const std::exception& e) {
    LOG_ERROR("Failed to handle incoming RPC: {}", e.what());
  }
}

void RaftNode::RaftNodeImpl::HandleRequestVote(const RequestVoteRequest& req,
                                               RequestVoteResponse& resp) {
  std::lock_guard<std::mutex> lock(mtx_);

  resp.term_ = current_term_;
  resp.vote_granted_ = false;

  if (metrics_) {
    metrics_->GetCounter("raft_requestvote_received_total",
                         {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }

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
    if (metrics_) {
      metrics_->GetCounter("raft_votes_granted_total",
                           {{"node_id", std::to_string(server_id_)}})
          .Increment();
    }
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

  if (metrics_) {
    metrics_->GetCounter("raft_appendentries_received_total",
                         {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }

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

      // Persist log entry (async)
      if (log_persister_) {
        log_persister_->Append(entry);
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
  std::lock_guard<std::mutex> lock(mtx_);

  resp.term_ = current_term_;

  // Term check: reject stale leader
  if (req.term_ < current_term_) {
    LOG_DEBUG("Node {} reject InstallSnapshot: req.term {} < {}", server_id_,
              req.term_, current_term_);
    return;
  }

  // Higher term: revert to follower
  if (req.term_ > current_term_) {
    LOG_INFO("Node {} term {} < {}, reverting to Follower", server_id_,
             current_term_, req.term_);
    BecomeFollowerLocked(req.term_);
    resp.term_ = current_term_;
  }

  // Update leader info
  leader_id_ = req.leader_id_;
  auto it = peer_map_.find(leader_id_);
  if (it != peer_map_.end()) {
    leader_addr_ = it->second;
  }

  // Reset election timer (we have a valid leader)
  ResetElectionTimerLocked();

  if (metrics_) {
    metrics_->GetCounter("raft_snapshots_received_total",
                         {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }

  // Handle snapshot chunk
  if (req.offset_ == 0) {
    // New snapshot transfer, clear buffer
    snapshot_temp_data_.clear();
    LOG_INFO("Node {} starting snapshot receive: index={}, term={}", server_id_,
             req.last_included_index_, req.last_included_term_);
  }

  // Append chunk data
  snapshot_temp_data_.append(req.data_.data(), req.data_.size());
  LOG_DEBUG("Node {} received snapshot chunk: offset={}, size={}, done={}",
            server_id_, req.offset_, req.data_.size(), req.done_);

  // Final chunk: restore state machine
  if (req.done_) {
    LOG_INFO(
        "Node {} restoring from snapshot: {} bytes, up to index {} term {}",
        server_id_, snapshot_temp_data_.size(), req.last_included_index_,
        req.last_included_term_);

    // Restore state machine
    std::vector<uint8_t> snapshot_bytes(snapshot_temp_data_.begin(),
                                        snapshot_temp_data_.end());

    if (!state_machine_->Restore(snapshot_bytes)) {
      LOG_ERROR("Node {} failed to restore from snapshot", server_id_);
      // Clear buffer and wait for leader to retry
      snapshot_temp_data_.clear();
      return;
    }

    // Update log: discard all entries covered by snapshot
    uint64_t old_first_index = log_.GetFirstIndex();
    log_.SetStartIndex(req.last_included_index_ + 1);
    last_snapshot_index_ = req.last_included_index_;

    // Truncate persisted log
    if (log_persister_) {
      auto status = log_persister_->TruncatePrefix(req.last_included_index_ + 1);
      if (!status.ok()) {
        LOG_WARN("Node {} failed to truncate persisted log after snapshot: {}",
                 server_id_, status.ToString());
      }
    }

    if (metrics_) {
      metrics_->GetCounter("raft_log_compactions_total",
                           {{"node_id", std::to_string(server_id_)}, {"trigger", "snapshot"}})
          .Increment();
      if (req.last_included_index_ >= old_first_index) {
        uint64_t compacted = req.last_included_index_ - old_first_index + 1;
        metrics_->GetCounter("raft_log_entries_compacted_total",
                             {{"node_id", std::to_string(server_id_)}})
            .Increment(compacted);
      }
    }

    // Update indices
    last_applied_ = req.last_included_index_;
    commit_index_ = req.last_included_index_;

    // Persist snapshot if persister available
    if (persister_) {
      auto status = persister_->SaveSnapshot(snapshot_temp_data_,
                                             req.last_included_index_,
                                             req.last_included_term_);
      if (!status.ok()) {
        LOG_WARN("Node {} failed to persist snapshot: {}", server_id_,
                 status.ToString());
        // Non-fatal: we can continue, snapshot will be resent if needed
      }
    }

    // Clear buffer
    snapshot_temp_data_.clear();

    LOG_INFO(
        "Node {} successfully restored from snapshot, log start={}, "
        "commit_index={}",
        server_id_, log_.GetFirstIndex(), commit_index_);
  }
}

void RaftNode::RaftNodeImpl::HandleClientRequest(const ClientRequest& req,
                                                 ClientResponse& resp) {
  mtx_.lock();

  // Check if we are the leader
  if (role_ != RaftNodeRole::LEADER) {
    resp.success = false;
    resp.error = "Not leader";
    resp.leader_id = leader_id_;
    resp.leader_addr = leader_addr_;
    mtx_.unlock();
    return;
  }

  // For read-only requests, query state machine directly (may be stale)
  if (req.read_only) {
    // TODO: Implement linearizable read using ReadIndex
    resp.success = false;
    resp.error = "Read not yet implemented";
    mtx_.unlock();
    return;
  }

  // Get or create client session for idempotency
  auto& session = client_sessions_[req.client_id];
  session.last_active = std::chrono::steady_clock::now();

  // Case 1: Old request (seq < last_seq) - already executed, return cached
  if (req.seq < session.last_seq) {
    resp.success = true;
    resp.response = session.last_response;
    resp.last_applied_index = session.last_index;
    resp.leader_id = server_id_;
    resp.leader_addr = config_.listen_addr;
    LOG_INFO("Client {} seq {} is old (last={}), returning cached result",
             req.client_id, req.seq, session.last_seq);
    mtx_.unlock();
    return;
  }

  // Case 2: Duplicate request (seq == last_seq) - return cached result
  if (req.seq == session.last_seq) {
    resp.success = true;
    resp.response = session.last_response;
    resp.last_applied_index = session.last_index;
    resp.leader_id = server_id_;
    resp.leader_addr = config_.listen_addr;
    LOG_INFO("Client {} seq {} is duplicate, returning cached result",
             req.client_id, req.seq);
    mtx_.unlock();
    return;
  }

  // Case 3: New request (seq > last_seq) - execute normally
  auto result = ProposeAndWaitLocked(req.command);

  // Update session cache if successful
  if (result.success) {
    session.last_seq = req.seq;
    session.last_response = result.response;
    session.last_index = result.applied_index;
    session.last_term = current_term_;
  }

  resp.success = result.success;
  resp.response = result.response;
  resp.error = result.error_message;
  resp.last_applied_index = result.applied_index;
  resp.leader_id = server_id_;
  resp.leader_addr = config_.listen_addr;

  mtx_.unlock();
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
void RaftNode::RaftNodeImpl::BroadcastReadIndexHeartbeatsLocked(
    uint64_t read_id) {
  if (metrics_) {
    metrics_->GetCounter("raft_readindex_heartbeats_sent_total",
                         {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }

  // Send empty AppendEntries (heartbeats) to all peers
  for (const auto& [peer_id, addr] : peer_map_) {
    (void)addr;

    AppendEntriesRequest req;
    req.term_ = current_term_;
    req.leader_id_ = server_id_;
    req.prev_log_index_ = next_index_[peer_id] - 1;
    req.prev_log_term_ = GetLogTermLocked(req.prev_log_index_);
    req.leader_commit_ = commit_index_;
    // Empty entries = heartbeat

    std::string data;
    auto status = protocol_->SerializeRequest(req, data);
    if (!status.ok()) {
      LOG_ERROR("Failed to serialize heartbeat: {}", status.ToString());
      continue;
    }

    auto it_addr = peer_map_.find(peer_id);
    if (it_addr == peer_map_.end()) continue;

    network_->SendRpc(
        peer_id, it_addr->second, data,
        std::chrono::milliseconds(config_.rpc_timeout_ms),
        [this, peer_id, read_id](const std::string& resp, bool success,
                                 const std::string& error) {
          if (!success) {
            LOG_WARN("ReadIndex heartbeat to {} failed: {}", peer_id, error);
            return;
          }

          AppendEntriesResponse response;
          auto status = protocol_->DeserializeResponse(resp, response);
          if (!status.ok()) {
            LOG_ERROR("Failed to deserialize heartbeat response: {}",
                      status.ToString());
            return;
          }

          if (response.success_) {
            std::lock_guard<std::mutex> lock(mtx_);
            HandleReadIndexAckLocked(peer_id, read_id);
          }
        });
  }

  // Mark heartbeats as sent
  auto it = pending_reads_.find(read_id);
  if (it != pending_reads_.end()) {
    it->second.heartbeats_sent = true;
  }
}

void RaftNode::RaftNodeImpl::HandleReadIndexAckLocked(NodeId from,
                                                      uint64_t read_id) {
  if (metrics_) {
    metrics_->GetCounter("raft_readindex_acks_received_total",
                         {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }

  auto it = pending_reads_.find(read_id);
  if (it == pending_reads_.end()) return;

  auto& read_req = it->second;
  read_req.acks.insert(from);

  // Check if we have majority
  int majority = (peer_addrs_.size() + 1) / 2 + 1;
  if (static_cast<int>(read_req.acks.size()) >= majority) {
    LOG_INFO("ReadIndex {} received majority acks ({}/{})", read_id,
             read_req.acks.size(), peer_addrs_.size() + 1);

    // Check if read_index is already applied
    if (last_applied_ >= read_req.read_index) {
      // Can complete immediately
      auto callback = std::move(read_req.callback);
      pending_reads_.erase(it);
      mtx_.unlock();
      callback();
      mtx_.lock();
    }
    // Otherwise, will be completed when log is applied
  }
}

void RaftNode::RaftNodeImpl::ProcessPendingReadsLocked() {
  std::vector<uint64_t> completed_reads;

  for (auto& [read_id, read_req] : pending_reads_) {
    // Check if we have majority acks and log is applied
    int majority = (peer_addrs_.size() + 1) / 2 + 1;
    if (static_cast<int>(read_req.acks.size()) >= majority &&
        last_applied_ >= read_req.read_index) {
      completed_reads.push_back(read_id);
    }
  }

  // Complete the reads (outside the loop to avoid iterator invalidation)
  for (uint64_t read_id : completed_reads) {
    auto it = pending_reads_.find(read_id);
    if (it != pending_reads_.end()) {
      auto callback = std::move(it->second.callback);
      pending_reads_.erase(it);

      if (metrics_) {
        metrics_->GetCounter("raft_readindex_completed_total",
                             {{"node_id", std::to_string(server_id_)}})
            .Increment();
      }

      LOG_DEBUG("Completing ReadIndex {}", read_id);
      mtx_.unlock();
      callback();
      mtx_.lock();
    }
  }
}

// ========== Membership Change Implementation ==========

Status RaftNode::RaftNodeImpl::AddNode(NodeId id, const NodeAddr& addr) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  // Check if node already exists
  if (cluster_config_.Contains(id)) {
    return Status::Error("Node already in cluster");
  }

  // Check if only changing one node at a time
  // (This is a simplified check - in production, track pending changes)

  // Create config change entry as a special command
  std::string cmd = "CONFIG_CHANGE:ADD:" + std::to_string(id) + ":" + addr;

  // Propose as normal log entry
  auto [index, status] = log_.Append(current_term_, cmd);
  if (!status.ok()) {
    return status;
  }

  // Persist log entry
  if (log_persister_) {
    auto entry_opt = log_.GetEntry(index);
    if (entry_opt) {
      log_persister_->Append(*entry_opt);
    }
  }

  // Add to peer map immediately (optimistic)
  peer_map_[id] = addr;
  next_index_[id] = log_.GetLastLogInfo().first + 1;
  match_index_[id] = 0;

  LOG_INFO("Node {} proposing AddNode for {} at index {}", server_id_, id,
           index);

  // Trigger replication
  BroadcastAppendEntriesLocked();

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::RemoveNode(NodeId id) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  // Check if node exists
  if (!cluster_config_.Contains(id)) {
    return Status::Error("Node not in cluster");
  }

  // Prevent removing ourselves while leader
  // (We should step down first)
  if (id == server_id_) {
    LOG_WARN("Node {} removing itself from cluster - will step down", id);
  }

  // Create config change entry
  std::string cmd = "CONFIG_CHANGE:REMOVE:" + std::to_string(id);

  // Propose as normal log entry
  auto [index, status] = log_.Append(current_term_, cmd);
  if (!status.ok()) {
    return status;
  }

  // Persist log entry
  if (log_persister_) {
    auto entry_opt = log_.GetEntry(index);
    if (entry_opt) {
      log_persister_->Append(*entry_opt);
    }
  }

  // Remove from peer map immediately (optimistic)
  peer_map_.erase(id);
  next_index_.erase(id);
  match_index_.erase(id);

  // Remove from peer_addrs_
  peer_addrs_.erase(std::remove_if(peer_addrs_.begin(), peer_addrs_.end(),
                                   [id, this](const NodeAddr& a) {
                                     return ParseNodeId(a) == id;
                                   }),
                    peer_addrs_.end());

  LOG_INFO("Node {} proposing RemoveNode for {} at index {}", server_id_, id,
           index);

  // Trigger replication
  BroadcastAppendEntriesLocked();

  // If removing ourselves, step down
  if (id == server_id_) {
    BecomeFollowerLocked(current_term_);
  }

  return Status::OK();
}

ClusterConfig RaftNode::RaftNodeImpl::GetConfig() const {
  std::lock_guard<std::mutex> lock(config_mutex_);
  return cluster_config_;
}

void RaftNode::RaftNodeImpl::ApplyConfigChangeLocked(const std::string& cmd) {
  // Parse config change command
  // Format: CONFIG_CHANGE:ADD:node_id:addr  or  CONFIG_CHANGE:REMOVE:node_id

  if (cmd.find("CONFIG_CHANGE:ADD:") == 0) {
    // Parse ADD command
    size_t pos1 = strlen("CONFIG_CHANGE:ADD:");
    size_t pos2 = cmd.find(':', pos1);
    if (pos2 == std::string::npos) {
      LOG_ERROR("Invalid ADD config change command: {}", cmd);
      return;
    }

    NodeId id = std::stoll(cmd.substr(pos1, pos2 - pos1));
    NodeAddr addr = cmd.substr(pos2 + 1);

    std::lock_guard<std::mutex> config_lock(config_mutex_);

    // Add to config if not already present
    if (!cluster_config_.Contains(id)) {
      cluster_config_.nodes.push_back(id);
      cluster_config_.version++;

      // Update peer map if not already present
      if (id != server_id_ && peer_map_.find(id) == peer_map_.end()) {
        peer_map_[id] = addr;
        peer_addrs_.push_back(addr);

        // Initialize leader state if leader
        if (role_ == RaftNodeRole::LEADER) {
          next_index_[id] = log_.GetLastLogInfo().first + 1;
          match_index_[id] = 0;
        }
      }

      LOG_INFO("Node {} applied AddNode for {} (config version {})", server_id_,
               id, cluster_config_.version);
    }

  } else if (cmd.find("CONFIG_CHANGE:REMOVE:") == 0) {
    // Parse REMOVE command
    size_t pos = strlen("CONFIG_CHANGE:REMOVE:");
    NodeId id = std::stoll(cmd.substr(pos));

    std::lock_guard<std::mutex> config_lock(config_mutex_);

    // Remove from config
    cluster_config_.nodes.erase(std::remove(cluster_config_.nodes.begin(),
                                            cluster_config_.nodes.end(), id),
                                cluster_config_.nodes.end());
    cluster_config_.version++;

    // Remove from peer map
    peer_map_.erase(id);
    next_index_.erase(id);
    match_index_.erase(id);

    // Remove from peer_addrs_
    peer_addrs_.erase(std::remove_if(peer_addrs_.begin(), peer_addrs_.end(),
                                     [id, this](const NodeAddr& a) {
                                       return ParseNodeId(a) == id;
                                     }),
                      peer_addrs_.end());

    LOG_INFO("Node {} applied RemoveNode for {} (config version {})",
             server_id_, id, cluster_config_.version);

    // If we removed ourselves, stop
    if (id == server_id_) {
      LOG_INFO("Node {} removed from cluster, stopping", server_id_);
      // Schedule stop (can't hold lock during Stop)
      timer_->SetTimeout(std::chrono::milliseconds(0), [this]() { Stop(); });
    }
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

void RaftNode::SetRoleChangeCallback(
    std::function<void(RaftNodeRole role, Term term)> callback) {
  raft_node_impl_->SetRoleChangeCallback(std::move(callback));
}

void RaftNode::SetLeaderChangeCallback(
    std::function<void(NodeId leader_id, const NodeAddr& addr)> callback) {
  raft_node_impl_->SetLeaderChangeCallback(std::move(callback));
}

Status RaftNode::Propose(
    const std::string& command,
    std::function<void(const ApplyResult& result)> callback) {
  return raft_node_impl_->Propose(command, std::move(callback));
}

Status RaftNode::ReadIndex(std::function<void()> callback) {
  return raft_node_impl_->ReadIndex(std::move(callback));
}

Status RaftNode::AddNode(NodeId id, const NodeAddr& addr) {
  return raft_node_impl_->AddNode(id, addr);
}

Status RaftNode::RemoveNode(NodeId id) {
  return raft_node_impl_->RemoveNode(id);
}

ClusterConfig RaftNode::GetConfig() const {
  return raft_node_impl_->GetConfig();
}
