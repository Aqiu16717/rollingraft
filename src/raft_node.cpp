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

using namespace rollingraft;

// ========== 待处理提案 ==========
struct PendingProposal {
  Index index;                                         // 日志索引
  std::function<void(const ApplyResult&)> callback;    // 完成回调
  std::chrono::steady_clock::time_point propose_time;  // 提交时间
};

// ========== RaftNode 实现 ==========
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

  // RPC 处理器（由 NetworkTransport 调用）
  void HandleRequestVote(const RequestVoteRequest&, RequestVoteResponse&);
  void HandleAppendEntries(const AppendEntriesRequest&, AppendEntriesResponse&);
  void HandleInstallSnapshot(const InstallSnapshotRequest&,
                             InstallSnapshotResponse&);

 private:
  // 状态转换（调用时必须持有 mtx_）
  void BecomeFollowerLocked(Term term);
  void BecomeCandidateLocked();
  void BecomeLeaderLocked();

  // 定时器管理（调用时必须持有 mtx_）
  void ResetElectionTimerLocked();
  void CancelElectionTimerLocked();
  void StartHeartbeatTimerLocked();
  void StopHeartbeatTimerLocked();

  // 选举相关
  void BroadcastRequestVoteLocked();
  void SendRequestVoteToPeerLocked(NodeId peer_id, const NodeAddr& addr);
  void HandleRequestVoteResponse(NodeId from, const RequestVoteResponse& resp,
                                 Term original_term);

  // 日志复制相关
  void BroadcastAppendEntriesLocked();
  void SendAppendEntriesToPeerLocked(NodeId peer_id);
  void HandleAppendEntriesResponse(NodeId from,
                                   const AppendEntriesResponse& resp);

  // 提交和应用
  void TryCommitLocked();
  void ApplyCommittedLocked();

  // 工具方法
  uint64_t GetLogTermLocked(uint64_t index);
  NodeId ParseNodeId(const NodeAddr& addr);

  // 超时处理回调
  void OnElectionTimeout();
  void OnHeartbeatTimeout();

  // RPC 处理入口
  void HandleIncomingRpc(NodeId from, const std::string& data,
                         std::string& response);

  // 状态检查
  bool IsRunning() const { return state_ == NodeState::kRunning; }

 private:
  // ========== 节点标识 ==========
  NodeId server_id_;
  std::vector<NodeAddr> peer_addrs_;
  std::unordered_map<NodeId, NodeAddr> peer_map_;

  // ========== Raft 持久化状态 ==========
  Term current_term_ = 0;
  NodeId voted_for_ = -1;
  RaftLog log_;

  // ========== Raft 易失状态 ==========
  Index commit_index_ = 0;
  Index last_applied_ = 0;
  NodeId leader_id_ = -1;
  NodeAddr leader_addr_;
  RaftNodeRole role_ = RaftNodeRole::FOLLOWER;
  uint32_t vote_count_ = 0;

  // ========== Leader 状态 ==========
  std::unordered_map<NodeId, Index> next_index_;
  std::unordered_map<NodeId, Index> match_index_;

  // ========== 定时器状态 ==========
  int election_timeout_ = 0;
  TimerId election_timer_ = 0;
  TimerId heartbeat_timer_ = 0;

  // ========== 依赖组件 ==========
  RaftNodeConfig config_;
  std::shared_ptr<StateMachine> state_machine_;
  std::unique_ptr<NetworkTransport> network_;
  std::unique_ptr<TimerService> timer_;
  std::unique_ptr<Persister> persister_;
  std::unique_ptr<Protocol> protocol_;

  // ========== 运行时状态 ==========
  enum class NodeState {
    kInitialized = 0,
    kRunning = 1,
    kStopping = 2,
    kStopped = 3
  };
  std::atomic<NodeState> state_{NodeState::kInitialized};

  // ========== 线程同步 ==========
  mutable std::mutex mtx_;

  // ========== 待处理提案 ==========
  std::unordered_map<uint64_t, PendingProposal> pending_proposals_;

 private:
  // Volatile state on leaders

  // for each server, index of the next log entry
  // to send to that server (initialized to leader
  // last log index + 1)
  std::vector<uint32_t> next_index_;
  // for each server, index of highest log entry
  // known to be replicated on server
  // (initialized to 0, increases monotonically)
  std::vector<uint32_t> match_index_;

 private:
  std::mutex mtx_;

 private:
  using WorkGuard = asio::executor_work_guard<asio::io_context::executor_type>;
  std::unique_ptr<WorkGuard> work_guard_;
  std::unique_ptr<Server> server_;
  asio::io_context io_context_;
  std::thread io_thread_;
  std::atomic<bool> is_running_;

 private:
  RaftNodeConfig config_;
  std::shared_ptr<StateMachine> state_machine_;
  std::unique_ptr<NetworkTransport> network_;
  std::unique_ptr<TimerService> timer_;
  std::unique_ptr<Persister> persister_;
  std::unique_ptr<Protocol> protocol_;
};

void RaftNode::RaftNodeImpl::SetRole(const RaftNodeRole& role) { role_ = role; }

Status RaftNode::RaftNodeImpl::BecomeFollower() {
  LOG_INFO("Node {} become follower.", server_id_);
  SetRole(RaftNodeRole::FOLLOWER);
  RandomizeElectionTimeout();
  return Status();
}

// 1. become candidate
// 2. start election
Status RaftNode::RaftNodeImpl::BecomeCandidate() {
  ++current_term_;
  ++vote_count_;
  SetRole(RaftNodeRole::CANDIDATE);
  return Status();
}

Status RaftNode::RaftNodeImpl::BecomeLeader() {
  SetRole(RaftNodeRole::LEADER);
  // AppendEntriesRequest req{
  //     .} AppendEntries(req);
  return Status();
}

Status RaftNode::RaftNodeImpl::Election() {
  RequestVoteRequest req(current_term_, server_id_, log_.LastLogIndex(),
                         log_.LastLogTerm());

  // parallel
  std::vector<std::future<RequestVoteResponse>> fus;
  for (int i = 0; i < peers_.size(); ++i) {
    fus.push_back(std::async(std::launch::async, [&]() {
      RequestVoteResponse res;
      RequestVote(req, res);
      return res;
    }));
  }

  for (auto& fu : fus) {
    RequestVoteResponse res = fu.get();
    if (res.vote_granted_) {
      ++vote_count_;
    }
  }

  if (vote_count_ > peers_.size() / 2) {
    return Status();
  }

  return Status();
}

Status RaftNode::RaftNodeImpl::RequestVote(const RequestVoteRequest& request,
                                           RequestVoteResponse& response) {
  // lock for thread safety
  std::lock_guard<std::mutex> lock(mtx_);

  // always set response.term_ to current_term_
  response.term_ = current_term_;

  LOG_INFO("Node {} received RequestVote from {} at term {}", server_id_,
           request.candidate_id_, request.term_);

  // refuse vote if candidate's term is older than my term
  if (request.term_ < current_term_) {
    response.vote_granted_ = false;
    LOG_INFO("Rejecting vote for {}: their term {} is older than mine {}",
             request.candidate_id_, request.term_, current_term_);
    return Status::OK();
  }

  // if cnadidate's term is newer, become follower whatever my state is
  if (request.term_ > current_term_) {
    LOG_INFO("Updating term from {} to {}", current_term_, request.term_);
    // reset voted_for_
    voted_for_ = -1;
    current_term_ = request.term_;
    // todo
    // persister_->
    BecomeFollower();
  }

  // Each server will vote for at most one candidate in a
  // given term, on a first-come-first-served basis (note: Sec-
  // tion 5.4 adds an additional restriction on votes).

  return Status();
}

Status RaftNode::RaftNodeImpl::AppendEntries(
    const AppendEntriesRequest& append_entries_request,
    AppendEntriesResponse& append_enctries_response) {
  for (;;) {
    // send
  }
  return Status();
}

Status RaftNode::RaftNodeImpl::InstallSnapshot(
    const InstallSnapshotRequest& install_snapshot_request,
    InstallSnapshotResponse& install_snapshot_response) {
  return Status();
}

void RaftNode::RaftNodeImpl::RandomizeElectionTimeout() {
  constexpr uint64_t kMinElectionTimeout = 150;
  constexpr uint64_t kMaxElectionTimeout = 300;

  // Thread-safe random number generator
  // use static variables to ensure it is initialized only once
  static std::mt19937_64 rng;
  // Ensure the random number seed is initialized only once
  static std::once_flag init_flag;
  std::call_once(init_flag, []() {
    // Use system time as the seed to ensure that the random sequence is
    // different each time it is started
    rng.seed(std::chrono::system_clock::now().time_since_epoch().count());
  });

  // Generate a random number between [kMinElectionTimeout,
  // kMaxElectionTimeout)
  std::uniform_int_distribution<uint64_t> dist(kMinElectionTimeout,
                                               kMaxElectionTimeout);
  election_timeout_ = dist(rng);

  // Reset the elapsed time
  timeout_elapsed_ = 0;

  LOG_INFO("Node {} randomized election timeout to {}ms (role: {})", server_id_,
           election_timeout_, RaftNodeRoleToString(role_));
}

Status RaftNode::RaftNodeImpl::Start() {
  if (is_running_.exchange(true)) {
    return Status::OK();
  }

  LOG_INFO("Starting RaftNode {} on {}", config_.node_id, config_.listen_addr);

  Status status = server_->Start();
  if (!status.ok()) {
    LOG_ERROR("Failed to start server: {}", status.ToString());
    return status;
  }

  work_guard_ = std::make_unique<WorkGuard>(asio::make_work_guard(io_context_));

  if (!work_guard_) {
    return Status::RaftNodeStartError("Failed to allocate work_guard");
  }

  io_thread_ = std::thread([this]() { io_context_.run(); });

  status = BecomeFollower();
  if (!status.ok()) {
    LOG_ERROR("Failed to become follower: {}", status.ToString());
    // todo: cleanup or stop
    return status;
  }

  LOG_INFO("Start success: {} on {}", config_.node_id, config_.listen_addr);
  return Status::OK();
}

Status RaftNode::RaftNodeImpl::Stop() {
  if (!is_running_.exchange(false)) {
    return Status::OK();
  }

  LOG_INFO("Stopping RaftNode: {} on {}", config_.node_id, config_.listen_addr);
  if (server_) {
    server_->Stop();
  }

  if (work_guard_) {
    work_guard_.reset();
  }

  io_context_.stop();

  if (io_thread_.joinable()) {
    if (std::this_thread::get_id() == io_thread_.get_id()) {
      LOG_ERROR(
          "Stop() called from within the IO thread! Deatching instead of "
          "joining.");
      io_thread_.detach();
    } else {
      io_thread_.join();
    }
  }

  LOG_INFO("Stop success: {} on {}", config_.node_id, config_.listen_addr);
  return Status::OK();
}
