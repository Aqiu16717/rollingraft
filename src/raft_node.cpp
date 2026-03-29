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

  // ========== 回调 ==========
  std::function<void(RaftNodeRole, uint64_t)> role_change_callback_;
  std::function<void(NodeId, std::string)> leader_change_callback_;
};

// ========== 构造函数/析构函数 ==========

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

  // 构建 peer 映射表
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

// ========== 公共接口实现 ==========

Status RaftNode::RaftNodeImpl::Start() {
  NodeState expected = NodeState::kInitialized;
  if (!state_.compare_exchange_strong(expected, NodeState::kRunning)) {
    return Status::Error("Already started or stopped");
  }

  LOG_INFO("Starting RaftNode {} on {}...", config_.node_id,
           config_.listen_addr);

  // 1. 初始化持久化
  if (persister_) {
    auto status = persister_->Open(config_.data_dir);
    if (!status.ok()) {
      state_ = NodeState::kInitialized;
      return status;
    }

    // 恢复持久化状态
    PersistentState state;
    if (persister_->LoadState(state).ok()) {
      current_term_ = state.current_term;
      voted_for_ = state.voted_for;
      LOG_INFO("Restored state: term={}, voted_for={}", current_term_,
               voted_for_);
    }
  }

  // 2. 初始化网络层
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

  // 3. 启动定时器服务
  timer_->Start();

  // 4. 进入 Follower 状态
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
      return Status::OK();  // 已经停止
    }
    return Status::Error("Node not running");
  }

  LOG_INFO("Stopping RaftNode {}...", config_.node_id);

  // 1. 停止定时器（加锁）
  {

Status RaftNode::RaftNodeImpl::AppendEntries(
    const AppendEntriesRequest& append_entries_request,
    AppendEntriesResponse& append_enctries_response) {
  for (;;) {
    // send
  }
  return Status();
}

  // 3. 停止 NetworkTransport
  if (network_) {
    network_->Stop();
  }

  // 4. 关闭持久化
  if (persister_) {
    persister_->Close();
  }

  // 5. 清理待处理提案
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

  // 追加到本地日志
  auto [index, status] = log_.Append(current_term_, command);
  if (!status.ok()) {
    return status;
  }

  // 记录待处理提案
  PendingProposal proposal;
  proposal.index = index;
  proposal.callback = std::move(callback);
  proposal.propose_time = std::chrono::steady_clock::now();
  pending_proposals_[index] = std::move(proposal);

  // 触发日志复制
  BroadcastAppendEntriesLocked();

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::ReadIndex(std::function<void()> callback) {
  // TODO: 实现线性一致性读
  (void)callback;
  return Status::Error("Not implemented");
}

// ========== 状态转换 ==========

void RaftNode::RaftNodeImpl::BecomeFollowerLocked(Term term) {
  RaftNodeRole old_role = role_;

  role_ = RaftNodeRole::FOLLOWER;
  current_term_ = term;
  voted_for_ = -1;
  vote_count_ = 0;
  leader_id_ = -1;
  leader_addr_.clear();

  // 停止 Leader 定时器
  StopHeartbeatTimerLocked();

  // 重置并启动选举定时器
  ResetElectionTimerLocked();

  // 持久化
  if (persister_) {
    persister_->SaveState({current_term_, voted_for_});
  }

  // 回调
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
  vote_count_ = 1;  // 给自己投票

  // 持久化
  if (persister_) {
    persister_->SaveState({current_term_, voted_for_});
  }

  // 回调
  if (old_role != role_ && role_change_callback_) {
    role_change_callback_(role_, current_term_);
  }

  LOG_INFO("Node {} became Candidate at term {}", server_id_, current_term_);

  // 发送投票请求
  BroadcastRequestVoteLocked();

  // 重置选举定时器
  ResetElectionTimerLocked();
}

void RaftNode::RaftNodeImpl::BecomeLeaderLocked() {
  RaftNodeRole old_role = role_;

  role_ = RaftNodeRole::LEADER;
  leader_id_ = server_id_;
  leader_addr_ = config_.listen_addr;

  // 初始化 Leader 状态
  auto [last_index, _] = log_.GetLastLogInfo();
  next_index_.clear();
  match_index_.clear();

  for (const auto& [peer_id, addr] : peer_map_) {
    (void)addr;
    next_index_[peer_id] = last_index + 1;
    match_index_[peer_id] = 0;
  }

  // 停止选举定时器
  CancelElectionTimerLocked();

  // 启动心跳定时器
  StartHeartbeatTimerLocked();

  // 回调
  if (old_role != role_ && role_change_callback_) {
    role_change_callback_(role_, current_term_);
  }
  if (leader_change_callback_) {
    leader_change_callback_(server_id_, config_.listen_addr);
  }

  LOG_INFO("Node {} became Leader at term {}", server_id_, current_term_);

  // 立即发送心跳（建立权威）
  BroadcastAppendEntriesLocked();
}