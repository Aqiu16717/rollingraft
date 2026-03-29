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
    std::lock_guard<std::mutex> lock(mtx_);
    CancelElectionTimerLocked();
    StopHeartbeatTimerLocked();
  }

  // 2. 停止 TimerService
  if (timer_) {
    timer_->Stop();
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

// ========== 定时器管理 ==========

void RaftNode::RaftNodeImpl::ResetElectionTimerLocked() {
  CancelElectionTimerLocked();

  // 随机超时 [election_timeout, 2 * election_timeout)
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

// ========== 选举处理 ==========

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

  // 序列化请求
  std::string data;
  // protocol_->SerializeRequest(req, data);  // TODO: 实现序列化

  Term original_term = current_term_;  // 保存当前任期用于比较

  network_->SendRpc(
      peer_id, addr, data, std::chrono::milliseconds(config_.rpc_timeout_ms),
      [this, peer_id, original_term](const std::string& resp, bool success,
                                     const std::string& error) {
        if (!success) {
          LOG_WARN("RequestVote to {} failed: {}", peer_id, error);
          return;
        }

        RequestVoteResponse response;
        // protocol_->DeserializeResponse(resp, response); // TODO
        HandleRequestVoteResponse(peer_id, response, original_term);
      });
}

void RaftNode::RaftNodeImpl::HandleRequestVoteResponse(
    NodeId from, const RequestVoteResponse& resp, Term original_term) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!IsRunning()) return;
  if (role_ != RaftNodeRole::CANDIDATE) return;

  // 如果响应任期更高，转为 Follower
  if (resp.term_ > current_term_) {
    LOG_INFO("Node {} term {} < {}, reverting to Follower", server_id_,
             current_term_, resp.term_);
    BecomeFollowerLocked(resp.term_);
    return;
  }

  // 如果任期已改变，忽略此响应
  if (original_term != current_term_) {
    return;
  }

  // 忽略旧任期的响应
  if (resp.term_ < current_term_) {
    return;
  }

  if (resp.vote_granted_) {
    ++vote_count_;
    LOG_INFO("Node {} got vote from {}, total: {}/{}", server_id_, from,
             vote_count_, peer_addrs_.size() + 1);

    // 获得多数票，成为 Leader
    if (vote_count_ > (peer_addrs_.size() + 1) / 2) {
      BecomeLeaderLocked();
    }
  }
}

// ========== 日志复制 ==========

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

  // 获取日志条目
  auto [last_index, _] = log_.GetLastLogInfo();
  if (next_idx <= last_index) {
    Index end =
        std::min(next_idx + config_.max_entries_per_append, last_index + 1);
    req.entries_ = log_.GetEntries(next_idx, end);
  }

  // 序列化并发送
  std::string data;
  // protocol_->SerializeRequest(req, data); // TODO

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

  // 如果响应任期更高，转为 Follower
  if (resp.term_ > current_term_) {
    BecomeFollowerLocked(resp.term_);
    return;
  }

  if (resp.success_) {
    // 更新进度
    Index new_match = next_index_[from] - 1 + resp.entries_count_;
    match_index_[from] = std::max(match_index_[from], new_match);
    next_index_[from] = match_index_[from] + 1;

    // 尝试提交
    TryCommitLocked();
  } else {
    // 日志不匹配，回退
    if (resp.conflict_index_ > 0) {
      next_index_[from] = resp.conflict_index_;
    } else {
      next_index_[from] = std::max<Index>(1, next_index_[from] - 1);
    }

    // 延迟重试
    timer_->SetTimeout(std::chrono::milliseconds(10), [this, from]() {
      std::lock_guard<std::mutex> lock(mtx_);
      if (role_ == RaftNodeRole::LEADER) {
        SendAppendEntriesToPeerLocked(from);
      }
    });
  }
}

// ========== 提交和应用 ==========

void RaftNode::RaftNodeImpl::TryCommitLocked() {
  auto [last_index, _] = log_.GetLastLogInfo();

  for (Index index = last_index; index > commit_index_; --index) {
    // 只提交当前任期的日志
    if (GetLogTermLocked(index) != current_term_) {
      break;
    }

    // 统计复制到多数节点的日志
    int count = 1;  // 自己
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

    // 应用到 StateMachine
    auto result = state_machine_->Apply(
        std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(entry.data_.data()),
            entry.data_.size()),
        last_applied_);

    // 回调等待的用户
    auto it = pending_proposals_.find(last_applied_);
    if (it != pending_proposals_.end()) {
      it->second.callback(result);
      pending_proposals_.erase(it);
    }
  }
}

// ========== RPC 处理 ==========

void RaftNode::RaftNodeImpl::HandleIncomingRpc(NodeId from,
                                               const std::string& data,
                                               std::string& response) {
  (void)from;
  (void)data;
  (void)response;
  // TODO: 根据消息类型分发到具体处理器
}

void RaftNode::RaftNodeImpl::HandleRequestVote(const RequestVoteRequest& req,
                                               RequestVoteResponse& resp) {
  std::lock_guard<std::mutex> lock(mtx_);

  resp.term_ = current_term_;
  resp.vote_granted_ = false;

  // 如果请求任期更高，转为 Follower
  if (req.term_ > current_term_) {
    BecomeFollowerLocked(req.term_);
    resp.term_ = current_term_;
  }

  // 拒绝旧任期的请求
  if (req.term_ < current_term_) {
    LOG_DEBUG("Node {} reject vote: req.term {} < {}", server_id_, req.term_,
              current_term_);
    return;
  }

  // 检查日志是否至少一样新
  auto [last_index, last_term] = log_.GetLastLogInfo();

  bool log_is_up_to_date =
      (req.last_log_term_ > last_term) ||
      (req.last_log_term_ == last_term && req.last_log_index_ >= last_index);

  if (!log_is_up_to_date) {
    LOG_DEBUG("Node {} reject vote: candidate log not up-to-date", server_id_);
    return;
  }

  // 检查是否已投票
  if (voted_for_ == -1 || voted_for_ == req.candidate_id_) {
    voted_for_ = req.candidate_id_;
    resp.vote_granted_ = true;

    // 重置选举定时器
    ResetElectionTimerLocked();

    // 持久化
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

  // 如果 Leader 任期更高，转为 Follower
  if (req.term_ > current_term_) {
    BecomeFollowerLocked(req.term_);
    resp.term_ = current_term_;
  }

  // 拒绝旧任期的 Leader
  if (req.term_ < current_term_) {
    LOG_DEBUG("Node {} reject AppendEntries: req.term {} < {}", server_id_,
              req.term_, current_term_);
    return;
  }

  // 更新 Leader 信息
  leader_id_ = req.leader_id_;
  auto it = peer_map_.find(leader_id_);
  if (it != peer_map_.end()) {
    leader_addr_ = it->second;
  }

  // 重置选举定时器
  ResetElectionTimerLocked();

  // 检查 prev_log 是否匹配
  if (req.prev_log_index_ > 0) {
    Term prev_term = GetLogTermLocked(req.prev_log_index_);
    if (prev_term != req.prev_log_term_) {
      LOG_DEBUG("Node {} log mismatch at index {}: local={}, remote={}",
                server_id_, req.prev_log_index_, prev_term, req.prev_log_term_);
      resp.conflict_index_ = req.prev_log_index_;
      return;
    }
  }

  // 追加日志条目
  if (!req.entries_.empty()) {
    // 检查冲突并截断
    for (const auto& entry : req.entries_) {
      Term existing_term = GetLogTermLocked(entry.index_);
      if (existing_term != 0 && existing_term != entry.term_) {
        LOG_INFO("Node {} truncating log from index {}", server_id_,
                 entry.index_);
        log_.TruncateSuffix(entry.index_);