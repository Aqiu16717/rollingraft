#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <shared_mutex>
#include <span>
#include <unordered_map>
#include <vector>

#include "rollingraft/client_session_manager.h"
#include "rollingraft/event.h"
#include "rollingraft/log_persister.h"
#include "rollingraft/logger.h"
#include "rollingraft/metrics.h"
#include "rollingraft/network_transport.h"
#include "rollingraft/persister.h"
#include "rollingraft/protocol.h"
#include "rollingraft/raft_log.h"
#include "rollingraft/raft_node.h"
#include "rollingraft/rpc.h"
#include "rollingraft/runtime_config.h"
#include "rollingraft/state_machine.h"
#include "rollingraft/timer_service.h"
#include "rollingraft/types.h"

#include "metrics_http_server.h"
#include "raft_group.h"
#include "shared_node_infra.h"

namespace rollingraft {

// ========== RaftNode Implementation ==========
class RaftNode::RaftNodeImpl {
 public:
  RaftNodeImpl(const RaftNodeConfig& config, std::shared_ptr<StateMachine> state_machine,
               std::shared_ptr<SharedNodeInfra> infra, std::shared_ptr<Persister> persister);
  ~RaftNodeImpl();

  Status Start();
  Status Stop();

 private:
  void DoGracefulShutdown();
  void ForceShutdown();

 public:
  bool IsLeader() const;
  RaftNodeRole GetRole() const;
  Term CurrentTerm() const;
  std::string GetLeaderAddr() const;
  Index GetCommitIndex() const;

  EventBus& GetEventBus() { return event_bus_; }

  void SetRoleChangeCallback(std::function<void(RaftNodeRole, uint64_t)> cb);
  void SetLeaderChangeCallback(std::function<void(NodeId, std::string)> cb);

  Status Propose(const std::string& command, std::function<void(const ApplyResult&)> callback,
                 uint64_t session_id = 0, uint64_t seq_num = 0);
  Status ProposeBatch(const std::vector<std::string>& commands,
                      std::function<void(const std::vector<ApplyResult>& results)> callback);
  ApplyResult ProposeAndWaitLocked(const std::string& command,
                                   std::unique_lock<std::mutex>& lock_r);
  Status ReadIndex(std::function<void()> callback);

  // Membership change (only for leader)
  Status AddNode(NodeId id, const NodeAddr& addr);
  Status AddLearner(NodeId id, const NodeAddr& addr);
  Status PromoteLearner(NodeId id);
  Status RemoveNode(NodeId id);
  ClusterConfig GetConfig() const;

  // Snapshot & leadership transfer
  Status TriggerSnapshot();
  Status TransferLeadershipTo(NodeId target_id);

  // RPC handlers (called by NetworkTransport)
  void HandleRequestVote(const RequestVoteRequest&, RequestVoteResponse&);
  void HandlePreVote(const PreVoteRequest&, PreVoteResponse&);
  void HandleAppendEntries(const AppendEntriesRequest&, AppendEntriesResponse&);
  void HandleInstallSnapshot(const InstallSnapshotRequest&, InstallSnapshotResponse&);
  void HandleClientRequest(const ClientRequest&, ClientResponse&);
  void HandleReadIndexRequest(const ReadIndexRequest&, ReadIndexResponse&);

 private:
  // State transitions (must hold group_->election_mtx_ when calling)
  void BecomeFollowerLocked(Term term);
  void BecomeCandidateLocked();
  void BecomeLeaderLocked();

  // Timer management (must hold appropriate manager mtx when calling)
  void ResetElectionTimerLocked();
  void CancelElectionTimerLocked();
  void StartHeartbeatTimerLocked();
  void StopHeartbeatTimerLocked();
  void StartSnapshotCheckTimerLocked();
  void StopSnapshotCheckTimerLocked();

  // Quiesced mode
  void RecordActivityLocked();
  bool ShouldEnterQuiescedLocked() const;
  void EnterQuiescedLocked();
  void ExitQuiescedLocked();

  // Snapshot related
  void MaybeTriggerAutoSnapshotLocked();
  void DoSnapshotLocked(const std::string& trigger);

  // Election related
  void BroadcastRequestVoteLocked();
  void SendRequestVoteToPeerLocked(NodeId peer_id, const NodeAddr& addr);
  void HandleRequestVoteResponse(NodeId from, const RequestVoteResponse& resp, Term original_term);
  void BroadcastPreVoteLocked();
  void SendPreVoteToPeerLocked(NodeId peer_id, const NodeAddr& addr);
  void HandlePreVoteResponse(NodeId from, const PreVoteResponse& resp, Term original_term);

  // Log replication related
  void BroadcastAppendEntriesLocked();
  void SendAppendEntriesToPeerLocked(NodeId peer_id);
  void HandleAppendEntriesResponse(NodeId from, const AppendEntriesResponse& resp);
  void HandleHeartbeatResponse(NodeId from, const AppendEntriesResponse& resp);
  void ScheduleAppendEntriesRetry(NodeId peer_id, bool is_heartbeat = false);
  void ScheduleAppendEntriesRetryLocked(
      NodeId peer_id);  // Precondition: caller holds
                        // group_->election_mtx_ + group_->replication_mtx_

  // CheckQuorum: leader steps down if it hasn't received quorum acks
  void CheckQuorumLocked();  // Precondition: caller holds group_->election_mtx_

  // ReadIndex related
  void BroadcastReadIndexHeartbeatsLocked(uint64_t read_id);
  void HandleReadIndexAckLocked(NodeId from, uint64_t read_id);
  void ProcessPendingReadsLocked();

  // Dead node detection & auto-removal (leader only)
  void MaybeRemoveDeadNodesLocked();

  // Snapshot related
  void SendInstallSnapshotToPeerLocked(NodeId peer_id);
  void SendNextSnapshotChunkLocked(NodeId peer_id);
  void HandleInstallSnapshotResponse(NodeId from, const InstallSnapshotResponse& resp,
                                     bool rpc_success);

  // Commit and apply
  void TryCommitLocked();
  void ApplyCommittedLocked();

  // Utility methods
  uint64_t GetLogTermLocked(uint64_t index);

  // Metrics helpers (must hold appropriate locks; see design-metrics.md)
  void UpdateLeaderLeaseMetricLocked();
  void SetPeerReplicationLagMetricLocked(NodeId peer_id);

  // Membership change
  void ApplyConfigChangeLocked(const std::string& cmd);
  void MaybeAutoPromoteLearnersLocked();

  // Timeout handlers
  void OnElectionTimeout();
  void OnHeartbeatTimeout();

  // RPC entry point
  void HandleIncomingRpc(NodeId from, const std::string& data, std::string& response);

  // Async apply loop
  void ApplyLoop();

  // State check
  bool IsRunning() const { return state_ == NodeState::kRunning; }

 private:
  // ========== Group-local state machine ==========
  std::shared_ptr<RaftGroup> group_;

  // ========== Dependencies ==========
  std::shared_ptr<SharedNodeInfra> infra_;
  std::shared_ptr<Persister> persister_;
  std::unique_ptr<LogPersister> log_persister_;

  // Cached pointers into infra_ so existing code paths can keep using
  // network_->, timer_->, metrics_->, etc. without churn.
  NetworkTransport* network_ = nullptr;
  TimerService* timer_ = nullptr;
  Protocol* protocol_ = nullptr;
  MetricsRegistry* metrics_ = nullptr;
  MetricsHttpServer* metrics_server_ = nullptr;
  RuntimeConfig* runtime_config_ = nullptr;

  // ========== Runtime State ==========
  enum class NodeState { kInitialized = 0, kRunning = 1, kStopping = 2, kStopped = 3 };
  std::atomic<NodeState> state_{NodeState::kInitialized};
  std::atomic<uint64_t> next_correlation_id_{1};

  // ========== Event Bus ==========
  EventBus event_bus_;
};

}  // namespace rollingraft
