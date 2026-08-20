#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
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
// enable_shared_from_this lets outbound async callbacks (RPC responses,
// timers) hold a weak_ptr and bail out when the group has been destroyed
// (e.g. RaftStore::RemoveGroup) instead of dereferencing freed memory.
class RaftNode::RaftNodeImpl : public std::enable_shared_from_this<RaftNodeImpl> {
 public:
  RaftNodeImpl(const RaftNodeConfig& config, std::shared_ptr<StateMachine> state_machine,
               std::shared_ptr<SharedNodeInfra> infra, std::shared_ptr<Persister> persister,
               uint64_t group_id = 0, bool manage_network = true);
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
  NodeId GetLeaderId() const;
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

  // RPC entry point (public so RaftStore can route multi-raft group messages)
  void HandleIncomingRpc(NodeId from, const std::string& data, std::string& response);

  // Coarse-grained tick dispatched by RaftStore.  Drives group-local timeouts
  // (election, etc.) from the shared TimerService instead of one timer per
  // group.  Safe to call when the group is not running — it is a no-op.
  void OnStoreTick();

  // RPC handlers (called by NetworkTransport)
  void HandleRequestVote(const RequestVoteRequest&, RequestVoteResponse&);
  void HandlePreVote(const PreVoteRequest&, PreVoteResponse&);
  void HandleAppendEntries(const AppendEntriesRequest&, AppendEntriesResponse&);
  void HandleInstallSnapshot(const InstallSnapshotRequest&, InstallSnapshotResponse&);
  void HandleClientRequest(const ClientRequest&, ClientResponse&);
  void HandleReadIndexRequest(const ReadIndexRequest&, ReadIndexResponse&);

  // Snapshot receive helpers (two-phase: heavy I/O outside the manager locks).
  bool RestoreFromSnapshotFile(const std::string& temp_path, Index index, Term term);
  void PersistSnapshotFile(const std::string& temp_path, Index index, Term term);

  // Serialize a raft event into the SSE JSON payload (event name, node_id,
  // group_id, timestamp, type-specific fields). Shared by the legacy
  // single-group broadcast and the multi-raft store subscription.
  static std::string FormatSseEvent(const RaftEvent& event, NodeId node_id, uint64_t group_id);

 private:
  // State transitions (must hold group_->election_mtx_ when calling)
  void BecomeFollowerLocked(Term term);
  void BecomeFollowerWithReplicationLocked(Term term);
  void BecomeFollowerLockedImpl(Term term, bool replication_lock_held);
  void BecomeCandidateLocked();
  void BecomeLeaderLocked();

  // Timer management (must hold appropriate manager mtx when calling)
  void ResetElectionTimerLocked();
  void CancelElectionTimerLocked();
  void StartHeartbeatTimerLocked(uint32_t interval_ms = 0);
  void StopHeartbeatTimerLocked();
  void StartSnapshotCheckTimerLocked();
  void StopSnapshotCheckTimerLocked();

  // Quiesced mode
  void RecordActivityLocked();
  bool ShouldEnterQuiescedLocked() const;
  void EnterQuiescedLocked();
  void ExitQuiescedLocked();

  // Snapshot related
  // Two-phase snapshot creation: snapshot data is created and persisted
  // WITHOUT holding manager locks (user state machine + disk I/O can be slow),
  // then applied under locks. snapshot_in_progress_ guards against re-entry.
  bool ShouldTriggerSnapshotLocked();
  Status CreateAndPersistSnapshot(const std::string& trigger, Index& out_index, Term& out_term);
  void ApplySnapshotLocked(Index snapshot_index, Term snapshot_term, const std::string& trigger);
  Status RunAutoSnapshotIfNeeded();

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

  // Election quorum check: true if the given voter set reaches a majority of
  // the new config and, during joint consensus, also of the old config.
  // Acquires group_->membership_mtx_ (shared) internally.
  bool ElectionQuorumSatisfiedLocked(const std::set<NodeId>& voters);
  // Lock-free core of the above; caller must already hold membership_mtx_
  // (shared or unique). Used by paths that hold the lock (e.g. ReadIndex).
  static bool QuorumSatisfied(const ClusterConfig& config, const std::set<NodeId>& voters);

  // ReadIndex related
  void BroadcastReadIndexHeartbeatsLocked(uint64_t read_id);
  void HandleReadIndexAckLocked(NodeId from, uint64_t read_id);
  void ProcessPendingReadsLocked();

  // Dead node detection & auto-removal (leader only)
  void MaybeRemoveDeadNodesLocked();

  // Snapshot related
  void SendInstallSnapshotToPeerLocked(NodeId peer_id);
  void PrepareSnapshotForPeer(NodeId peer_id);  // Unlocked: CreateSnapshot + relock/send
  void SendNextSnapshotChunkLocked(NodeId peer_id);
  void HandleInstallSnapshotResponse(NodeId from, const InstallSnapshotResponse& resp,
                                     bool rpc_success);

  // Commit and apply
  void TryCommitLocked();
  void ApplyCommittedLocked();
  // Persist-completion callback for Propose/ProposeBatch log appends.
  // Steps down on disk failure; otherwise advances flushed_index_ and retries
  // commit. Runs on the persister thread with no group locks held.
  void OnLogEntryPersisted(Index index, Status status);
  // Build the full PersistentState (term, vote, cluster membership) from
  // group state. Every SaveState call must persist the cluster membership
  // too, otherwise an election-time save would wipe it.
  // Precondition: group_->election_mtx_ held; acquires membership_mtx_ shared.
  PersistentState CurrentPersistentStateLocked();
  // Persist the current cluster membership (called after each applied config
  // change). Precondition: group_->membership_mtx_ held (unique or shared).
  void PersistClusterConfigLocked();

  // Utility methods
  uint64_t GetLogTermLocked(uint64_t index);

  // Metrics helpers (must hold appropriate locks; see design-metrics.md)
  void UpdateLeaderLeaseMetricLocked();
  void SetPeerReplicationLagMetricLocked(NodeId peer_id);

  // Membership change
  void ApplyConfigChangeLocked(const std::string& cmd);
  void MaybeAutoPromoteLearnersLocked();

  // Timeout handlers (tick-driven)
  void OnElectionTimeout();
  void OnElectionTimeoutLocked();  // Precondition: caller holds group_->election_mtx_
  void OnHeartbeatTimeout();
  void OnHeartbeatTimeoutLocked();  // Precondition: caller holds election_mtx_ + replication_mtx_
  void CheckHeartbeatTimeoutLocked();
  void CheckSnapshotTimeoutLocked();

  // Async apply loop
  void ApplyLoop();

  // State check
  bool IsRunning() const { return state_ == NodeState::kRunning; }

  // Coarse tick for the legacy single-group path.  Multi-raft groups are
  // ticked by RaftStore's shared timer instead.
  TimerId tick_timer_ = 0;

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
  bool manage_network_ = true;
  // Set while a two-phase snapshot is between the unlocked I/O and the locked
  // apply, so a concurrent tick cannot start a second snapshot.
  std::atomic<bool> snapshot_in_progress_{false};
  // Serializes all SaveSnapshotStream writes for this group (leader-side
  // creation and receive-side install). NOT part of the manager lock
  // hierarchy: acquire it BEFORE the manager locks, never after holding one.
  std::mutex snapshot_io_mtx_;

  // ========== Event Bus ==========
  EventBus event_bus_;
};

}  // namespace rollingraft
