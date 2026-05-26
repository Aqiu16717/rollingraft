#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <mutex>
#include <random>
#include <set>
#include <shared_mutex>
#include <span>
#include <unordered_map>
#include <vector>

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
#include "rollingraft/state_machine.h"
#include "rollingraft/timer_service.h"
#include "rollingraft/types.h"

#include "metrics_http_server.h"
#include "rollingraft/runtime_config.h"

namespace rollingraft {

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
               std::shared_ptr<Persister> persister,
               std::unique_ptr<Protocol> protocol);
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

  Status Propose(const std::string& command,
                 std::function<void(const ApplyResult&)> callback);
  Status ProposeBatch(
      const std::vector<std::string>& commands,
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
  void HandleInstallSnapshot(const InstallSnapshotRequest&,
                             InstallSnapshotResponse&);
  void HandleClientRequest(const ClientRequest&, ClientResponse&);

 private:
  // State transitions (must hold election_mtx_ when calling)
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

  // Snapshot related
  void MaybeTriggerAutoSnapshotLocked();
  void DoSnapshotLocked(const std::string& trigger);

  // Election related
  void BroadcastRequestVoteLocked();
  void SendRequestVoteToPeerLocked(NodeId peer_id, const NodeAddr& addr);
  void HandleRequestVoteResponse(NodeId from, const RequestVoteResponse& resp,
                                 Term original_term);
  void BroadcastPreVoteLocked();
  void SendPreVoteToPeerLocked(NodeId peer_id, const NodeAddr& addr);
  void HandlePreVoteResponse(NodeId from, const PreVoteResponse& resp,
                             Term original_term);

  // Log replication related
  void BroadcastAppendEntriesLocked();
  void SendAppendEntriesToPeerLocked(NodeId peer_id);
  void HandleAppendEntriesResponse(NodeId from,
                                   const AppendEntriesResponse& resp);
  void ScheduleAppendEntriesRetry(NodeId peer_id);  // With exponential backoff
  void ScheduleAppendEntriesRetryLocked(
      NodeId peer_id);  // Precondition: caller holds election_mtx_ +
                        // replication_mtx_

  // CheckQuorum: leader steps down if it hasn't received quorum acks
  void CheckQuorumLocked();  // Precondition: caller holds election_mtx_

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
  void MaybeAutoPromoteLearnersLocked();

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

  // Backpressure: number of in-flight AppendEntries per peer.
  mutable std::mutex pending_appends_mtx_;
  std::unordered_map<NodeId, size_t> pending_appends_;

  // Heartbeat coalescing: track last heartbeat sent to each peer.
  std::unordered_map<NodeId, std::chrono::steady_clock::time_point>
      last_heartbeat_sent_;

  // Pre-vote state
  uint32_t pre_vote_count_ = 0;
  bool pre_vote_running_ = false;
  Term pre_vote_term_ = 0;

  // CheckQuorum state
  bool check_quorum_enabled_ = true;  // Enabled by default
  bool pre_vote_enabled_ = true;      // Enabled by default
  std::chrono::steady_clock::time_point last_leader_contact_;
  std::unordered_map<NodeId, std::chrono::steady_clock::time_point> quorum_acks_;

  // Membership change safety: true while a CONFIG_CHANGE log entry
  // has been proposed but not yet committed. Prevents concurrent
  // membership changes which violate single-node-change safety.
  bool pending_config_change_ = false;

  // ========== Cluster Config ==========
  ClusterConfig cluster_config_;
  // Note: config_mutex_ replaced by membership_mtx_ (std::shared_mutex)
  // to allow concurrent config reads without serializing with writes.

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
  std::shared_ptr<Persister> persister_;
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
  std::atomic<uint64_t> next_correlation_id_{1};

  // ========== Thread Synchronization ==========
  // Lock hierarchy (strict left-to-right):
  //   election_mtx_ -> replication_mtx_ -> snapshot_mtx_ ->
  //   membership_mtx_ -> applier_mtx_
  // Violating this order WILL cause deadlocks.
  //
  // Cross-manager call rules:
  // * Read-only snapshot: acquire in hierarchy order (Pattern A)
  // * Mutations: drop caller lock, then call downstream (Pattern B)
  // * Callbacks: always invoke outside all locks (Pattern C)
  //
  mutable std::mutex election_mtx_;
  mutable std::mutex replication_mtx_;
  mutable std::mutex snapshot_mtx_;
  mutable std::shared_mutex membership_mtx_;  // Read-heavy config access
  mutable std::mutex applier_mtx_;

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

  // ========== Runtime Config ==========
  std::unique_ptr<RuntimeConfig> runtime_config_;

  // ========== Event Bus ==========
  EventBus event_bus_;

  // ========== Callbacks ==========
  std::function<void(RaftNodeRole, uint64_t)> role_change_callback_;
  std::function<void(NodeId, std::string)> leader_change_callback_;
};

}  // namespace rollingraft
