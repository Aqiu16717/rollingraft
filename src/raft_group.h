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
#include "rollingraft/raft_log.h"
#include "rollingraft/raft_node.h"
#include "rollingraft/runtime_config.h"
#include "rollingraft/state_machine.h"
#include "rollingraft/timer_service.h"
#include "rollingraft/types.h"

namespace rollingraft {

// Forward declaration for the PIMPL wrapper.
class RaftNode;

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
  Index read_index;                                  // The commit index to wait for
  std::function<void()> callback;                    // Completion callback
  std::chrono::steady_clock::time_point start_time;  // Request timestamp
  std::set<NodeId> acks;                             // Nodes that acknowledged
  bool heartbeats_sent = false;                      // Whether heartbeats were sent
};

/**
 * @brief Group-local Raft state machine container.
 *
 * PR-B extracts all per-group state (term, log, indices, leader state,
 * membership, locks, callbacks) out of RaftNodeImpl.  RaftNodeImpl keeps
 * node-level responsibilities: shared infrastructure wiring, lifecycle,
 * persistence adapter and the public wrapper API.
 *
 * State members are left public so that the existing split implementation
 * files can migrate incrementally without a massive method-move churn.
 * Encapsulation will improve in follow-up PRs.
 */
class RaftGroup {
 public:
  explicit RaftGroup(uint64_t group_id, const RaftNodeConfig& config,
                     std::shared_ptr<StateMachine> state_machine);

  // Basic accessors used by the wrapper.
  NodeId server_id() const { return server_id_; }
  const std::vector<NodeAddr>& peer_addrs() const { return peer_addrs_; }
  const std::unordered_map<NodeId, NodeAddr>& peer_map() const { return peer_map_; }

  static NodeId ParseNodeId(const NodeAddr& addr);

  // Retry tracking for AppendEntries
  struct RetryState {
    int attempts = 0;
    std::chrono::steady_clock::time_point last_retry;
  };

  // Pipeline replication: ordered inflight window per peer.
  struct InflightEntry {
    Index start_index;
    size_t count;
  };

  // ========== Async Apply Thread ==========
  struct ApplyTask {
    Index index;
    std::string data;
    std::function<void(const ApplyResult&)> callback;
    bool is_config_change = false;
    std::optional<std::chrono::steady_clock::time_point> propose_time;
    uint64_t session_id = 0;
    uint64_t seq_num = 0;
  };

  // ========== Per-Group Identity ==========
  uint64_t group_id_ = 0;

  // ========== Per-Group Configuration & StateMachine ==========
  RaftNodeConfig config_;
  std::unique_ptr<RuntimeConfig> runtime_config_;
  std::shared_ptr<StateMachine> state_machine_;

  // ========== Node Identity ==========
  NodeId server_id_ = -1;
  std::vector<NodeAddr> peer_addrs_;
  std::unordered_map<NodeId, NodeAddr> peer_map_;

  // ========== Raft Persistent State ==========
  Term current_term_ = 0;
  NodeId voted_for_ = -1;
  RaftLog log_;

  // ========== Raft Volatile State ==========
  Index commit_index_ = 0;
  std::atomic<Index> last_applied_{0};
  Index flushed_index_ = 0;  // Highest log index durably persisted
  NodeId leader_id_ = -1;
  NodeAddr leader_addr_;
  RaftNodeRole role_ = RaftNodeRole::FOLLOWER;
  uint32_t vote_count_ = 0;
  // Per-term voter dedup sets: a peer's (pre-)vote is only counted once, so a
  // duplicate response can never inflate the count into a false majority.
  std::set<NodeId> votes_received_;
  std::set<NodeId> pre_votes_received_;

  // ========== Leader State ==========
  std::unordered_map<NodeId, Index> next_index_;
  std::unordered_map<NodeId, Index> match_index_;
  std::unordered_map<uint64_t, ClientSession> client_sessions_;  // Legacy RPC idempotency

  // Client session manager for Propose() API idempotency
  std::unique_ptr<ClientSessionManager> session_manager_;
  // Tracks which log entries have session info for result caching on apply
  std::unordered_map<Index, std::pair<uint64_t, uint64_t>> proposal_sessions_;

  std::unordered_map<NodeId, RetryState> retry_state_;
  std::unordered_map<NodeId, std::deque<InflightEntry>> inflight_;

  // Heartbeat coalescing: track last heartbeat sent to each peer.
  std::unordered_map<NodeId, std::chrono::steady_clock::time_point> last_heartbeat_sent_;

  // Leader lease: expiry timestamp for local reads without heartbeat broadcast.
  std::chrono::steady_clock::time_point leader_lease_expiry_;

  // Pre-vote state
  uint32_t pre_vote_count_ = 0;
  bool pre_vote_running_ = false;
  Term pre_vote_term_ = 0;

  // CheckQuorum state
  bool check_quorum_enabled_ = true;  // Enabled by default
  bool pre_vote_enabled_ = true;      // Enabled by default
  bool has_leader_contact_ = false;
  std::chrono::steady_clock::time_point last_leader_contact_;
  std::unordered_map<NodeId, std::chrono::steady_clock::time_point> quorum_acks_;

  // Quiesced mode state
  std::atomic<bool> quiesced_{false};
  std::chrono::steady_clock::time_point last_activity_time_;
  uint32_t consecutive_quiesced_timeouts_ = 0;

  // Dead node detection: last time we received a valid response from each peer
  std::unordered_map<NodeId, std::chrono::steady_clock::time_point> last_contact_time_;

  // Membership change safety
  bool pending_config_change_ = false;

  // ========== Cluster Config ==========
  ClusterConfig cluster_config_;

  // ========== Tick-driven Timer State ==========
  // These deadlines are evaluated by OnStoreTick() (driven by RaftStore's
  // shared tick for multi-raft, or by RaftNodeImpl's own tick for legacy).
  bool election_timer_enabled_ = false;
  std::chrono::steady_clock::time_point election_deadline_;
  bool heartbeat_timer_enabled_ = false;
  std::chrono::steady_clock::time_point heartbeat_deadline_;
  bool snapshot_check_timer_enabled_ = false;
  std::chrono::steady_clock::time_point snapshot_check_deadline_;

  // ========== Snapshot State ==========
  Index last_snapshot_index_ = 0;
  // Term of the log entry at last_snapshot_index_. Required to answer
  // prev_log_term checks exactly at the snapshot boundary; without it both
  // sides answer 0 for compacted indices and mismatched boundaries falsely
  // "match".
  Term last_snapshot_term_ = 0;
  std::unordered_map<NodeId, SnapshotSendState> snapshot_sends_;
  std::string snapshot_temp_path_;
  // In-progress snapshot receive. Continuation chunks must match this
  // (index, term) pair and arrive at the expected offset; anything else
  // means two transfers interleaved (e.g. leader change mid-send) and is
  // rejected so they can never mix into the same temp file.
  Index snapshot_recv_index_ = 0;
  Term snapshot_recv_term_ = 0;
  uint64_t snapshot_recv_expected_offset_ = 0;
  // True while a completed transfer's temp file is being restored/persisted
  // (unlocked window). New offset-0 chunks are dropped during this window —
  // a retransmission would truncate the very file being read.
  bool snapshot_restore_in_progress_ = false;

  // ========== Pending Proposals ==========
  std::unordered_map<uint64_t, PendingProposal> pending_proposals_;

  // ========== Async Apply Thread ==========
  std::thread apply_thread_;
  std::deque<ApplyTask> apply_queue_;
  std::mutex apply_queue_mtx_;
  std::condition_variable apply_queue_cv_;
  std::atomic<bool> apply_running_{false};
  Index last_enqueued_ = 0;

  // ========== Pending ReadIndex Requests ==========
  std::unordered_map<uint64_t, PendingReadIndex> pending_reads_;
  uint64_t next_read_id_ = 1;

  // Pre-built label map to avoid repeated heap allocations on the hot path.
  std::map<std::string, std::string> metrics_node_label_;

  // ========== Callbacks ==========
  std::function<void(RaftNodeRole, uint64_t)> role_change_callback_;
  std::function<void(NodeId, std::string)> leader_change_callback_;

  // ========== Thread Synchronization ==========
  // Lock hierarchy (strict left-to-right):
  //   election_mtx_ -> replication_mtx_ -> snapshot_mtx_ ->
  //   membership_mtx_ -> applier_mtx_
  mutable std::mutex election_mtx_;
  mutable std::mutex replication_mtx_;
  mutable std::mutex snapshot_mtx_;
  mutable std::shared_mutex membership_mtx_;
  mutable std::mutex applier_mtx_;
};

}  // namespace rollingraft
