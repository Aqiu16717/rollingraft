/**
 * @file raft_node.h
 * @brief Public API for Raft consensus node
 *
 * RollingRaft is a C++ implementation of the Raft consensus algorithm.
 * This header provides the main RaftNode class for building distributed
 * systems with strong consistency guarantees.
 *
 * @see https://raft.github.io/ for Raft algorithm details
 */

#pragma once

#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "rollingraft/state_machine.h"
#include "rollingraft/status.h"
#include "rollingraft/types.h"

namespace rollingraft {

/**
 * Raft node role states.
 *
 * At any given time each server is in one of three states:
 * - Follower: passive, responds to requests from leaders and candidates
 * - Candidate: initiates elections when timeout occurs
 * - Leader: handles all client requests, replicates log entries
 *
 * In normal operation there is exactly one leader and all other
 * servers are followers.
 */
enum RaftNodeRole { FOLLOWER = 0, CANDIDATE = 1, LEADER = 2, RaftNodeRoleEnd };

class EventBus;
class NetworkTransport;
class TimerService;
class Persister;
class Protocol;

/**
 * Cluster configuration containing current node set.
 *
 * Thread-safe for read operations. Configuration changes are
 * propagated through the Raft log.
 *
 * Supports joint consensus for safe membership changes:
 * - Normal mode: single configuration (nodes)
 * - Joint mode: transitional configuration (old_nodes + nodes)
 */
struct ClusterConfig {
  std::vector<NodeId> nodes;      // Current/new cluster node IDs (Cnew)
  std::vector<NodeId> old_nodes;  // Previous cluster node IDs (Cold) when in joint
  std::vector<NodeId> learners;   // Non-voting learners receiving replication
  uint64_t version = 0;           // Config version, incremented on each change
  bool is_joint = false;          // True when in joint consensus transition

  /**
   * Check if a node ID is in the cluster (voters or learners).
   * In joint mode, checks both old and new configurations.
   * @param id Node ID to check
   * @return true if node is in the cluster
   */
  bool Contains(NodeId id) const {
    if (IsMember(id, nodes)) return true;
    if (is_joint && IsMember(id, old_nodes)) return true;
    if (IsMember(id, learners)) return true;
    return false;
  }

  /**
   * Check if a node ID is a voting member.
   * In joint mode, any node in old or new can vote.
   * Learners are NOT voters.
   * @param id Node ID to check
   * @return true if node can vote
   */
  bool IsVoter(NodeId id) const {
    if (IsMember(id, nodes)) return true;
    if (is_joint && IsMember(id, old_nodes)) return true;
    return false;
  }

  /**
   * Check if a node ID is a learner.
   * @param id Node ID to check
   * @return true if node is a learner
   */
  bool IsLearner(NodeId id) const { return IsMember(id, learners); }

  /**
   * Get the majority size for the current (new) configuration.
   * @return Number of nodes needed for quorum (nodes/2 + 1)
   */
  uint32_t GetMajority() const {
    return static_cast<uint32_t>(nodes.size()) / 2 + 1;
  }

  /**
   * Get the majority size for the old configuration (joint mode only).
   * @return Number of nodes needed for old quorum
   */
  uint32_t GetOldMajority() const {
    return static_cast<uint32_t>(old_nodes.size()) / 2 + 1;
  }

  /**
   * Check if both old and new majorities are satisfied.
   * Used for commit calculation in joint consensus mode.
   * @param old_count Votes/acks in old configuration
   * @param new_count Votes/acks in new configuration
   * @return true if both majorities are met
   */
  bool JointMajoritySatisfied(int old_count, int new_count) const {
    if (!is_joint) return new_count >= static_cast<int>(GetMajority());
    return old_count >= static_cast<int>(GetOldMajority()) &&
           new_count >= static_cast<int>(GetMajority());
  }

 private:
  static bool IsMember(NodeId id, const std::vector<NodeId>& list) {
    for (NodeId node : list) {
      if (node == id) return true;
    }
    return false;
  }
};

/**
 * Configuration for creating a RaftNode.
 *
 * All fields have sensible defaults except node_id, listen_addr,
 * peers, and data_dir which must be explicitly set.
 */
struct RaftNodeConfig {
  NodeId node_id;                  // Unique node identifier
  std::string listen_addr;         // Address to listen on, e.g., "0.0.0.0:8001"
  std::vector<std::string> peers;  // Addresses of peer nodes
  std::vector<NodeId> peer_node_ids;  // Optional: explicit node IDs for peers (must match peers.size() if set)
  std::string data_dir;            // Directory for persistent storage

  // Timing parameters
  uint32_t election_timeout_ms =
      300;  // Base election timeout (randomized 1x-2x)
  uint32_t heartbeat_interval_ms =
      50;  // Leader heartbeat interval (faster for quick replication)
  uint32_t max_entries_per_append = 100;  // Max entries per AppendEntries RPC

  // Auto-snapshot configuration
  uint32_t snapshot_threshold_entries =
      10000;  // Entries since last snapshot to trigger
  uint32_t snapshot_threshold_bytes =
      10 * 1024 * 1024;  // Bytes since last snapshot to trigger (10MB)
  uint32_t snapshot_check_interval_ms =
      5000;  // How often to check (leader only)

  uint32_t rpc_timeout_ms =
      500;  // RPC call timeout (shorter for faster fail detection)
  uint32_t max_retry_attempts = 5;    // Max retry attempts for AppendEntries
  uint32_t base_retry_delay_ms = 10;  // Base delay for exponential backoff
  uint32_t max_retry_delay_ms = 500;  // Max retry delay

  // Log compaction retention: number of entries to keep preceding the
  // snapshot index. 0 = delete everything covered by snapshot.
  uint32_t log_retention_entries = 0;

  // Propose timeout: how long to wait for a proposal to commit (ms)
  uint32_t propose_timeout_ms = 5000;
  // Max snapshot size in bytes. 0 = unlimited. Default 100MB.
  uint32_t max_snapshot_size_bytes = 100 * 1024 * 1024;

  // Graceful shutdown timeout (ms). 0 = wait indefinitely. Default 30s.
  uint32_t shutdown_timeout_ms = 30000;

  // Leader lease read optimization: allow local reads without heartbeat
  // broadcast when lease is valid (based on majority voter acks).
  bool leader_lease_enabled = true;

  // Factory functions for dependency injection (testing)
  std::function<std::unique_ptr<NetworkTransport>()> network_factory = nullptr;
  std::function<std::unique_ptr<TimerService>()> timer_factory = nullptr;
  std::function<std::unique_ptr<Persister>()> persister_factory = nullptr;
  std::function<std::unique_ptr<Protocol>()> protocol_factory = nullptr;

  // Metrics configuration
  bool metrics_enabled = false;
  std::string metrics_addr;  // e.g., "0.0.0.0:9001", empty = disabled
  // TLS configuration for control plane (metrics HTTP server)
  bool tls_enabled = false;
  std::string tls_cert_file;
  std::string tls_key_file;
  std::string tls_ca_file;

  // Admin API authentication token. If non-empty, admin endpoints
  // (/v1/members, /v1/snapshot/*, /v1/leadership/*, /v1/config PATCH)
  // require Authorization: Bearer <token>. Metrics/health endpoints remain
  // public. Empty = no authentication (backward compatible).
  std::string admin_token;

  // Logging configuration
  bool json_logging = false;  // Enable JSON structured logging format

  // CheckQuorum: leader steps down if it hasn't received quorum acks.
  // Should be disabled in deterministic tests that use simulated clocks.
  bool check_quorum_enabled = true;

  // Pre-vote: ask peers before becoming candidate to prevent term inflation.
  // Should be disabled in deterministic tests that rely on exact timing.
  bool pre_vote_enabled = true;

  // Dead node auto-removal: if a follower hasn't responded for
  // dead_node_timeout_ms, leader automatically proposes RemoveNode.
  bool auto_remove_dead_nodes = false;
  uint32_t dead_node_timeout_ms = 600;  // Default = election_timeout_ms * 2

  // Transport write coalescing (batching): when enabled, multiple outbound
  // messages to the same peer are concatenated into a single async_write.
  // Disabling may improve stability under extreme concurrency at the cost of
  // slightly higher syscall overhead. Default = enabled.
  bool transport_batching_enabled = true;

  /**
   * Validate configuration parameters.
   *
   * Performs fail-fast checks on all fields including:
   * - Required fields (listen_addr, data_dir) are non-empty
   * - Address format is host:port with valid port range (1-65535)
   * - Election timeout > heartbeat interval
   * - Peer node IDs match peer addresses count (if provided)
   * - TLS consistency (cert/key files exist when TLS enabled)
   *
   * @return Status::OK() if valid, error with details otherwise
   */
  Status Validate() const;
};

/**
 * Convert RaftNodeRole to human-readable string.
 * @param role The role to convert
 * @return String representation ("Follower", "Candidate", or "Leader")
 */
inline const char* RaftNodeRoleToString(RaftNodeRole role) {
  constexpr static const char* role_str[RaftNodeRoleEnd] = {
      "Follower", "Candidate", "Leader"};
  assert(role >= FOLLOWER && role < RaftNodeRoleEnd);
  return role_str[role];
}

/**
 * Main Raft consensus node.
 *
 * Manages the Raft state machine including leader election, log replication,
 * snapshot management, and dynamic membership changes.
 *
 * Thread-safety: All public methods are thread-safe.
 * Lifecycle: Create -> Configure -> Start -> [Use] -> Stop.
 */
class RaftNode {
 public:
  /**
   * Create a new Raft node.
   *
   * @param config Node configuration (ID, peers, paths)
   * @param sm User state machine for applying committed commands
   */
  RaftNode(const RaftNodeConfig& config, std::shared_ptr<StateMachine> sm);

  ~RaftNode();

  // Non-copyable, non-movable
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;
  RaftNode(RaftNode&&) = delete;
  RaftNode& operator=(RaftNode&&) = delete;

  /**
   * Start the node and join the cluster.
   *
   * Initializes network transport, loads persistent state,
   * and begins participating in the Raft protocol.
   *
   * @return Status::OK() on success, error otherwise
   * @note Set callbacks before calling Start()
   */
  Status Start();

  /**
   * Gracefully stop the node.
   *
   * Stops accepting new requests, flushes pending operations,
   * and closes network connections.
   *
   * @return Status::OK() on success
   */
  Status Stop();

  /**
   * Check if this node is the current leader.
   * @return true if node is leader and can accept proposals
   */
  bool IsLeader() const;

  /**
   * Set callback for role state changes.
   *
   * Called when the node transitions between follower/candidate/leader.
   * Must be called before Start().
   *
   * @param callback Function called on role change (role, term)
   */
  void SetRoleChangeCallback(
      std::function<void(RaftNodeRole role, Term term)> callback);

  /**
   * Set callback for leader changes.
   *
   * Called when a new leader is detected (even on followers).
   * Must be called before Start().
   *
   * @param callback Function called on leader change (leader_id, leader_addr)
   */
  void SetLeaderChangeCallback(
      std::function<void(NodeId leader_id, const NodeAddr& addr)> callback);

  /**
   * Propose a command for cluster-wide replication.
   *
   * Only the leader can propose commands. The command will be
   * replicated to a majority of nodes before being applied to
   * the state machine.
   *
   * @param command Opaque command data for state machine
   * @param callback Invoked when command is applied or fails
   * @return Status::OK() if proposal was accepted (not yet applied)
   * @note Callback is called asynchronously from a different thread
   */
  Status Propose(const std::string& command,
                 std::function<void(const ApplyResult& result)> callback);

  /**
   * Propose a batch of commands for cluster-wide replication.
   *
   * All commands in the batch are appended to the log atomically
   * under the same term. They are replicated and applied as a group.
   * The callback is invoked once all commands have been applied.
   *
   * Only the leader can propose commands.
   *
   * @param commands List of opaque command data for state machine
   * @param callback Invoked when all commands are applied or batch fails
   * @return Status::OK() if batch was accepted (not yet applied)
   * @note Callback is called asynchronously from a different thread
   */
  Status ProposeBatch(
      const std::vector<std::string>& commands,
      std::function<void(const std::vector<ApplyResult>& results)> callback);

  /**
   * Perform a linearizable read.
   *
   * Ensures the node is still the leader by exchanging heartbeats
   * with a majority of the cluster. Callback is invoked when it's
   * safe to read from the state machine.
   *
   * Only the leader can perform linearizable reads.
   *
   * @param callback Invoked when read is safe to proceed
   * @return Status::OK() if read was initiated
   */
  Status ReadIndex(std::function<void()> callback);

  /**
   * Add a new node to the cluster.
   *
   * Only the leader can add nodes. The change is replicated as
   * a configuration change log entry. One node at a time.
   *
   * @param id Node ID to add
   * @param addr Network address of the new node
   * @return Status::OK() if configuration change was proposed
   */
  Status AddNode(NodeId id, const NodeAddr& addr);

  /**
   * Add a learner node to the cluster.
   * Learners receive log replication but do not vote or count towards quorum.
   * Must be called on the leader.
   *
   * @param id Unique node identifier
   * @param addr Network address of the new learner
   * @return Status::OK() if configuration change was proposed
   */
  Status AddLearner(NodeId id, const NodeAddr& addr);

  /**
   * Promote a learner to a voting member.
   * Uses joint consensus for safety since promotion changes quorum.
   * Must be called on the leader.
   *
   * @param id Node identifier of the learner to promote
   * @return Status::OK() if configuration change was proposed
   */
  Status PromoteLearner(NodeId id);

  /**
   * Remove a node from the cluster.
   *
   * Only the leader can remove nodes. The change is replicated as
   * a configuration change log entry. One node at a time.
   *
   * Cannot remove the leader itself.
   *
   * @param id Node ID to remove
   * @return Status::OK() if configuration change was proposed
   */
  Status RemoveNode(NodeId id);

  /**
   * Get current cluster configuration.
   * @return Current configuration including all nodes
   */
  ClusterConfig GetConfig() const;

  /**
   * Get current role (follower/candidate/leader).
   * @return Current Raft role
   */
  RaftNodeRole GetRole() const;

  /**
   * Get current Raft term.
   * @return Current term number
   */
  Term CurrentTerm() const;

  /**
   * Get current leader's address.
   * @return Leader address if known, empty string otherwise
   */
  NodeAddr GetLeaderAddr() const;

  /**
   * Get the current commit index.
   * @return Last committed log index
   */
  Index GetCommitIndex() const;

  /**
   * Get the event bus for subscribing to Raft events.
   * @return Reference to the node's EventBus
   */
  EventBus& GetEventBus();

  /**
   * Trigger a manual snapshot (only valid for leader).
   * @return Status::OK() if snapshot was triggered
   */
  Status TriggerSnapshot();

  /**
   * Transfer leadership to another node (simple version: leader steps down).
   * @param target_id Node ID to transfer leadership to
   * @return Status::OK() if transfer was initiated
   */
  Status TransferLeadershipTo(NodeId target_id);

 private:
  class RaftNodeImpl;
  std::unique_ptr<RaftNodeImpl> raft_node_impl_;  // PIMPL idiom
};

}  // namespace rollingraft
