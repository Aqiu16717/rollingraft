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

class NetworkTransport;
class TimerService;
class Persister;
class Protocol;

/**
 * Cluster configuration containing current node set.
 *
 * Thread-safe for read operations. Configuration changes are
 * propagated through the Raft log.
 */
struct ClusterConfig {
  std::vector<NodeId> nodes;  // Current cluster node IDs
  uint64_t version = 0;       // Config version, incremented on each change

  /**
   * Check if a node ID is in the cluster.
   * @param id Node ID to check
   * @return true if node is in the cluster
   */
  bool Contains(NodeId id) const {
    for (NodeId node : nodes) {
      if (node == id) return true;
    }
    return false;
  }

  /**
   * Get the majority size for the current cluster.
   * @return Number of nodes needed for quorum (nodes/2 + 1)
   */
  int GetMajority() const { return static_cast<int>(nodes.size()) / 2 + 1; }
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

  // Factory functions for dependency injection (testing)
  std::function<std::unique_ptr<NetworkTransport>()> network_factory = nullptr;
  std::function<std::unique_ptr<TimerService>()> timer_factory = nullptr;
  std::function<std::unique_ptr<Persister>()> persister_factory = nullptr;
  std::function<std::unique_ptr<Protocol>()> protocol_factory = nullptr;

  // Metrics configuration
  bool metrics_enabled = false;
  std::string metrics_addr;  // e.g., "0.0.0.0:9001", empty = disabled
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

 private:
  class RaftNodeImpl;
  std::unique_ptr<RaftNodeImpl> raft_node_impl_;  // PIMPL idiom
};

}  // namespace rollingraft
