/**
 * @file rpc.h
 * @brief Raft RPC message definitions
 *
 * Defines all RPC message types for Raft protocol:
 * - RequestVote: Candidate election requests
 * - AppendEntries: Log replication and heartbeats
 * - InstallSnapshot: Snapshot transfer for log compaction
 * - ClientRequest/Response: Client interaction
 * - ConfigChange: Dynamic membership changes
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rollingraft/raft_log.h"
#include "rollingraft/types.h"

namespace rollingraft {

/** RPC message type identifiers. */
enum class RaftMessageType : int8_t {
  KInvalid = -1,
  KRequestVoteRequest = 0,
  KRequestVoteResponse = 1,
  KAppendEntriesRequest = 2,
  KAppendEntriesResponse = 3,
  KInstallSnapshotRequest = 4,
  KInstallSnapshotResponse = 5,
  KClientRequest = 6,
  KClientResponse = 7,
  KConfigChangeRequest = 8,
  KConfigChangeResponse = 9
};

/** Base class for all Raft requests. */
struct RaftRequest {
  RaftMessageType type_;
  uint64_t correlation_id_ = 0;  // Unique ID for request-response matching

  RaftRequest() = delete;
  explicit RaftRequest(RaftMessageType type) : type_(type) {}
  virtual ~RaftRequest() = default;
};

/** Base class for all Raft responses. */
struct RaftResponse {
  RaftMessageType type_;
  uint64_t correlation_id_ = 0;  // Mirrors the request's correlation ID

  RaftResponse() = default;
  explicit RaftResponse(RaftMessageType type) : type_(type) {}
  virtual ~RaftResponse() = default;
};

/**
 * RequestVote RPC request.
 *
 * Invoked by candidates to gather votes during elections (Section 5.2).
 */
struct RequestVoteRequest : public RaftRequest {
  Term term_;             // Candidate's term
  NodeId candidate_id_;   // Candidate requesting vote
  Index last_log_index_;  // Index of candidate's last log entry (Section 5.4)
  Term last_log_term_;    // Term of candidate's last log entry (Section 5.4)

  RequestVoteRequest() : RaftRequest(RaftMessageType::KRequestVoteRequest) {}
  RequestVoteRequest(Term term, NodeId candidate_id, Index last_log_index,
                     Term last_log_term)
      : RaftRequest(RaftMessageType::KRequestVoteRequest),
        term_(term),
        candidate_id_(candidate_id),
        last_log_index_(last_log_index),
        last_log_term_(last_log_term) {}
};

/**
 * RequestVote RPC response.
 *
 * Returned by followers to candidates.
 */
struct RequestVoteResponse : RaftResponse {
  Term term_;          // Current term, for candidate to update itself
  bool vote_granted_;  // True means candidate received vote

  RequestVoteResponse()
      : RaftResponse(RaftMessageType::KRequestVoteResponse),
        term_(0),
        vote_granted_(false) {}
  RequestVoteResponse(Term term, bool vote_granted)
      : RaftResponse(RaftMessageType::KRequestVoteResponse),
        term_(term),
        vote_granted_(vote_granted) {}
};

/**
 * AppendEntries RPC request.
 *
 * Invoked by leaders to replicate log entries and send heartbeats
 * (Section 5.3).
 */
struct AppendEntriesRequest : public RaftRequest {
  Term term_;             // Leader's term
  NodeId leader_id_;      // So follower can redirect clients
  Index prev_log_index_;  // Index of log entry immediately preceding new ones
  Term prev_log_term_;    // Term of prev_log_index entry
  std::vector<RaftLogEntry>
      entries_;          // Log entries to store (empty for heartbeat)
  Index leader_commit_;  // Leader's commit index

  AppendEntriesRequest()
      : RaftRequest(RaftMessageType::KAppendEntriesRequest) {}
  AppendEntriesRequest(Term term, NodeId leader_id, Index prev_log_index,
                       Term prev_log_term, std::vector<RaftLogEntry> entries,
                       Index leader_commit)
      : RaftRequest(RaftMessageType::KAppendEntriesRequest),
        term_(term),
        leader_id_(leader_id),
        prev_log_index_(prev_log_index),
        prev_log_term_(prev_log_term),
        entries_(std::move(entries)),
        leader_commit_(leader_commit) {}
};

/**
 * AppendEntries RPC response.
 *
 * Returned by followers to leaders.
 */
struct AppendEntriesResponse : public RaftResponse {
  Term term_;             // Current term, for leader to update itself
  bool success_;          // True if follower contained matching prev log entry
  Index conflict_index_;  // Index of conflicting entry (optimization)
  Index entries_count_;   // Number of entries successfully replicated

  AppendEntriesResponse()
      : RaftResponse(RaftMessageType::KAppendEntriesResponse),
        term_(0),
        success_(false),
        conflict_index_(0),
        entries_count_(0) {}
  AppendEntriesResponse(Term term, bool success, Index conflict_index = 0,
                        Index entries_count = 0)
      : RaftResponse(RaftMessageType::KAppendEntriesResponse),
        term_(term),
        success_(success),
        conflict_index_(conflict_index),
        entries_count_(entries_count) {}
};

/**
 * InstallSnapshot RPC request.
 *
 * Invoked by leader to send chunks of a snapshot to a follower.
 * Leaders always send chunks in order.
 */
struct InstallSnapshotRequest : public RaftRequest {
  Term term_;                  // Leader's term
  NodeId leader_id_;           // So follower can redirect clients
  Index last_included_index_;  // Snapshot replaces entries up to this index
  Term last_included_term_;    // Term of last_included_index
  uint32_t offset_;            // Byte offset in snapshot file
  std::vector<char> data_;     // Raw bytes of snapshot chunk
  bool done_;                  // True if this is the last chunk

  InstallSnapshotRequest()
      : RaftRequest(RaftMessageType::KInstallSnapshotRequest) {}
  InstallSnapshotRequest(Term term, NodeId leader_id, Index last_included_index,
                         Term last_included_term, uint32_t offset,
                         std::vector<char> data, bool done)
      : RaftRequest(RaftMessageType::KInstallSnapshotRequest),
        term_(term),
        leader_id_(leader_id),
        last_included_index_(last_included_index),
        last_included_term_(last_included_term),
        offset_(offset),
        data_(std::move(data)),
        done_(done) {}
};

/**
 * InstallSnapshot RPC response.
 *
 * Returned by followers to leaders.
 */
struct InstallSnapshotResponse : public RaftResponse {
  Term term_;  // Current term, for leader to update itself

  InstallSnapshotResponse()
      : RaftResponse(RaftMessageType::KInstallSnapshotResponse), term_(0) {}
  explicit InstallSnapshotResponse(Term term)
      : RaftResponse(RaftMessageType::KInstallSnapshotResponse), term_(term) {}
};

/**
 * Client request sent to the Raft cluster.
 */
struct ClientRequest : public RaftRequest {
  std::string command;  // Command data for state machine
  uint64_t client_id;   // Unique client identifier
  uint64_t seq;         // Monotonic sequence number for deduplication
  bool read_only;       // True if this is a read-only query

  ClientRequest()
      : RaftRequest(RaftMessageType::KClientRequest),
        client_id(0),
        seq(0),
        read_only(false) {}
};

/**
 * Client response from the Raft cluster.
 */
struct ClientResponse : public RaftResponse {
  bool success;              // Whether the command was applied
  std::string response;      // Response data from state machine
  std::string error;         // Error message if failed
  Index last_applied_index;  // Index at which command was applied
  NodeId leader_id;          // Current leader ID (for redirection)
  std::string leader_addr;   // Current leader address (for redirection)

  ClientResponse()
      : RaftResponse(RaftMessageType::KClientResponse),
        success(false),
        last_applied_index(0),
        leader_id(-1) {}
};

/**
 * Configuration change request.
 *
 * Sent by client to leader to add or remove a cluster node.
 */
struct ConfigChangeRequest : public RaftRequest {
  enum class Type : int8_t { kAddNode = 0, kRemoveNode = 1 };

  Type type_;           // Add or remove
  NodeId node_id_;      // Target node ID
  NodeAddr node_addr_;  // Target node address (for add operations)

  ConfigChangeRequest()
      : RaftRequest(RaftMessageType::KConfigChangeRequest),
        type_(Type::kAddNode),
        node_id_(-1) {}
};

/**
 * Configuration change response.
 */
struct ConfigChangeResponse : public RaftResponse {
  bool success_;       // Whether the change was accepted
  std::string error_;  // Error message if failed

  ConfigChangeResponse()
      : RaftResponse(RaftMessageType::KConfigChangeResponse), success_(false) {}
};

/**
 * Synchronous RPC call helper for client requests.
 *
 * @param addr Server address to contact
 * @param req Request to send
 * @param resp Response to populate
 * @return Status of the network operation
 */
Status RpcCall(const std::string& addr, const ClientRequest& req,
               ClientResponse& resp);

}  // namespace rollingraft
