#pragma once

#include <cstdint>

#include "rollingraft/raft_log.h"
#include "rollingraft/raft_node.h"

namespace rollingraft {

enum class RaftMessageType : int8_t {
  KInvalid = -1,
  KRequestVoteRequest = 0,
  KRequestVoteResponse = 1,
  KAppendEntriesRequest = 2,
  KAppendEntriesResponse = 3,
  KInstallSnapshotRequest = 4,
  KInstallSnapshotResponse = 5
};

struct RaftRequest {
  RaftMessageType type_;

  RaftRequest() = delete;
  explicit RaftRequest(RaftMessageType type) : type_(type) {}
  virtual ~RaftRequest() = default;
};

struct RaftResponse {
  RaftMessageType type_;

  RaftResponse() = default;
  RaftResponse(RaftMessageType type) : type_(type) {}
  virtual ~RaftResponse() = default;
};

/**
 * RequestVote RPCs are initiated by
 * candidates during elections (Section 5.2),
 */
struct RequestVoteRequest : public RaftRequest {
  // candidate's term
  uint32_t term_;
  // candidate requesting vote
  RaftNodeId candidate_id_;
  // index of candidate’s last log entry (§5.4)
  uint32_t last_log_index_;
  // term of candidate’s last log entry (§5.4)
  uint32_t last_log_term_;

  RequestVoteRequest() = delete;
  RequestVoteRequest(uint32_t term, RaftNodeId candidate_id,
                     uint32_t last_log_index, uint32_t last_log_term)
      : RaftRequest(RaftMessageType::KRequestVoteRequest),
        term_(term),
        candidate_id_(candidate_id),
        last_log_index_(last_log_index),
        last_log_term_(last_log_term) {}
};

struct RequestVoteResponse : RaftResponse {
  // currentTerm, for candidate to update itself
  uint32_t term_;
  // true means candidate received vote
  bool vote_granted_;

  RequestVoteResponse() : term_(0), vote_granted_(false) {}
  RequestVoteResponse(uint32_t term, bool vote_granted)
      : RaftResponse(RaftMessageType::KRequestVoteResponse),
        term_(term),
        vote_granted_(vote_granted) {}
};

/**
 * AppendEntries RPCs are initiated by leaders to replicate log en-
 * tries and to provide a form of heartbeat (Section 5.3)
 */
struct AppendEntriesRequest : public RaftRequest {
  // leader's term
  uint32_t term_;
  // so follower can redirect clients
  RaftNodeId leader_id_;
  // index of log entry immediately preceding new ones
  uint32_t prev_log_index_;
  // term of prevLogIndex entry
  uint32_t prev_log_term_;
  // log entries to store (empty for heartbeat;
  // may send more than one for efficiency)
  RaftLog entries_;
  // leader's commitIndex
  uint32_t leader_commit_;

  AppendEntriesRequest() = delete;
  AppendEntriesRequest(uint32_t term, RaftNodeId leader_id,
                       uint32_t prev_log_index_, uint32_t prev_log_term,
                       RaftLog entries, uint32_t leader_commit)
      : RaftRequest(RaftMessageType::KAppendEntriesRequest),
        term_(term),
        leader_id_(leader_id),
        prev_log_index_(prev_log_index_),
        prev_log_term_(prev_log_term),
        entries_(entries),
        leader_commit_(leader_commit) {}
};

struct AppendEntriesResponse : public RaftResponse {
  // currentTerm, for leader to update itself
  uint32_t term_;
  // true if follower contained entry matching prevLogIndex and prevLogTerm
  bool success_;

  AppendEntriesResponse() = delete;
  AppendEntriesResponse(uint32_t term, bool success)
      : RaftResponse(RaftMessageType::KAppendEntriesResponse),
        term_(term),
        success_(success) {}
};

/**
 * Invoked by leader to send chunks of a snapshot to a follower.
 * Leaders always send chunks in order.
 */
struct InstallSnapshotRequest : public RaftRequest {
  // leader's term
  uint32_t term_;
  // so follower can redirect clients
  RaftNodeId leader_id_;
  // the snapshot replaces all entries up through
  // and including this index
  uint32_t last_included_index_;
  // term of lastIncludedIndex
  uint32_t last_included_term_;
  // byte offset where chunk is positioned in the snapshot file
  uint32_t offset_;
  // raw bytes of the snapshot chunk, starting at offset
  std::vector<char> data_;
  // true if this is the last chunk
  bool done_;

  InstallSnapshotRequest() = delete;
  InstallSnapshotRequest(uint32_t term, RaftNodeId leader_id,
                         uint32_t last_included_index,
                         uint32_t last_included_term, uint32_t offset,
                         std::vector<char> data, bool done)
      : RaftRequest(RaftMessageType::KInstallSnapshotRequest),

        term_(term),
        leader_id_(leader_id),
        last_included_index_(last_included_index),
        last_included_term_(last_included_term),
        offset_(offset),
        data_(data),
        done_(done) {}
};

struct InstallSnapshotResponse : public RaftResponse {
  // currentTerm, for leader to update itself
  uint32_t term_;

  InstallSnapshotResponse() = delete;
  InstallSnapshotResponse(uint32_t term)
      : RaftResponse(RaftMessageType::KInstallSnapshotResponse), term_(term) {}
};

struct ClientRequest {
  std::string command;
  uint64_t client_id;
  uint64_t seq;
  bool read_only = false;
};

struct ClientResponse {
  bool success;
  std::string response;
  uint32_t last_applied_index_;
  RaftNodeId leader_id;
  std::string leader_addr;
  Status error_code;
};

Status RpcCall(const std::string& addr, const ClientRequest& req,
               ClientResponse& resp);

}  // namespace rollingraft
