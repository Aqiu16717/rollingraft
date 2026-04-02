#pragma once

#include <cstdint>

#include "rollingraft/raft_log.h"
#include "rollingraft/types.h"

namespace rollingraft {

enum class RaftMessageType : int8_t {
  KInvalid = -1,
  KRequestVoteRequest = 0,
  KRequestVoteResponse = 1,
  KAppendEntriesRequest = 2,
  KAppendEntriesResponse = 3,
  KInstallSnapshotRequest = 4,
  KInstallSnapshotResponse = 5,
  KClientRequest = 6,
  KClientResponse = 7
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
  NodeId candidate_id_;
  // index of candidate’s last log entry (§5.4)
  uint32_t last_log_index_;
  // term of candidate’s last log entry (§5.4)
  uint32_t last_log_term_;

  RequestVoteRequest()
      : RaftRequest(RaftMessageType::KRequestVoteRequest) {}
  RequestVoteRequest(Term term, NodeId candidate_id, Index last_log_index,
                     Term last_log_term)
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
  Term term_;
  // so follower can redirect clients
  NodeId leader_id_;
  // index of log entry immediately preceding new ones
  Index prev_log_index_;
  // term of prevLogIndex entry
  Term prev_log_term_;
  // log entries to store (empty for heartbeat;
  // may send more than one for efficiency)
  std::vector<RaftLogEntry> entries_;
  // leader's commitIndex
  Index leader_commit_;

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

struct AppendEntriesResponse : public RaftResponse {
  // currentTerm, for leader to update itself
  Term term_;
  // true if follower contained entry matching prevLogIndex and prevLogTerm
  bool success_;
  // index of conflicting entry (for log backtracking optimization)
  Index conflict_index_;
  // number of entries successfully replicated
  Index entries_count_;

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
 * Invoked by leader to send chunks of a snapshot to a follower.
 * Leaders always send chunks in order.
 */
struct InstallSnapshotRequest : public RaftRequest {
  // leader's term
  Term term_;
  // so follower can redirect clients
  NodeId leader_id_;
  // the snapshot replaces all entries up through
  // and including this index
  Index last_included_index_;
  // term of lastIncludedIndex
  Term last_included_term_;
  // byte offset where chunk is positioned in the snapshot file
  uint32_t offset_;
  // raw bytes of the snapshot chunk, starting at offset
  std::vector<char> data_;
  // true if this is the last chunk
  bool done_;

  InstallSnapshotRequest() : RaftRequest(RaftMessageType::KInstallSnapshotRequest) {}
  InstallSnapshotRequest(Term term, NodeId leader_id, Index last_included_index,
                         Term last_included_term, uint32_t offset,
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

  InstallSnapshotResponse()
      : RaftResponse(RaftMessageType::KInstallSnapshotResponse), term_(0) {}
  InstallSnapshotResponse(uint32_t term)
      : RaftResponse(RaftMessageType::KInstallSnapshotResponse), term_(term) {}
};

struct ClientRequest : public RaftRequest {
  std::string command;
  uint64_t client_id;
  uint64_t seq;
  bool read_only = false;

  ClientRequest()
      : RaftRequest(RaftMessageType::KClientRequest),
        client_id(0),
        seq(0),
        read_only(false) {}
};

struct ClientResponse : public RaftResponse {
  bool success;
  std::string response;
  std::string error;
  Index last_applied_index;
  NodeId leader_id;
  std::string leader_addr;

  ClientResponse()
      : RaftResponse(RaftMessageType::KClientResponse),
        success(false),
        last_applied_index(0),
        leader_id(-1) {}
};

Status RpcCall(const std::string& addr, const ClientRequest& req,
               ClientResponse& resp);

}  // namespace rollingraft
