#pragma once

#include "log.h"

namespace rolingraft {

struct RaftRequest {
  virtual ~RaftRequest() = default;
};

struct RaftResponse {
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
  uint32_t candidate_id_;
  // index of candidate’s last log entry (§5.4)
  uint32_t last_log_index_;
  // term of candidate’s last log entry (§5.4)
  uint32_t last_log_term_;
};

struct RequestVoteResponse : RaftResponse {
  // currentTerm, for candidate to update itself
  uint32_t term_;
  // true means candidate received vote
  bool vote_granted_;
};

/**
 * AppendEntries RPCs are initiated by leaders to replicate log en-
 * tries and to provide a form of heartbeat (Section 5.3)
 */
struct AppendEntriesRequest : public RaftRequest {
  // leader's term
  uint32_t term_;
  // so follower can redirect clients
  uint32_t leader_id_;
  // index of log entry immediately preceding new ones
  uint32_t prev_log_index_;
  // term of prevLogIndex entry
  uint32_t prev_log_term_;
  // log entries to store (empty for heartbeat;
  // may send more than one for efficiency)
  Log entries_;
  // leader's commitIndex
  uint32_t leader_commit_;
};

struct AppendEntriesResponse : public RaftResponse {
  // currentTerm, for leader to update itself
  uint32_t term_;
  // true if follower contained entry matching prevLogIndex and prevLogTerm
  bool success_;
};

/**
 * Invoked by leader to send chunks of a snapshot to a follower.
 * Leaders always send chunks in order.
 */
struct InstallSnapshotRequest : public RaftRequest {
  // leader's term
  uint32_t term_;
  // so follower can redirect clients
  uint32_t leader_id_;
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
};

struct InstallSnapshotResponse : public RaftResponse {
  // currentTerm, for leader to update itself
  uint32_t term_;
};

}  // namespace rolingraft
