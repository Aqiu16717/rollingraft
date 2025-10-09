#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <vector>

#include "rollingraft/rpc.h"
#include "rollingraft/status.h"

namespace rollingraft {

/**
 * At any given time each server is in one of three states:
 * leader, follower, or candidate.
 * In normal operation there is exactly one leader and all
 * of the other servers are followers.
 * Followers are passive: they issue no requests on
 * their own but simply respond to requests from leaders
 * and candidates.
 * The leader handles all client requests (if a client contacts
 * a follower, the follower redirects it to the leader).
 * The third state, candidate, is used to elect a new leader
 * as described in Section 5.2. Figure 4 shows the states and
 * their transitions; the transitions are discussed below.
 */
enum RaftNodeState {
  FOLLOWER = 0,
  CANDIDATE = 1,
  LEADER = 2,
  RaftNodeStateEnd
};

inline const char* RaftNodeStateToString(RaftNodeState state) {
  constexpr static const char* state_str[RaftNodeStateEnd] = {
      "Follower", "Candidate", "Leader"};
  assert(state >= FOLLOWER && state < RaftNodeStateEnd);
  return state_str[state];
}

class RaftNode {
 public:
  RaftNode() = default;
  RaftNode(uint32_t id, int port, std::vector<uint32_t> peers);
  ~RaftNode();

  Status RequestVote(const RequestVoteRequest&, RequestVoteResponse&);
  Status AppendEntries(const AppendEntriesRequest&, AppendEntriesResponse&);
  Status InstallSnapshot(const InstallSnapshotRequest&,
                         InstallSnapshotResponse&);

  Status BecomeFollower();
  Status BecomeCandidate();
  Status BecomeLeader();

  Status Election();

  inline void SetState(RaftNodeState state);

 private:
  class RaftNodeImpl;
  std::unique_ptr<RaftNodeImpl> raft_node_impl_;
};

}  // namespace rollingraft
