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
enum RaftNodeRole { FOLLOWER = 0, CANDIDATE = 1, LEADER = 2, RaftNodeRoleEnd };

class NetworkTransport;
class TimerService;
class Persister;
class Protocol;

struct RaftNodeConfig {
  NodeId node_id;
  std::string listen_addr;
  std::vector<std::string> peers;
  std::string data_dir;

  uint32_t election_timeout_ms = 300;
  uint32_t heartbeat_interval_ms = 100;
  uint32_t max_entries_per_append = 100;
  uint32_t snapshot_threshold = 10000;
  uint32_t rpc_timeout_ms = 1000;

  std::function<std::unique_ptr<NetworkTransport>()> network_factory = nullptr;
  std::function<std::unique_ptr<TimerService>()> timer_factory = nullptr;
  std::function<std::unique_ptr<Persister>()> persister_factory = nullptr;
  std::function<std::unique_ptr<Protocol>()> protocol_factory = nullptr;
};

inline const char* RaftNodeRoleToString(RaftNodeRole role) {
  constexpr static const char* role_str[RaftNodeRoleEnd] = {
      "Follower", "Candidate", "Leader"};
  assert(role >= FOLLOWER && role < RaftNodeRoleEnd);
  return role_str[role];
}

class RaftNode {
 public:
  RaftNode(const RaftNodeConfig& config, std::shared_ptr<StateMachine> sm);
  ~RaftNode();

  // copy is not allowed
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  // move is not allowed
  RaftNode(RaftNode&&) = delete;
  RaftNode& operator=(RaftNode&&) = delete;

  Status Start();
  Status Stop();

  bool IsLeader() const;
  RaftNodeRole GetRole() const;
  Term CurrentTerm() const;
  NodeAddr GetLeaderAddr() const;

 private:
  class RaftNodeImpl;
  std::unique_ptr<RaftNodeImpl> raft_node_impl_;
};

}  // namespace rollingraft
