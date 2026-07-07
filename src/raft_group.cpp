/**
 * @file raft_group.cpp
 * @brief RaftGroup group-local state initialization
 */

#include "raft_group.h"

#include <stdexcept>

#include "rollingraft/logger.h"

namespace rollingraft {

namespace {

// Extract node id from address string in "host:port" format.
// Returns -1 if the port cannot be parsed as a non-negative integer.
NodeId ExtractNodeIdFromAddr(const NodeAddr& addr) {
  size_t colon_pos = addr.rfind(':');
  if (colon_pos == std::string::npos || colon_pos == addr.size() - 1) {
    return -1;
  }
  std::string port_str = addr.substr(colon_pos + 1);
  try {
    size_t idx = 0;
    int port = std::stoi(port_str, &idx);
    if (idx != port_str.size() || port < 0) {
      return -1;
    }
    return port;
  } catch (const std::exception&) {
    return -1;
  }
}

}  // namespace

RaftGroup::RaftGroup(uint64_t group_id, const RaftNodeConfig& config,
                     std::shared_ptr<StateMachine> state_machine)
    : group_id_(group_id),
      config_(config),
      state_machine_(std::move(state_machine)),
      server_id_(config.node_id),
      peer_addrs_(config.peers) {
  if (!state_machine_) {
    throw std::invalid_argument("StateMachine cannot be null");
  }

  metrics_node_label_ = {{"node_id", std::to_string(server_id_)},
                         {"group_id", std::to_string(group_id_)}};

  // Build peer map
  bool has_explicit_peer_ids = !config.peer_node_ids.empty();
  if (has_explicit_peer_ids && config.peer_node_ids.size() != peer_addrs_.size()) {
    throw std::invalid_argument("peer_node_ids size must match peers size");
  }
  for (size_t i = 0; i < peer_addrs_.size(); ++i) {
    NodeId peer_id = has_explicit_peer_ids ? config.peer_node_ids[i] : ParseNodeId(peer_addrs_[i]);
    if (peer_id < 0) {
      throw std::invalid_argument("Cannot determine peer_id for address: " + peer_addrs_[i]);
    }
    peer_map_[peer_id] = peer_addrs_[i];
  }

  check_quorum_enabled_ = config.check_quorum_enabled;
  pre_vote_enabled_ = config.pre_vote_enabled;

  // Initialize client session manager
  session_manager_ = std::make_unique<ClientSessionManager>();

  // Initialize cluster config from peers
  cluster_config_.nodes.push_back(server_id_);
  for (size_t i = 0; i < peer_addrs_.size(); ++i) {
    NodeId peer_id = has_explicit_peer_ids ? config.peer_node_ids[i] : ParseNodeId(peer_addrs_[i]);
    if (peer_id >= 0) {
      cluster_config_.nodes.push_back(peer_id);
    }
  }
  cluster_config_.version = 1;
}

NodeId RaftGroup::ParseNodeId(const NodeAddr& addr) { return ExtractNodeIdFromAddr(addr); }

}  // namespace rollingraft
