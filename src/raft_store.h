#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "rollingraft/raft_node.h"
#include "rollingraft/state_machine.h"
#include "rollingraft/status.h"
#include "rollingraft/timer_service.h"
#include "rollingraft/types.h"

#include "raft_node_impl.h"
#include "shared_node_infra.h"

namespace rollingraft {

/**
 * @brief Node-level configuration shared by all groups hosted on this process.
 */
struct RaftStoreConfig {
  NodeId node_id = -1;
  NodeAddr listen_addr;
  std::vector<NodeAddr> peers;
  std::vector<NodeId> peer_node_ids;  // Optional: must match peers.size() if set
  std::string data_dir;

  bool metrics_enabled = false;
  std::string metrics_addr;
  // Admin API authentication token, forwarded to the shared metrics server.
  std::string admin_token;

  bool tls_enabled = false;
  std::string tls_cert_file;
  std::string tls_key_file;
  std::string tls_ca_file;

  // Optional factories for dependency injection (testing).
  std::function<std::unique_ptr<NetworkTransport>()> network_factory = nullptr;
  std::function<std::unique_ptr<TimerService>()> timer_factory = nullptr;
  std::function<std::unique_ptr<Protocol>()> protocol_factory = nullptr;
};

/**
 * @brief Per-group options used when creating a RaftGroup inside a store.
 */
struct RaftGroupOptions {
  uint64_t group_id = 0;
  // Optional per-group overrides; 0 means "use store default".
  uint32_t election_timeout_ms = 0;
  uint32_t heartbeat_interval_ms = 0;
  // Optional per-group peer node IDs. If empty, inherits from RaftStoreConfig.
  std::vector<NodeId> peer_node_ids;
};

/**
 * @brief Multi-raft store that owns shared node infrastructure and a table of
 * per-group Raft state machines.
 *
 * PR-C implements the transport routing layer: inbound RPCs carry a group_id
 * and are dispatched to the matching RaftNodeImpl. group_id == 0 keeps the
 * legacy single-group compatibility path.
 */
class RaftStore {
 public:
  explicit RaftStore(const RaftStoreConfig& config);
  ~RaftStore();

  // Not copyable / movable.
  RaftStore(const RaftStore&) = delete;
  RaftStore& operator=(const RaftStore&) = delete;

  Status Initialize();
  Status Start();
  Status Stop();

  Status CreateGroup(uint64_t group_id, const RaftGroupOptions& options,
                     std::shared_ptr<StateMachine> state_machine);
  Status RemoveGroup(uint64_t group_id);

  RaftNode::RaftNodeImpl* GetGroup(uint64_t group_id) const;
  std::vector<uint64_t> ListGroups() const;

  SharedNodeInfra* GetInfra() const { return infra_.get(); }

  /**
   * Dispatch an inbound RPC to the target group.
   * group_id == 0 is reserved for the legacy single-group path and is not
   * routed through the store table.
   */
  void OnIncomingRpc(NodeId from, uint64_t group_id, const std::string& data,
                     std::string& response);

 private:
  RaftNodeConfig MakeGroupConfig(uint64_t group_id, const RaftGroupOptions& options) const;
  // Wire store-level /v1/status and admin providers on the shared metrics
  // server. Call after infra_->metrics_server_ exists.
  void RegisterStoreProviders();

  RaftStoreConfig config_;
  std::shared_ptr<SharedNodeInfra> infra_;

  std::unordered_map<uint64_t, std::shared_ptr<RaftNode::RaftNodeImpl>> groups_;
  mutable std::shared_mutex groups_mtx_;

  // Shared coarse-grained tick timer.  RaftStore dispatches a tick to every
  // group on each interval, allowing group-local timeouts (election, etc.) to
  // be driven from a single timer instead of one timer per group.
  TimerId tick_timer_ = 0;

  std::atomic<bool> initialized_{false};
  std::atomic<bool> running_{false};
};

}  // namespace rollingraft
