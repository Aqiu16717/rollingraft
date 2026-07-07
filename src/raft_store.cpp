/**
 * @file raft_store.cpp
 * @brief Multi-raft store implementation
 */

#include "raft_store.h"

#include <filesystem>
#include <stdexcept>

#include "rollingraft/logger.h"
#include "rollingraft/tls_config.h"

#include "asio_timer_service.h"
#include "json_protocol.h"
#include "multi_raft_persister.h"
#include "nlohmann/json.hpp"

namespace rollingraft {

// Forward declarations for the default transport factories (defined in
// asio_network_transport.cpp).
std::unique_ptr<NetworkTransport> CreateDefaultNetworkTransport();
std::unique_ptr<NetworkTransport> CreateAsioNetworkTransport(const TlsConfig& tls_config);

RaftStore::RaftStore(const RaftStoreConfig& config) : config_(config) {
  if (config.node_id < 0) {
    throw std::invalid_argument("RaftStoreConfig node_id must be non-negative");
  }
  if (config.listen_addr.empty()) {
    throw std::invalid_argument("RaftStoreConfig listen_addr cannot be empty");
  }
}

RaftStore::~RaftStore() {
  if (running_.load(std::memory_order_acquire)) {
    auto status = Stop();
    if (!status.ok()) {
      LOG_WARN("RaftStore stop failed during destruction: {}", status.ToString());
    }
  }
}

Status RaftStore::Initialize() {
  bool expected = false;
  if (!initialized_.compare_exchange_strong(expected, true)) {
    return Status::Error("Already initialized");
  }

  infra_ = std::make_shared<SharedNodeInfra>();
  infra_->network_ =
      config_.network_factory
          ? config_.network_factory()
          : (config_.tls_enabled
                 ? CreateAsioNetworkTransport(TlsConfig{.enabled = true,
                                                        .cert_file = config_.tls_cert_file,
                                                        .key_file = config_.tls_key_file,
                                                        .ca_file = config_.tls_ca_file})
                 : CreateDefaultNetworkTransport());
  infra_->timer_ = config_.timer_factory ? config_.timer_factory() : TimerService::CreateDefault();
  infra_->protocol_ =
      config_.protocol_factory ? config_.protocol_factory() : std::make_unique<JsonProtocol>();

  if (!infra_->network_) {
    return Status::Error("NetworkTransport cannot be null");
  }
  if (!infra_->timer_) {
    return Status::Error("TimerService cannot be null");
  }
  if (!infra_->protocol_) {
    return Status::Error("Protocol cannot be null");
  }

  if (config_.metrics_enabled) {
    infra_->metrics_ = std::make_unique<MetricsRegistry>();
  }

  RuntimeConfig::Values defaults;
  infra_->runtime_config_ = std::make_unique<RuntimeConfig>(defaults);

  return Status::OK();
}

Status RaftStore::Start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return Status::Error("Already started or stopped");
  }

  if (!initialized_) {
    return Status::Error("Not initialized");
  }

  // Register the store's group router as the multi-group handler.
  auto handler = [this](NodeId from, uint64_t group_id, const std::string& data,
                        std::string& response) { OnIncomingRpc(from, group_id, data, response); };
  infra_->network_->SetGroupRequestHandler(handler);

  // The legacy single-group handler is not used by the store; each group
  // handles its own RPCs after routing.
  auto legacy_handler = [](NodeId /*from*/, const std::string& /*data*/, std::string& response) {
    nlohmann::json err;
    err["error"] = "LEGACY_PATH_NOT_SUPPORTED";
    err["message"] = "Use group_id > 0 for multi-raft routing";
    response = err.dump();
  };
  auto status = infra_->network_->Initialize(config_.listen_addr, legacy_handler);
  if (!status.ok()) {
    running_.store(false, std::memory_order_release);
    return status;
  }

  infra_->timer_->Start();

  // Start the shared coarse-grained tick timer.  Each group receives a tick
  // every kTickIntervalMs, driving group-local timeouts from a single timer.
  tick_timer_ = infra_->timer_->SetInterval(std::chrono::milliseconds(10), [this]() {
    std::shared_lock<std::shared_mutex> lock(groups_mtx_);
    for (const auto& [group_id, group] : groups_) {
      (void)group_id;
      group->OnStoreTick();
    }
  });

  return infra_->network_->Start();
}

Status RaftStore::Stop() {
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) {
    return Status::OK();
  }

  // Stop all groups first (they own timers/threads that may interact with
  // the network).  Intentionally do NOT clear groups_ here: the group objects
  // must outlive the running network so that any late in-flight RPC callbacks
  // never reference destroyed RaftNodeImpl state.  groups_ is cleared when the
  // RaftStore destructor runs, after the network has been stopped.
  {
    std::lock_guard<std::shared_mutex> lock(groups_mtx_);
    for (auto& [group_id, group] : groups_) {
      (void)group_id;
      group->Stop();
    }
  }

  if (infra_) {
    if (tick_timer_ != 0 && infra_->timer_) {
      infra_->timer_->CancelTimer(tick_timer_);
      tick_timer_ = 0;
    }
    if (infra_->network_) {
      infra_->network_->Stop();
    }
    if (infra_->timer_) {
      infra_->timer_->Stop();
    }
  }

  return Status::OK();
}

Status RaftStore::CreateGroup(uint64_t group_id, const RaftGroupOptions& options,
                              std::shared_ptr<StateMachine> state_machine) {
  if (group_id == 0) {
    return Status::Error("group_id must be > 0");
  }
  if (!state_machine) {
    return Status::Error("StateMachine cannot be null");
  }

  auto config = MakeGroupConfig(group_id, options);
  auto status = config.Validate();
  if (!status.ok()) {
    return status;
  }

  std::shared_ptr<Persister> persister;
  if (!config_.data_dir.empty()) {
    persister = std::make_shared<MultiRaftPersister>(group_id, config_.data_dir);
  }

  auto group = std::make_shared<RaftNode::RaftNodeImpl>(config, std::move(state_machine), infra_,
                                                        persister, group_id,
                                                        /*manage_network=*/false);

  status = group->Start();
  if (!status.ok()) {
    return status;
  }

  {
    std::lock_guard<std::shared_mutex> lock(groups_mtx_);
    auto it = groups_.find(group_id);
    if (it != groups_.end()) {
      it->second->Stop();
    }
    groups_[group_id] = std::move(group);
  }

  LOG_INFO("RaftStore created group {} on node {}", group_id, config_.node_id);
  return Status::OK();
}

Status RaftStore::RemoveGroup(uint64_t group_id) {
  std::lock_guard<std::shared_mutex> lock(groups_mtx_);
  auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return Status::Error("GROUP_NOT_FOUND", "Group not found: " + std::to_string(group_id));
  }
  it->second->Stop();
  groups_.erase(it);
  return Status::OK();
}

RaftNode::RaftNodeImpl* RaftStore::GetGroup(uint64_t group_id) const {
  std::shared_lock<std::shared_mutex> lock(groups_mtx_);
  auto it = groups_.find(group_id);
  return it != groups_.end() ? it->second.get() : nullptr;
}

std::vector<uint64_t> RaftStore::ListGroups() const {
  std::shared_lock<std::shared_mutex> lock(groups_mtx_);
  std::vector<uint64_t> result;
  result.reserve(groups_.size());
  for (const auto& [group_id, _] : groups_) {
    (void)_;
    result.push_back(group_id);
  }
  return result;
}

void RaftStore::OnIncomingRpc(NodeId from, uint64_t group_id, const std::string& data,
                              std::string& response) {
  if (group_id == 0) {
    nlohmann::json err;
    err["error"] = "GROUP_NOT_FOUND";
    err["message"] = "group_id 0 is not routed through RaftStore";
    response = err.dump();
    return;
  }

  std::shared_ptr<RaftNode::RaftNodeImpl> group;
  {
    std::shared_lock<std::shared_mutex> lock(groups_mtx_);
    auto it = groups_.find(group_id);
    if (it != groups_.end()) {
      group = it->second;
    }
  }

  if (!group) {
    nlohmann::json err;
    err["error"] = "GROUP_NOT_FOUND";
    err["message"] = "Group " + std::to_string(group_id) + " not found";
    response = err.dump();
    return;
  }

  group->HandleIncomingRpc(from, data, response);
}

RaftNodeConfig RaftStore::MakeGroupConfig(uint64_t group_id,
                                          const RaftGroupOptions& options) const {
  RaftNodeConfig config;
  config.node_id = config_.node_id;
  config.listen_addr = config_.listen_addr;
  config.peers = config_.peers;
  config.peer_node_ids =
      options.peer_node_ids.empty() ? config_.peer_node_ids : options.peer_node_ids;
  config.data_dir = config_.data_dir;
  if (!config.data_dir.empty()) {
    config.data_dir += "/groups/" + std::to_string(group_id);
  }

  if (options.election_timeout_ms > 0) {
    config.election_timeout_ms = options.election_timeout_ms;
  }
  if (options.heartbeat_interval_ms > 0) {
    config.heartbeat_interval_ms = options.heartbeat_interval_ms;
  }

  config.metrics_enabled = config_.metrics_enabled;
  config.metrics_addr = config_.metrics_addr;
  config.tls_enabled = config_.tls_enabled;
  config.tls_cert_file = config_.tls_cert_file;
  config.tls_key_file = config_.tls_key_file;
  config.tls_ca_file = config_.tls_ca_file;

  return config;
}

}  // namespace rollingraft
