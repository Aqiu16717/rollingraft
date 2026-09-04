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

RaftStore::RaftStore(const RaftStoreConfig& config)
    : config_(config), transport_batching_enabled_(config.transport_batching_enabled) {
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
          : (config_.tls_enabled ? CreateAsioNetworkTransport(TlsConfig{
                                       .enabled = true,
                                       .cert_file = config_.tls_cert_file,
                                       .key_file = config_.tls_key_file,
                                       .ca_file = config_.tls_ca_file,
                                       .mutual_auth = config_.tls_mutual_auth,
                                       .node_id = config_.node_id,
                                       .allowed_cns = config_.tls_allowed_peer_identities})
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

  return Status::OK();
}

Status RaftStore::Start() {
  if (!initialized_.load(std::memory_order_acquire)) {
    return Status::Error("Not initialized");
  }

  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return Status::Error("Already started");
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

  try {
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

    // Create the store-owned metrics HTTP server and wire store-level status /
    // admin providers. Groups skip creation via the create-once guard in
    // RaftNodeImpl::Start and never register group-capturing handlers.
    if (config_.metrics_enabled && !config_.metrics_addr.empty()) {
      MetricsHttpServer::TlsConfig tls_config;
      tls_config.enabled = config_.tls_enabled;
      tls_config.cert_file = config_.tls_cert_file;
      tls_config.key_file = config_.tls_key_file;
      tls_config.ca_file = config_.tls_ca_file;
      {
        std::lock_guard<std::mutex> lock(metrics_server_mtx_);
        infra_->metrics_server_ = std::make_unique<MetricsHttpServer>(
            config_.metrics_addr, infra_->metrics_.get(), tls_config, config_.admin_token);
      }

      RegisterStoreProviders();
      {
        std::lock_guard<std::mutex> lock(metrics_server_mtx_);
        infra_->metrics_server_->Start();
      }
    }
  } catch (const std::exception& e) {
    RollbackStart();
    return Status::Error("Failed to start RaftStore: " + std::string(e.what()));
  }

  infra_->network_->SetBatchingEnabled(transport_batching_enabled_.load(std::memory_order_acquire));
  status = infra_->network_->Start();
  if (!status.ok()) {
    RollbackStart();
  }
  return status;
}

void RaftStore::RollbackStart() {
  {
    std::lock_guard<std::mutex> lock(metrics_server_mtx_);
    if (infra_ && infra_->metrics_server_) {
      infra_->metrics_server_->Stop();
      infra_->metrics_server_.reset();
    }
  }
  if (tick_timer_ != 0 && infra_ && infra_->timer_) {
    infra_->timer_->CancelTimer(tick_timer_);
    tick_timer_ = 0;
  }
  if (infra_ && infra_->network_) {
    infra_->network_->Stop();
  }
  if (infra_ && infra_->timer_) {
    infra_->timer_->Stop();
  }
  running_.store(false, std::memory_order_release);
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
    // The shared metrics server is store-owned: groups never stop it (their
    // Stop would kill it out from under the remaining groups).
    {
      std::lock_guard<std::mutex> lock(metrics_server_mtx_);
      if (infra_->metrics_server_) {
        infra_->metrics_server_->Stop();
        infra_->metrics_server_.reset();
      }
    }
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
    auto transport_update =
        nlohmann::json{{"transport_batching_enabled",
                        transport_batching_enabled_.load(std::memory_order_acquire)}};
    (void)group->UpdateRuntimeConfig(transport_update.dump());
    auto it = groups_.find(group_id);
    if (it != groups_.end()) {
      it->second->Stop();
    }
    groups_[group_id] = group;
  }

  // Wire the group's EventBus to the shared metrics server's SSE broadcast.
  // The handler captures the store (not the group), so it stays valid after
  // the group is removed; the subscription dies with the group's EventBus.
  group->GetEventBus().SubscribeAll(
      [this, node_id = config_.node_id, group_id](const RaftEvent& event) {
        std::lock_guard<std::mutex> lock(metrics_server_mtx_);
        if (infra_ && infra_->metrics_server_) {
          infra_->metrics_server_->BroadcastEvent(
              RaftNode::RaftNodeImpl::FormatSseEvent(event, node_id, group_id));
        }
      });

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

std::shared_ptr<RaftNode::RaftNodeImpl> RaftStore::GetGroupShared(uint64_t group_id) const {
  std::shared_lock<std::shared_mutex> lock(groups_mtx_);
  auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return nullptr;
  }
  return it->second;
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

void RaftStore::RegisterStoreProviders() {
  if (!infra_->metrics_server_) {
    return;
  }

  // /v1/status: aggregate every group's status. Uses each group's public
  // getters (which take the group's own locks), so no store lock is held
  // while building the JSON.
  infra_->metrics_server_->SetStatusProvider([this]() -> std::string {
    nlohmann::json root;
    root["node_id"] = config_.node_id;
    nlohmann::json groups = nlohmann::json::array();

    std::vector<std::pair<uint64_t, std::shared_ptr<RaftNode::RaftNodeImpl>>> group_snapshot;
    {
      std::shared_lock<std::shared_mutex> lock(groups_mtx_);
      group_snapshot.reserve(groups_.size());
      for (const auto& [gid, g] : groups_) {
        group_snapshot.emplace_back(gid, g);
      }
    }

    // Top-level role/leader_id mirror the legacy single-node provider so
    // /readyz sees the fields it checks: a node is ready when it leads a
    // group or knows a group leader.
    bool any_leader = false;
    bool has_top_leader = false;
    NodeId top_leader_id = -1;
    for (const auto& [gid, group] : group_snapshot) {
      nlohmann::json j;
      j["group_id"] = gid;
      j["role"] = RaftNodeRoleToString(group->GetRole());
      j["term"] = group->CurrentTerm();
      j["leader_id"] = group->GetLeaderAddr().empty() ? nlohmann::json(nullptr)
                                                      : nlohmann::json(group->GetLeaderId());
      j["leader_addr"] = group->GetLeaderAddr();
      j["commit_index"] = group->GetCommitIndex();
      auto cfg = group->GetConfig();
      j["config_version"] = cfg.version;
      groups.push_back(std::move(j));

      if (group->GetRole() == RaftNodeRole::LEADER) {
        any_leader = true;
      }
      if (!has_top_leader && !group->GetLeaderAddr().empty()) {
        has_top_leader = true;
        top_leader_id = group->GetLeaderId();
      }
    }
    root["role"] = any_leader ? "Leader" : "Follower";
    root["leader_id"] = has_top_leader ? nlohmann::json(top_leader_id) : nlohmann::json(nullptr);
    root["groups"] = std::move(groups);
    return root.dump();
  });

  // Admin endpoints route by group_id (0 is rejected — multi-raft groups are
  // always >= 1).
  infra_->metrics_server_->SetAddMemberHandler(
      [this](int32_t node_id, const std::string& addr, uint64_t group_id) -> std::string {
        nlohmann::json j;
        if (group_id == 0) {
          j["error"] = "BAD_REQUEST";
          j["message"] = "group_id must be > 0";
          return j.dump();
        }
        auto group = GetGroupShared(group_id);
        if (!group) {
          j["error"] = "GROUP_NOT_FOUND";
          j["message"] = "Group " + std::to_string(group_id) + " not found";
          return j.dump();
        }
        auto status = group->AddNode(static_cast<NodeId>(node_id), addr);
        if (status.ok()) {
          j["status"] = "accepted";
          j["message"] = "Configuration change proposed";
        } else {
          j["error"] = status.IsNotLeader() ? "NOT_LEADER" : "ERROR";
          j["message"] = status.GetMessage();
          if (!group->GetLeaderAddr().empty()) {
            j["leader_hint"] = group->GetLeaderAddr();
          }
        }
        return j.dump();
      });

  infra_->metrics_server_->SetRemoveMemberHandler(
      [this](int32_t node_id, uint64_t group_id) -> std::string {
        nlohmann::json j;
        if (group_id == 0) {
          j["error"] = "BAD_REQUEST";
          j["message"] = "group_id must be > 0";
          return j.dump();
        }
        auto group = GetGroupShared(group_id);
        if (!group) {
          j["error"] = "GROUP_NOT_FOUND";
          j["message"] = "Group " + std::to_string(group_id) + " not found";
          return j.dump();
        }
        auto status = group->RemoveNode(static_cast<NodeId>(node_id));
        if (status.ok()) {
          j["status"] = "accepted";
          j["message"] = "Configuration change proposed";
        } else {
          j["error"] = status.IsNotLeader() ? "NOT_LEADER" : "ERROR";
          j["message"] = status.GetMessage();
          if (!group->GetLeaderAddr().empty()) {
            j["leader_hint"] = group->GetLeaderAddr();
          }
        }
        return j.dump();
      });

  infra_->metrics_server_->SetTriggerSnapshotHandler([this](uint64_t group_id) -> std::string {
    nlohmann::json j;
    if (group_id == 0) {
      j["error"] = "BAD_REQUEST";
      j["message"] = "group_id must be > 0";
      return j.dump();
    }
    auto group = GetGroup(group_id);
    if (!group) {
      j["error"] = "GROUP_NOT_FOUND";
      j["message"] = "Group " + std::to_string(group_id) + " not found";
      return j.dump();
    }
    auto status = group->TriggerSnapshot();
    if (status.ok()) {
      j["status"] = "triggered";
      j["message"] = "Snapshot creation initiated";
    } else {
      j["error"] = status.IsNotLeader() ? "NOT_LEADER" : "ERROR";
      j["message"] = status.GetMessage();
      if (!group->GetLeaderAddr().empty()) {
        j["leader_hint"] = group->GetLeaderAddr();
      }
    }
    return j.dump();
  });

  infra_->metrics_server_->SetTransferLeadershipHandler(
      [this](int32_t target_id, uint64_t group_id) -> std::string {
        nlohmann::json j;
        if (group_id == 0) {
          j["error"] = "BAD_REQUEST";
          j["message"] = "group_id must be > 0";
          return j.dump();
        }
        auto group = GetGroupShared(group_id);
        if (!group) {
          j["error"] = "GROUP_NOT_FOUND";
          j["message"] = "Group " + std::to_string(group_id) + " not found";
          return j.dump();
        }
        auto status = group->TransferLeadershipTo(static_cast<NodeId>(target_id));
        if (status.ok()) {
          j["status"] = "initiated";
          j["message"] = "Leadership transfer initiated";
        } else {
          j["error"] = status.IsNotLeader() ? "NOT_LEADER" : "ERROR";
          j["message"] = status.GetMessage();
          if (!group->GetLeaderAddr().empty()) {
            j["leader_hint"] = group->GetLeaderAddr();
          }
        }
        return j.dump();
      });

  // Consensus tuning is group-local. Store admin requests must identify the
  // target group so one group's timing changes cannot affect another group.
  infra_->metrics_server_->SetConfigProvider([this](uint64_t group_id) -> std::string {
    if (group_id == 0) {
      return "{\"error\":\"BAD_REQUEST\",\"message\":\"group_id must be > 0\"}";
    }
    auto group = GetGroupShared(group_id);
    if (!group) {
      return nlohmann::json{{"error", "GROUP_NOT_FOUND"},
                            {"message", "Group " + std::to_string(group_id) + " not found"}}
          .dump();
    }
    return group->GetRuntimeConfigJson();
  });
  infra_->metrics_server_->SetConfigUpdater([this](const std::string& json,
                                                   uint64_t group_id) -> std::string {
    if (group_id == 0) {
      return "{\"error\":\"BAD_REQUEST\",\"message\":\"group_id must be > 0\"}";
    }
    auto group = GetGroupShared(group_id);
    if (!group) {
      return nlohmann::json{{"error", "GROUP_NOT_FOUND"},
                            {"message", "Group " + std::to_string(group_id) + " not found"}}
          .dump();
    }
    auto old_config = group->GetRuntimeConfigValues();
    auto status = group->UpdateRuntimeConfig(json);
    if (status.ok()) {
      auto new_config = group->GetRuntimeConfigValues();
      if (new_config.transport_batching_enabled != old_config.transport_batching_enabled) {
        transport_batching_enabled_.store(new_config.transport_batching_enabled,
                                          std::memory_order_release);
        std::string transport_update =
            nlohmann::json{{"transport_batching_enabled", new_config.transport_batching_enabled}}
                .dump();
        {
          std::lock_guard<std::shared_mutex> lock(groups_mtx_);
          for (const auto& [id, hosted_group] : groups_) {
            (void)id;
            (void)hosted_group->UpdateRuntimeConfig(transport_update);
          }
        }
        infra_->network_->SetBatchingEnabled(new_config.transport_batching_enabled);
      }
      return "{\"status\":\"updated\",\"message\":\"Configuration updated successfully\"}";
    }
    nlohmann::json j;
    j["error"] = "INVALID_CONFIG";
    j["message"] = status.GetMessage();
    return j.dump();
  });
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
  config.admin_token = config_.admin_token;
  config.tls_enabled = config_.tls_enabled;
  config.tls_cert_file = config_.tls_cert_file;
  config.tls_key_file = config_.tls_key_file;
  config.tls_ca_file = config_.tls_ca_file;
  config.tls_mutual_auth = config_.tls_mutual_auth;
  config.tls_allowed_peer_identities = config_.tls_allowed_peer_identities;
  config.transport_batching_enabled = transport_batching_enabled_.load(std::memory_order_acquire);

  return config;
}

}  // namespace rollingraft
