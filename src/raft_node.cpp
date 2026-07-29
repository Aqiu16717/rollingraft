#include "rollingraft/tls_config.h"

#include "asio_timer_service.h"
#include "json_protocol.h"
#include "raft_node_impl.h"

// Forward declaration for default network transport
namespace rollingraft {
std::unique_ptr<NetworkTransport> CreateDefaultNetworkTransport();
std::unique_ptr<NetworkTransport> CreateAsioNetworkTransport(const TlsConfig& tls_config);
}  // namespace rollingraft

using namespace rollingraft;

namespace {

/**
 * Validate host:port address format.
 * @param addr Address string to validate
 * @param field_name Field name for error messages
 * @return Status::OK() if valid, error otherwise
 */
Status ValidateAddr(const std::string& addr, const char* field_name) {
  if (addr.empty()) {
    return Status::Error("CONFIG_INVALID", std::string(field_name) + " cannot be empty");
  }

  size_t colon_pos = addr.rfind(':');
  if (colon_pos == std::string::npos || colon_pos == 0 || colon_pos == addr.size() - 1) {
    return Status::Error("CONFIG_INVALID",
                         std::string(field_name) + " must be in host:port format: " + addr);
  }

  std::string port_str = addr.substr(colon_pos + 1);
  try {
    size_t idx = 0;
    int port = std::stoi(port_str, &idx);
    if (idx != port_str.size() || port <= 0 || port > 65535) {
      return Status::Error("CONFIG_INVALID",
                           std::string(field_name) + " has invalid port: " + port_str);
    }
  } catch (const std::exception&) {
    return Status::Error("CONFIG_INVALID",
                         std::string(field_name) + " has invalid port: " + port_str);
  }

  return Status::OK();
}

}  // namespace

// ========== RaftNodeConfig Validation ==========

Status RaftNodeConfig::Validate() const {
  // Required fields
  if (node_id < 0) {
    return Status::Error("CONFIG_INVALID", "node_id must be non-negative");
  }

  auto status = ValidateAddr(listen_addr, "listen_addr");
  if (!status.ok()) {
    return status;
  }

  if (data_dir.empty()) {
    return Status::Error("CONFIG_INVALID", "data_dir cannot be empty");
  }

  // Timing parameters
  if (election_timeout_ms == 0) {
    return Status::Error("CONFIG_INVALID", "election_timeout_ms must be > 0");
  }
  if (heartbeat_interval_ms == 0) {
    return Status::Error("CONFIG_INVALID", "heartbeat_interval_ms must be > 0");
  }
  if (election_timeout_ms <= heartbeat_interval_ms) {
    return Status::Error("CONFIG_INVALID", "election_timeout_ms (" +
                                               std::to_string(election_timeout_ms) +
                                               ") must be > heartbeat_interval_ms (" +
                                               std::to_string(heartbeat_interval_ms) + ")");
  }

  // Peer consistency
  if (!peer_node_ids.empty() && peer_node_ids.size() != peers.size()) {
    return Status::Error("CONFIG_INVALID",
                         "peer_node_ids.size() (" + std::to_string(peer_node_ids.size()) +
                             ") must match peers.size() (" + std::to_string(peers.size()) + ")");
  }

  // Metrics address
  if (metrics_enabled && !metrics_addr.empty()) {
    status = ValidateAddr(metrics_addr, "metrics_addr");
    if (!status.ok()) {
      return status;
    }
  }

  // TLS consistency
  if (tls_enabled) {
    if (tls_cert_file.empty()) {
      return Status::Error("CONFIG_INVALID", "tls_cert_file cannot be empty when tls_enabled=true");
    }
    if (tls_key_file.empty()) {
      return Status::Error("CONFIG_INVALID", "tls_key_file cannot be empty when tls_enabled=true");
    }
  }

  // Positive values
  if (rpc_timeout_ms == 0) {
    return Status::Error("CONFIG_INVALID", "rpc_timeout_ms must be > 0");
  }
  if (max_entries_per_append == 0) {
    return Status::Error("CONFIG_INVALID", "max_entries_per_append must be > 0");
  }
  // Shutdown timeout must be 0 (infinite) or >= 100ms
  if (shutdown_timeout_ms > 0 && shutdown_timeout_ms < 100) {
    return Status::Error("CONFIG_INVALID", "shutdown_timeout_ms must be 0 or >= 100");
  }
  // Dead node timeout must be > 0 if auto-removal is enabled
  if (auto_remove_dead_nodes && dead_node_timeout_ms == 0) {
    return Status::Error("CONFIG_INVALID",
                         "dead_node_timeout_ms must be > 0 when auto_remove_dead_nodes is enabled");
  }

  // Compression type must be 0 (none) or 1 (snappy)
  if (compression_type > 1) {
    return Status::Error("CONFIG_INVALID", "compression_type must be 0 (none) or 1 (snappy), got " +
                                               std::to_string(compression_type));
  }

  return Status::OK();
}

// ========== RaftNode Public Interface ==========

RaftNode::RaftNode(const RaftNodeConfig& config, std::shared_ptr<StateMachine> sm) {
  auto status = config.Validate();
  if (!status.ok()) {
    throw std::invalid_argument("RaftNodeConfig validation failed: " + status.ToString());
  }

  auto infra = std::make_shared<SharedNodeInfra>();
  infra->network_ =
      config.network_factory
          ? config.network_factory()
          : (config.tls_enabled
                 ? CreateAsioNetworkTransport(TlsConfig{.enabled = true,
                                                        .cert_file = config.tls_cert_file,
                                                        .key_file = config.tls_key_file,
                                                        .ca_file = config.tls_ca_file})
                 : CreateDefaultNetworkTransport());
  infra->timer_ = config.timer_factory ? config.timer_factory() : TimerService::CreateDefault();
  infra->protocol_ =
      config.protocol_factory ? config.protocol_factory() : std::make_unique<JsonProtocol>();

  raft_node_impl_ = std::make_unique<RaftNodeImpl>(
      config, sm, std::move(infra),
      config.persister_factory ? std::shared_ptr<Persister>(config.persister_factory()) : nullptr);
}

RaftNode::~RaftNode() = default;

Status RaftNode::Start() { return raft_node_impl_->Start(); }

Status RaftNode::Stop() { return raft_node_impl_->Stop(); }

bool RaftNode::IsLeader() const { return raft_node_impl_->IsLeader(); }

RaftNodeRole RaftNode::GetRole() const { return raft_node_impl_->GetRole(); }

Term RaftNode::CurrentTerm() const { return raft_node_impl_->CurrentTerm(); }

NodeAddr RaftNode::GetLeaderAddr() const { return raft_node_impl_->GetLeaderAddr(); }

EventBus& RaftNode::GetEventBus() { return raft_node_impl_->GetEventBus(); }

void RaftNode::SetRoleChangeCallback(std::function<void(RaftNodeRole role, Term term)> callback) {
  raft_node_impl_->SetRoleChangeCallback(std::move(callback));
}

void RaftNode::SetLeaderChangeCallback(
    std::function<void(NodeId leader_id, const NodeAddr& addr)> callback) {
  raft_node_impl_->SetLeaderChangeCallback(std::move(callback));
}

Status RaftNode::Propose(const std::string& command,
                         std::function<void(const ApplyResult& result)> callback,
                         uint64_t session_id, uint64_t seq_num) {
  return raft_node_impl_->Propose(command, std::move(callback), session_id, seq_num);
}

Status RaftNode::ProposeBatch(
    const std::vector<std::string>& commands,
    std::function<void(const std::vector<ApplyResult>& results)> callback) {
  return raft_node_impl_->ProposeBatch(commands, std::move(callback));
}

Status RaftNode::ReadIndex(std::function<void()> callback) {
  return raft_node_impl_->ReadIndex(std::move(callback));
}

Status RaftNode::AddNode(NodeId id, const NodeAddr& addr) {
  return raft_node_impl_->AddNode(id, addr);
}

Status RaftNode::AddLearner(NodeId id, const NodeAddr& addr) {
  return raft_node_impl_->AddLearner(id, addr);
}

Status RaftNode::PromoteLearner(NodeId id) { return raft_node_impl_->PromoteLearner(id); }

Status RaftNode::RemoveNode(NodeId id) { return raft_node_impl_->RemoveNode(id); }

ClusterConfig RaftNode::GetConfig() const { return raft_node_impl_->GetConfig(); }

Status RaftNode::TriggerSnapshot() { return raft_node_impl_->TriggerSnapshot(); }

Status RaftNode::TransferLeadershipTo(NodeId target_id) {
  return raft_node_impl_->TransferLeadershipTo(target_id);
}

Index RaftNode::GetCommitIndex() const { return raft_node_impl_->GetCommitIndex(); }
