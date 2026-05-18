#include "asio_timer_service.h"
#include "json_protocol.h"
#include "raft_node_impl.h"

// Forward declaration for default network transport
namespace rollingraft {
std::unique_ptr<NetworkTransport> CreateDefaultNetworkTransport();
}  // namespace rollingraft

using namespace rollingraft;

// ========== RaftNode Public Interface ==========

RaftNode::RaftNode(const RaftNodeConfig& config,
                   std::shared_ptr<StateMachine> sm)
    : raft_node_impl_(std::make_unique<RaftNodeImpl>(
          config, sm,
          config.network_factory ? config.network_factory()
                                 : CreateDefaultNetworkTransport(),
          config.timer_factory ? config.timer_factory()
                               : TimerService::CreateDefault(),
          config.persister_factory
              ? std::shared_ptr<Persister>(config.persister_factory())
              : nullptr,
          config.protocol_factory ? config.protocol_factory()
                                  : std::make_unique<JsonProtocol>())) {}

RaftNode::~RaftNode() = default;

Status RaftNode::Start() { return raft_node_impl_->Start(); }

Status RaftNode::Stop() { return raft_node_impl_->Stop(); }

bool RaftNode::IsLeader() const { return raft_node_impl_->IsLeader(); }

RaftNodeRole RaftNode::GetRole() const { return raft_node_impl_->GetRole(); }

Term RaftNode::CurrentTerm() const { return raft_node_impl_->CurrentTerm(); }

NodeAddr RaftNode::GetLeaderAddr() const {
  return raft_node_impl_->GetLeaderAddr();
}

EventBus& RaftNode::GetEventBus() { return raft_node_impl_->GetEventBus(); }

void RaftNode::SetRoleChangeCallback(
    std::function<void(RaftNodeRole role, Term term)> callback) {
  raft_node_impl_->SetRoleChangeCallback(std::move(callback));
}

void RaftNode::SetLeaderChangeCallback(
    std::function<void(NodeId leader_id, const NodeAddr& addr)> callback) {
  raft_node_impl_->SetLeaderChangeCallback(std::move(callback));
}

Status RaftNode::Propose(
    const std::string& command,
    std::function<void(const ApplyResult& result)> callback) {
  return raft_node_impl_->Propose(command, std::move(callback));
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

Status RaftNode::RemoveNode(NodeId id) {
  return raft_node_impl_->RemoveNode(id);
}

ClusterConfig RaftNode::GetConfig() const {
  return raft_node_impl_->GetConfig();
}

Status RaftNode::TriggerSnapshot() {
  return raft_node_impl_->TriggerSnapshot();
}

Status RaftNode::TransferLeadershipTo(NodeId target_id) {
  return raft_node_impl_->TransferLeadershipTo(target_id);
}
