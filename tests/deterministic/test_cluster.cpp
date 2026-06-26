#include "test_cluster.h"

#include <chrono>
#include <filesystem>
#include <future>

#include "mock/mock_persister.h"
#include "simulated_clock.h"
#include "simulated_network.h"
#include "simulated_network_transport.h"
#include "simulated_timer_service.h"
#include <gtest/gtest.h>
namespace rollingraft {
TestCluster::TestCluster(const Options& options)
    : options_(options),
      clock_(std::make_unique<SimulatedClock>()),
      network_(std::make_unique<SimulatedNetwork>(clock_.get(), options.seed)) {
  nodes_.resize(options.num_nodes);
  state_machines_.resize(options.num_nodes);
  data_dirs_.resize(options.num_nodes);
  for (size_t i = 0; i < options.num_nodes; ++i) {
    state_machines_[i] = std::make_shared<MockStateMachine>();
    data_dirs_[i] =
        options.data_dir_prefix + "_" + std::to_string(options.seed) + "_" + std::to_string(i);
    std::filesystem::remove_all(data_dirs_[i]);
    std::filesystem::create_directories(data_dirs_[i]);
  }
}
TestCluster::~TestCluster() {
  StopAll();
  for (const auto& d : data_dirs_) std::filesystem::remove_all(d);
}
void TestCluster::StartAll() {
  for (size_t i = 0; i < options_.num_nodes; ++i) StartNode(static_cast<NodeId>(i));
}
void TestCluster::StopAll() {
  for (size_t i = 0; i < options_.num_nodes; ++i) StopNode(static_cast<NodeId>(i));
}
void TestCluster::StartNode(NodeId id) {
  if (static_cast<size_t>(id) >= options_.num_nodes || nodes_[id]) return;
  RaftNodeConfig config;
  config.node_id = id;
  config.listen_addr = "127.0.0.1:" + std::to_string(8000 + id);
  config.data_dir = data_dirs_[id];
  config.election_timeout_ms = options_.election_timeout_ms;
  config.heartbeat_interval_ms = options_.heartbeat_interval_ms;
  config.rpc_timeout_ms = options_.rpc_timeout_ms;
  for (size_t i = 0; i < options_.num_nodes; ++i) {
    if (static_cast<NodeId>(i) != id) {
      config.peers.push_back("127.0.0.1:" + std::to_string(8000 + i));
      config.peer_node_ids.push_back(static_cast<NodeId>(i));
    }
  }
  config.check_quorum_enabled = false;  // Simulated clock incompatible with real-time CheckQuorum
  config.pre_vote_enabled = false;      // Pre-vote timing sensitive in simulated tests
  config.timer_factory = [this]() { return std::make_unique<SimulatedTimerService>(clock_.get()); };
  config.persister_factory = []() { return nullptr; };
  config.network_factory = [this, id]() {
    return std::make_unique<SimulatedNetworkTransport>(static_cast<NodeId>(id), network_.get(),
                                                       clock_.get());
  };
  nodes_[id] = std::make_unique<RaftNode>(config, state_machines_[id]);
  nodes_[id]->Start();
}
void TestCluster::StopNode(NodeId id) {
  if (static_cast<size_t>(id) >= options_.num_nodes || !nodes_[id]) return;
  nodes_[id]->Stop();
  nodes_[id].reset();
}
void TestCluster::CrashNode(NodeId id) {
  if (static_cast<size_t>(id) >= options_.num_nodes) return;
  nodes_[id].reset();
}
void TestCluster::RestartNode(NodeId id) {
  StopNode(id);
  StartNode(id);
}
void TestCluster::AdvanceTime(uint64_t ms) {
  clock_->Advance(ms);
  network_->DeliverAll();
}
void TestCluster::RunUntilLeaderElected() {
  for (int i = 0; i < 1000; ++i) {
    AdvanceTime(10);
    if (GetLeaderId() != -1) return;
  }
  ADD_FAILURE() << "No leader elected";
}
void TestCluster::RunUntilCommit(Index index) {
  for (int i = 0; i < 1000; ++i) {
    AdvanceTime(10);
    bool ok = true;
    for (size_t j = 0; j < options_.num_nodes; ++j)
      if (nodes_[j] && (GetCommitIndex(static_cast<NodeId>(j)) < index ||
                        GetLastApplied(static_cast<NodeId>(j)) < index)) {
        ok = false;
        break;
      }
    if (ok) return;
  }
  ADD_FAILURE() << "Index not committed";
}
void TestCluster::RunFor(uint64_t ms) {
  uint64_t t = clock_->Now() + ms;
  while (clock_->Now() < t) AdvanceTime(10);
}
void TestCluster::RunUntilIdle() {
  clock_->RunUntilIdle();
  network_->DeliverAll();
}
void TestCluster::Partition(const std::vector<NodeId>& ga, const std::vector<NodeId>& gb) {
  for (NodeId a : ga)
    for (NodeId b : gb) network_->Partition(a, b);
}
void TestCluster::HealAllPartitions() { network_->HealAllPartitions(); }
void TestCluster::DropMessages(float p) { network_->DropMessages(p); }
void TestCluster::DelayMessages(uint64_t d) { network_->DelayAll(d); }
Status TestCluster::ProposeToLeader(const std::string& command) {
  NodeId lid = GetLeaderId();
  if (lid == -1) return Status::Error("NO_LEADER", "No leader");
  return ProposeToNode(lid, command);
}
Status TestCluster::ProposeToNode(NodeId id, const std::string& command) {
  if (static_cast<size_t>(id) >= options_.num_nodes || !nodes_[id])
    return Status::Error("NODE_DOWN", "Node not running");
  auto promise = std::make_shared<std::promise<ApplyResult>>();
  auto future = promise->get_future();
  auto status =
      nodes_[id]->Propose(command, [promise](const ApplyResult& r) { promise->set_value(r); });
  if (!status.ok()) return status;
  for (int i = 0; i < 5000; ++i) {
    AdvanceTime(10);
    if (future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) break;
  }
  if (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
    return Status::Error("TIMEOUT", "Proposal timed out");
  }
  return Status::OK();
}
void TestCluster::AssertNoLeader() const { ASSERT_EQ(GetLeaderIds().size(), 0); }
void TestCluster::AssertSingleLeader() const { ASSERT_EQ(GetLeaderIds().size(), 1); }
void TestCluster::AssertCommitted(Index index) const {
  for (size_t i = 0; i < options_.num_nodes; ++i)
    if (nodes_[i]) ASSERT_GE(GetCommitIndex(static_cast<NodeId>(i)), index);
}
void TestCluster::AssertAllApplied(Index index) const {
  for (size_t i = 0; i < options_.num_nodes; ++i)
    if (nodes_[i]) ASSERT_GE(GetLastApplied(static_cast<NodeId>(i)), index);
}
void TestCluster::AssertStateMachineEqual() const {
  std::vector<std::string> ref;
  for (size_t i = 0; i < options_.num_nodes; ++i) {
    if (!nodes_[i]) continue;
    auto c = state_machines_[i]->GetAppliedCommands();
    if (ref.empty())
      ref = c;
    else
      ASSERT_EQ(c, ref);
  }
}
RaftNodeRole TestCluster::GetRole(NodeId id) const {
  if (static_cast<size_t>(id) >= options_.num_nodes || !nodes_[id]) return FOLLOWER;
  return nodes_[id]->GetRole();
}
NodeId TestCluster::GetLeaderId() const {
  auto l = GetLeaderIds();
  return l.empty() ? -1 : l[0];
}
std::vector<NodeId> TestCluster::GetLeaderIds() const {
  std::vector<NodeId> l;
  for (size_t i = 0; i < options_.num_nodes; ++i)
    if (nodes_[i] && nodes_[i]->IsLeader()) l.push_back(static_cast<NodeId>(i));
  return l;
}
Index TestCluster::GetCommitIndex(NodeId id) const {
  if (static_cast<size_t>(id) >= options_.num_nodes || !nodes_[id]) return 0;
  return nodes_[id]->GetCommitIndex();
}
Index TestCluster::GetLastApplied(NodeId id) const {
  if (static_cast<size_t>(id) >= options_.num_nodes || !state_machines_[id]) return 0;
  return state_machines_[id]->GetLastAppliedIndex();
}
size_t TestCluster::GetRunningNodeCount() const {
  size_t c = 0;
  for (const auto& n : nodes_)
    if (n) ++c;
  return c;
}
RaftNode* TestCluster::GetNode(NodeId id) const {
  if (static_cast<size_t>(id) >= options_.num_nodes) return nullptr;
  return nodes_[id].get();
}
MockStateMachine* TestCluster::GetStateMachine(NodeId id) const {
  if (static_cast<size_t>(id) >= options_.num_nodes) return nullptr;
  return state_machines_[id].get();
}
SimulatedClock* TestCluster::GetClock() const { return clock_.get(); }
SimulatedNetwork* TestCluster::GetNetwork() const { return network_.get(); }
}  // namespace rollingraft
