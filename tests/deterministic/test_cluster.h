#pragma once
#include <cstdint>
#include <memory>
#include <vector>
#include "rollingraft/raft_node.h"
#include "rollingraft/types.h"
#include "tests/mock/mock_state_machine.h"
namespace rollingraft {
class SimulatedClock; class SimulatedNetwork;
class TestCluster {
 public:
  struct Options { size_t num_nodes = 3; uint64_t seed = 42; uint32_t election_timeout_ms = 300; uint32_t heartbeat_interval_ms = 50; uint32_t rpc_timeout_ms = 500; std::string data_dir_prefix = "/tmp/rollingraft_test"; };
  explicit TestCluster(const Options& options); ~TestCluster();
  void StartAll(); void StopAll(); void StartNode(NodeId id); void StopNode(NodeId id);
  void CrashNode(NodeId id); void RestartNode(NodeId id); void AdvanceTime(uint64_t ms);
  void RunUntilLeaderElected(); void RunUntilCommit(Index index); void RunFor(uint64_t ms); void RunUntilIdle();
  void Partition(std::vector<NodeId> group_a, std::vector<NodeId> group_b);
  void HealAllPartitions(); void DropMessages(float probability); void DelayMessages(uint64_t delay_ms);
  Status ProposeToLeader(const std::string& command); Status ProposeToNode(NodeId id, const std::string& command);
  void AssertNoLeader() const; void AssertSingleLeader() const; void AssertCommitted(Index index) const; void AssertAllApplied(Index index) const; void AssertStateMachineEqual() const;
  RaftNodeRole GetRole(NodeId id) const; NodeId GetLeaderId() const; std::vector<NodeId> GetLeaderIds() const;
  Index GetCommitIndex(NodeId id) const; Index GetLastApplied(NodeId id) const; size_t GetRunningNodeCount() const;
  SimulatedClock* GetClock() const; SimulatedNetwork* GetNetwork() const;
  RaftNode* GetNode(NodeId id) const; MockStateMachine* GetStateMachine(NodeId id) const;
 private:
  Options options_; std::unique_ptr<SimulatedClock> clock_; std::unique_ptr<SimulatedNetwork> network_;
  std::vector<std::unique_ptr<RaftNode>> nodes_; std::vector<std::shared_ptr<MockStateMachine>> state_machines_;
  std::vector<std::string> data_dirs_;
};
}  // namespace rollingraft
