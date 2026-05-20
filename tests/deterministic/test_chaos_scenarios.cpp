#include <gtest/gtest.h>

#include "simulated_network.h"
#include "test_cluster.h"

using namespace rollingraft;

class ChaosScenariosTest : public ::testing::Test {
 protected:
  TestCluster::Options MakeOptions() {
    TestCluster::Options opts;
    opts.num_nodes = 3;
    opts.election_timeout_ms = 300;
    opts.heartbeat_interval_ms = 50;
    opts.rpc_timeout_ms = 500;
    return opts;
  }
};

// ========== Scenario 1: Network Partition Recovery ==========
TEST_F(ChaosScenariosTest, PartitionRecovery) {
  TestCluster cluster(MakeOptions());
  cluster.StartAll();
  cluster.RunUntilLeaderElected();
  cluster.AssertSingleLeader();

  NodeId leader = cluster.GetLeaderId();
  ASSERT_NE(leader, -1);

  // Propose a command before partition
  auto status = cluster.ProposeToLeader("cmd_before_partition");
  ASSERT_TRUE(status.ok()) << status.ToString();
  cluster.RunUntilCommit(1);
  cluster.AssertCommitted(1);

  // Partition leader from followers
  std::vector<NodeId> group_a = {leader};
  std::vector<NodeId> group_b;
  for (size_t i = 0; i < 3; ++i) {
    if (static_cast<NodeId>(i) != leader) group_b.push_back(static_cast<NodeId>(i));
  }
  cluster.Partition(group_a, group_b);

  // Run for a while — partitioned leader should step down
  cluster.RunFor(1000);

  // Heal partition
  cluster.HealAllPartitions();

  // Cluster should re-elect a leader.
  // Allow extra time for the old partitioned leader to receive messages
  // from the new leader and step down.
  cluster.RunUntilLeaderElected();
  cluster.RunFor(500);
  cluster.AssertSingleLeader();

  // Propose after recovery
  status = cluster.ProposeToLeader("cmd_after_recovery");
  ASSERT_TRUE(status.ok()) << status.ToString();
  cluster.RunUntilCommit(2);
  cluster.AssertCommitted(2);
  cluster.AssertStateMachineEqual();
}

// ========== Scenario 2: Delay Storm ==========
TEST_F(ChaosScenariosTest, DelayStorm) {
  TestCluster cluster(MakeOptions());
  cluster.StartAll();
  cluster.RunUntilLeaderElected();
  cluster.AssertSingleLeader();

  // Inject high delay
  cluster.DelayMessages(150);

  // Propose multiple commands
  for (int i = 0; i < 5; ++i) {
    auto status = cluster.ProposeToLeader("cmd_" + std::to_string(i));
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  // Run longer to allow delayed messages to settle
  cluster.RunFor(2000);
  cluster.AssertCommitted(5);
  cluster.AssertStateMachineEqual();
}

// ========== Scenario 3: Duplicate Messages ==========
TEST_F(ChaosScenariosTest, DuplicateMessages) {
  TestCluster cluster(MakeOptions());
  cluster.StartAll();
  cluster.RunUntilLeaderElected();
  cluster.AssertSingleLeader();

  // Enable 30% duplicate probability
  cluster.GetNetwork()->DuplicateMessages(0.30f);

  // Propose commands
  for (int i = 0; i < 5; ++i) {
    auto status = cluster.ProposeToLeader("dup_cmd_" + std::to_string(i));
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  cluster.RunFor(1500);
  cluster.AssertCommitted(5);
  cluster.AssertStateMachineEqual();
}
