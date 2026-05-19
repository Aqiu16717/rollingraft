#include <gtest/gtest.h>
#include "simulated_clock.h"
#include "simulated_network.h"
#include "test_cluster.h"

namespace rollingraft {

// ========== Chaos Test 1: Network Partition Recovery ==========
TEST(ChaosTest, PartitionRecovery) {
  TestCluster cluster({.num_nodes = 5, .seed = 42});
  cluster.StartAll();
  cluster.RunUntilLeaderElected();
  ASSERT_NE(cluster.GetLeaderId(), -1) << "No leader elected initially";

  for (int i = 0; i < 3; ++i) {
    auto status = cluster.ProposeToLeader("cmd" + std::to_string(i));
    ASSERT_TRUE(status.ok()) << "Propose failed: " << status.GetMessage();
  }

  NodeId leader = cluster.GetLeaderId();
  std::vector<NodeId> alone = {leader};
  std::vector<NodeId> others;
  for (NodeId i = 0; i < 5; ++i) {
    if (i != leader) others.push_back(i);
  }
  cluster.Partition(alone, others);
  cluster.RunFor(1000);

  cluster.HealAllPartitions();
  cluster.RunUntilLeaderElected();
  cluster.RunFor(500);
  ASSERT_NE(cluster.GetLeaderId(), -1) << "No leader after partition heal";

  for (int i = 3; i < 6; ++i) {
    auto status = cluster.ProposeToLeader("cmd" + std::to_string(i));
    ASSERT_TRUE(status.ok()) << "Post-recovery propose failed: " << status.GetMessage();
  }
  cluster.RunFor(500);
  cluster.AssertStateMachineEqual();
}

// ========== Chaos Test 2: Delay Storm ==========
TEST(ChaosTest, DelayStorm) {
  // election_timeout must be > 2x max message delay + heartbeat interval
  // to prevent followers from timing out before heartbeat acks arrive.
  TestCluster::Options opts;
  opts.num_nodes = 3;
  opts.seed = 123;
  opts.election_timeout_ms = 1000;  // > 2 * 200ms delay + margin
  opts.heartbeat_interval_ms = 50;
  opts.rpc_timeout_ms = 500;
  TestCluster cluster(opts);
  cluster.StartAll();
  cluster.DelayMessages(200);
  cluster.RunUntilLeaderElected();
  ASSERT_NE(cluster.GetLeaderId(), -1) << "No leader elected under delay storm";

  auto status = cluster.ProposeToLeader("delayed_cmd");
  ASSERT_TRUE(status.ok()) << "Propose under delay failed: " << status.GetMessage();

  cluster.DelayMessages(0);
  cluster.RunFor(500);
  cluster.AssertSingleLeader();
  cluster.AssertStateMachineEqual();
}

// ========== Chaos Test 3: Message Duplication + Reordering ==========
TEST(ChaosTest, DuplicateAndReorder) {
  TestCluster cluster({.num_nodes = 3, .seed = 999});
  cluster.StartAll();
  cluster.GetNetwork()->DuplicateMessages(0.3f);
  cluster.GetNetwork()->ReorderMessages(true);
  cluster.RunUntilLeaderElected();
  ASSERT_NE(cluster.GetLeaderId(), -1) << "No leader under duplication/reorder";

  for (int i = 0; i < 10; ++i) {
    auto status = cluster.ProposeToLeader("dup_cmd" + std::to_string(i));
    ASSERT_TRUE(status.ok()) << "Propose failed at cmd " << i << ": "
                             << status.GetMessage();
  }

  cluster.GetNetwork()->DuplicateMessages(0.0f);
  cluster.GetNetwork()->ReorderMessages(false);
  cluster.RunFor(500);
  cluster.AssertSingleLeader();
  cluster.AssertStateMachineEqual();
}

}  // namespace rollingraft
