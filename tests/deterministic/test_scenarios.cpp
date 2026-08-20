#include <atomic>
#include <memory>

#include "simulated_clock.h"
#include "simulated_network.h"
#include "test_cluster.h"
#include <gtest/gtest.h>

namespace rollingraft {

// ========== Scenario: repeated partition / heal cycles ==========
// The cluster must elect exactly one leader after every heal and converge
// to identical state machines.
TEST(DeterministicScenario, PartitionStormConverges) {
  TestCluster cluster({.num_nodes = 5, .seed = 7});
  cluster.StartAll();
  cluster.RunUntilLeaderElected();
  ASSERT_NE(cluster.GetLeaderId(), -1);

  for (int round = 0; round < 3; ++round) {
    // Propose some entries before splitting.
    for (int i = 0; i < 3; ++i) {
      ASSERT_TRUE(
          cluster.ProposeToLeader("pre_" + std::to_string(round) + "_" + std::to_string(i)).ok());
    }
    cluster.RunFor(200);

    // Split into two halves (leader on one side).
    NodeId leader = cluster.GetLeaderId();
    std::vector<NodeId> a = {leader};
    std::vector<NodeId> b;
    for (NodeId i = 0; i < 5; ++i) {
      if (i != leader) {
        b.push_back(i);
      }
    }
    cluster.Partition(a, b);
    cluster.RunFor(500);
    cluster.HealAllPartitions();
    cluster.RunUntilLeaderElected();
    cluster.RunFor(300);

    EXPECT_EQ(cluster.CountLeaders(), 1) << "round " << round << " must have one leader";
  }

  // Everything eventually applied on every node.
  cluster.RunFor(500);
  cluster.AssertStateMachineEqual();
}

// ========== Scenario: leader isolated after commits ==========
// A partitioned leader must not commit new entries without a quorum, and
// healing must restore a single consistent state.
TEST(DeterministicScenario, IsolatedLeaderCannotCommit) {
  TestCluster cluster({.num_nodes = 3, .seed = 11});
  cluster.StartAll();
  cluster.RunUntilLeaderElected();
  NodeId leader = cluster.GetLeaderId();
  ASSERT_NE(leader, -1);

  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(cluster.ProposeToLeader("committed_" + std::to_string(i)).ok());
  }
  cluster.RunFor(300);
  auto commit_before = cluster.GetCommitIndex(leader);

  // Isolate the leader; it should not be able to commit anything new.
  std::vector<NodeId> alone = {leader};
  std::vector<NodeId> others;
  for (NodeId i = 0; i < 3; ++i) {
    if (i != leader) {
      others.push_back(i);
    }
  }
  cluster.Partition(alone, others);
  cluster.RunFor(500);
  EXPECT_EQ(cluster.GetCommitIndex(leader), commit_before)
      << "isolated leader must not advance commit index";

  // Proposals on the isolated leader either fail or never commit.
  cluster.ProposeToNode(leader, "should_not_commit");
  cluster.RunFor(500);
  EXPECT_EQ(cluster.GetCommitIndex(leader), commit_before)
      << "isolated leader committed an entry without quorum";

  // Heal; the cluster re-elects and converges to the committed prefix.
  cluster.HealAllPartitions();
  cluster.RunUntilLeaderElected();
  cluster.RunFor(500);
  cluster.AssertStateMachineEqual();
}

TEST(DeterministicScenario, CheckQuorumUsesSimulatedClock) {
  TestCluster cluster(
      {.num_nodes = 3, .seed = 19, .election_timeout_ms = 300, .check_quorum_enabled = true});
  cluster.StartAll();
  cluster.RunUntilLeaderElected();
  NodeId isolated_leader = cluster.GetLeaderId();
  ASSERT_NE(isolated_leader, -1);

  std::vector<NodeId> majority;
  for (NodeId id = 0; id < 3; ++id) {
    if (id != isolated_leader) {
      majority.push_back(id);
    }
  }
  cluster.Partition({isolated_leader}, majority);
  cluster.RunFor(500);

  EXPECT_NE(cluster.GetRole(isolated_leader), RaftNodeRole::LEADER)
      << "isolated leader must step down after one simulated election timeout";
}

TEST(DeterministicScenario, LeaderLeaseExpiresOnSimulatedClock) {
  TestCluster cluster({.num_nodes = 3, .seed = 23, .election_timeout_ms = 300});
  cluster.StartAll();
  cluster.RunUntilLeaderElected();
  NodeId isolated_leader = cluster.GetLeaderId();
  ASSERT_NE(isolated_leader, -1);

  // Allow an initial heartbeat response to establish the lease.
  cluster.RunFor(100);
  auto lease_read_completed = std::make_shared<std::atomic<bool>>(false);
  ASSERT_TRUE(cluster.GetNode(isolated_leader)
                  ->ReadIndex([lease_read_completed] { *lease_read_completed = true; })
                  .ok());
  EXPECT_TRUE(lease_read_completed->load());

  std::vector<NodeId> others;
  for (NodeId id = 0; id < 3; ++id) {
    if (id != isolated_leader) {
      others.push_back(id);
    }
  }
  cluster.Partition({isolated_leader}, others);
  cluster.RunFor(400);

  auto expired_read_completed = std::make_shared<std::atomic<bool>>(false);
  ASSERT_TRUE(cluster.GetNode(isolated_leader)
                  ->ReadIndex([expired_read_completed] { *expired_read_completed = true; })
                  .ok());
  cluster.RunFor(100);
  EXPECT_FALSE(expired_read_completed->load())
      << "expired lease must fall back to quorum heartbeats";
}

// ========== Scenario: membership change (add then remove) ==========
// Adding a voter and removing it must commit and converge on all nodes.
TEST(DeterministicScenario, MembershipAddRemoveConverges) {
  TestCluster cluster({.num_nodes = 3, .seed = 31});
  cluster.StartAll();
  cluster.RunUntilLeaderElected();
  auto* leader_node = cluster.GetNode(cluster.GetLeaderId());
  ASSERT_NE(leader_node, nullptr);

  // Add node 3 (0-indexed) as a voter.
  ASSERT_TRUE(leader_node->AddNode(3, "127.0.0.1:8003").ok());
  cluster.RunFor(1000);

  bool has_new = false;
  for (NodeId i = 0; i < 3; ++i) {
    auto cfg = cluster.GetNode(i)->GetConfig();
    if (std::find(cfg.nodes.begin(), cfg.nodes.end(), 3) != cfg.nodes.end()) {
      has_new = true;
      break;
    }
  }
  EXPECT_TRUE(has_new) << "added node should appear in cluster config";

  // Propose through the leader; the larger quorum must still work.
  ASSERT_TRUE(cluster.ProposeToLeader("post_add").ok());
  cluster.RunFor(500);

  // Remove node 3 again.
  auto* leader_after = cluster.GetNode(cluster.GetLeaderId());
  ASSERT_NE(leader_after, nullptr);
  ASSERT_TRUE(leader_after->RemoveNode(3).ok());
  cluster.RunFor(1000);

  bool still_has = false;
  for (NodeId i = 0; i < 3; ++i) {
    auto cfg = cluster.GetNode(i)->GetConfig();
    if (std::find(cfg.nodes.begin(), cfg.nodes.end(), 3) != cfg.nodes.end()) {
      still_has = true;
      break;
    }
  }
  EXPECT_FALSE(still_has) << "removed node should leave the cluster config";
  cluster.AssertStateMachineEqual();
}

}  // namespace rollingraft
