#include "rollingraft/raft_node.h"

#include <gtest/gtest.h>

using namespace rollingraft;

class JointConsensusTest : public ::testing::Test {};

TEST_F(JointConsensusTest, ClusterConfig_NormalMajority) {
  ClusterConfig config;
  config.nodes = {1, 2, 3};
  config.is_joint = false;

  EXPECT_EQ(config.GetMajority(), 2);  // 3/2 + 1 = 2
  EXPECT_TRUE(config.Contains(1));
  EXPECT_TRUE(config.Contains(2));
  EXPECT_TRUE(config.Contains(3));
  EXPECT_FALSE(config.Contains(4));
  EXPECT_TRUE(config.IsVoter(1));
  EXPECT_TRUE(config.JointMajoritySatisfied(0, 2));
  EXPECT_FALSE(config.JointMajoritySatisfied(0, 1));
}

TEST_F(JointConsensusTest, ClusterConfig_JointMajority_BothRequired) {
  ClusterConfig config;
  config.old_nodes = {1, 2, 3};
  config.nodes = {1, 2, 3, 4};
  config.is_joint = true;

  EXPECT_EQ(config.GetOldMajority(), 2);  // 3/2 + 1 = 2
  EXPECT_EQ(config.GetMajority(), 3);     // 4/2 + 1 = 3

  // Must satisfy BOTH old and new majorities
  EXPECT_TRUE(config.JointMajoritySatisfied(2, 3));
  EXPECT_FALSE(config.JointMajoritySatisfied(1, 3));  // old insufficient
  EXPECT_FALSE(config.JointMajoritySatisfied(2, 2));  // new insufficient
  EXPECT_FALSE(config.JointMajoritySatisfied(1, 2));  // both insufficient
}

TEST_F(JointConsensusTest, ClusterConfig_JointVoterCheck) {
  ClusterConfig config;
  config.old_nodes = {1, 2, 3};
  config.nodes = {2, 3, 4};
  config.is_joint = true;

  // In joint mode, any node in old OR new can vote
  EXPECT_TRUE(config.IsVoter(1));   // in old only
  EXPECT_TRUE(config.IsVoter(2));   // in both
  EXPECT_TRUE(config.IsVoter(4));   // in new only
  EXPECT_FALSE(config.IsVoter(5));  // in neither
}

TEST_F(JointConsensusTest, ClusterConfig_Contains_JointMode) {
  ClusterConfig config;
  config.old_nodes = {1, 2};
  config.nodes = {3, 4};
  config.is_joint = true;

  EXPECT_TRUE(config.Contains(1));
  EXPECT_TRUE(config.Contains(2));
  EXPECT_TRUE(config.Contains(3));
  EXPECT_TRUE(config.Contains(4));
  EXPECT_FALSE(config.Contains(5));
}

TEST_F(JointConsensusTest, ClusterConfig_NormalMode_NoOldNodes) {
  ClusterConfig config;
  config.nodes = {1, 2, 3, 4, 5};
  config.is_joint = false;

  EXPECT_EQ(config.GetMajority(), 3);     // 5/2 + 1 = 3
  EXPECT_EQ(config.GetOldMajority(), 1);  // empty old -> 0/2 + 1 = 1
  EXPECT_TRUE(config.JointMajoritySatisfied(0, 3));
  EXPECT_FALSE(config.JointMajoritySatisfied(0, 2));
}
