#include <gtest/gtest.h>

#include "rollingraft/raft_node.h"

#include "mock/mock_persister.h"
#include "mock/mock_state_machine.h"
#include "test_port.h"

using namespace rollingraft;

// ========== ClusterConfig Learner Tests ==========

class LearnerClusterConfigTest : public ::testing::Test {};

TEST_F(LearnerClusterConfigTest, Learner_IsMember_But_NotVoter) {
  ClusterConfig config;
  config.nodes = {1, 2, 3};
  config.learners = {4, 5};

  EXPECT_TRUE(config.Contains(4));
  EXPECT_TRUE(config.Contains(5));
  EXPECT_TRUE(config.IsLearner(4));
  EXPECT_TRUE(config.IsLearner(5));
  EXPECT_FALSE(config.IsVoter(4));
  EXPECT_FALSE(config.IsVoter(5));

  // Voters are not learners
  EXPECT_TRUE(config.IsVoter(1));
  EXPECT_FALSE(config.IsLearner(1));
}

TEST_F(LearnerClusterConfigTest, Majority_ExcludesLearners) {
  ClusterConfig config;
  config.nodes = {1, 2, 3};
  config.learners = {4, 5, 6, 7};

  // Majority is based on voters only: 3/2 + 1 = 2
  EXPECT_EQ(config.GetMajority(), 2);
}

TEST_F(LearnerClusterConfigTest, RemoveNode_AlsoRemovesLearner) {
  ClusterConfig config;
  config.nodes = {1, 2, 3};
  config.learners = {4};

  // Simulate REMOVE:4
  config.nodes.erase(std::remove(config.nodes.begin(), config.nodes.end(), 4),
                     config.nodes.end());
  config.learners.erase(
      std::remove(config.learners.begin(), config.learners.end(), 4),
      config.learners.end());

  EXPECT_FALSE(config.Contains(4));
  EXPECT_FALSE(config.IsLearner(4));
}

// ========== RaftNode Learner API Tests ==========

class LearnerNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    sm_ = std::make_shared<MockStateMachine>();
    ports_ = GetTestPorts(10);
  }

  void TearDown() override {
    if (node_) {
      node_->Stop();
    }
  }

  RaftNodeConfig MakeConfig(NodeId id, const std::vector<NodeId>& peers) {
    RaftNodeConfig config;
    config.node_id = id;
    config.listen_addr = "127.0.0.1:" + std::to_string(ports_[id]);
    config.election_timeout_ms = 300;
    config.heartbeat_interval_ms = 100;
    config.data_dir = "/tmp/raft_test_node_" + std::to_string(id);

    for (NodeId peer_id : peers) {
      config.peers.push_back("127.0.0.1:" + std::to_string(ports_[peer_id]));
      config.peer_node_ids.push_back(peer_id);
    }

    return config;
  }

  std::shared_ptr<MockStateMachine> sm_;
  std::unique_ptr<RaftNode> node_;
  std::vector<uint16_t> ports_;
};

TEST_F(LearnerNodeTest, AddLearner_BeforeStart_ReturnsError) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  auto status = node_->AddLearner(4, "127.0.0.1:9004");
  EXPECT_FALSE(status.ok());
}

TEST_F(LearnerNodeTest, AddLearner_AsFollower_ReturnsNotLeader) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  auto status = node_->AddLearner(4, "127.0.0.1:9004");
  EXPECT_FALSE(status.ok());
}

TEST_F(LearnerNodeTest, PromoteLearner_BeforeStart_ReturnsError) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  auto status = node_->PromoteLearner(4);
  EXPECT_FALSE(status.ok());
}

TEST_F(LearnerNodeTest, PromoteLearner_AsFollower_ReturnsNotLeader) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  auto status = node_->PromoteLearner(4);
  EXPECT_FALSE(status.ok());
}

TEST_F(LearnerNodeTest, PromoteLearner_NotALearner_ReturnsError) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Node 2 is a voter, not a learner
  auto status = node_->PromoteLearner(2);
  EXPECT_FALSE(status.ok());
}
