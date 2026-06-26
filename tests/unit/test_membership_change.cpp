#include <chrono>
#include <memory>

#include "rollingraft/raft_node.h"

#include "mock/mock_persister.h"
#include "mock/mock_state_machine.h"
#include "test_port.h"
#include <gtest/gtest.h>

using namespace rollingraft;

/**
 * Membership change tests.
 */

class MembershipChangeTest : public ::testing::Test {
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
    // Note: ParseNodeId extracts port number as node ID
    // So node ID should match the port (8001, 8002, etc.)
    RaftNodeConfig config;
    config.node_id = 8000 + id;  // Node ID = port number
    config.listen_addr = "127.0.0.1:" + std::to_string(ports_[id]);
    config.election_timeout_ms = 300;
    config.heartbeat_interval_ms = 100;
    config.data_dir = "/tmp/raft_test_node_" + std::to_string(id);

    for (NodeId peer_id : peers) {
      config.peers.push_back("127.0.0.1:" + std::to_string(ports_[peer_id]));
      config.peer_node_ids.push_back(8000 + peer_id);
    }

    return config;
  }

  std::shared_ptr<MockStateMachine> sm_;
  std::unique_ptr<RaftNode> node_;
  std::vector<uint16_t> ports_;
};

TEST_F(MembershipChangeTest, GetConfig_InitiallyContainsSelfAndPeers) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  // Can get config before start
  // Note: ParseNodeId extracts port number as ID (8001, 8002, 8003)
  auto cluster_config = node_->GetConfig();
  EXPECT_EQ(cluster_config.nodes.size(), 3);
  EXPECT_TRUE(cluster_config.Contains(8001));  // self (port 8001)
  EXPECT_TRUE(cluster_config.Contains(8002));  // peer (port 8002)
  EXPECT_TRUE(cluster_config.Contains(8003));  // peer (port 8003)
  EXPECT_EQ(cluster_config.GetMajority(), 2);  // 3 nodes -> majority = 2
}

TEST_F(MembershipChangeTest, AddNode_RejectedIfNotLeader) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // As follower, AddNode should fail
  auto status = node_->AddNode(4, "127.0.0.1:9004");
  EXPECT_FALSE(status.ok());
}

TEST_F(MembershipChangeTest, RemoveNode_RejectedIfNotLeader) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // As follower, RemoveNode should fail
  auto status = node_->RemoveNode(2);
  EXPECT_FALSE(status.ok());
}

TEST_F(MembershipChangeTest, AddNode_RejectedIfAlreadyInCluster) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Cannot add node that already exists
  auto status = node_->AddNode(2, "127.0.0.1:9002");
  EXPECT_FALSE(status.ok());
}

TEST_F(MembershipChangeTest, RemoveNode_RejectedIfNotInCluster) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Cannot remove node that doesn't exist
  auto status = node_->RemoveNode(99);
  EXPECT_FALSE(status.ok());
}

// Note: Testing successful membership change requires:
// 1. Becoming leader (need vote responses)
// 2. Log replication to apply config change
// These require more complex test setup.
// See integration tests for complete membership change testing.
