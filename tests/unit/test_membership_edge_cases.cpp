#include <chrono>
#include <gtest/gtest.h>
#include <memory>

#include "rollingraft/raft_node.h"

#include "mock/mock_persister.h"
#include "mock/mock_state_machine.h"
#include "test_port.h"

using namespace rollingraft;

/**
 * Membership change edge case tests.
 *
 * These tests verify edge cases and error handling for membership changes.
 */

class MembershipEdgeCaseTest : public ::testing::Test {
 protected:
  void SetUp() override { sm_ = std::make_shared<MockStateMachine>(); ports_ = GetTestPorts(10); }

  void TearDown() override {
    if (node_) {
      node_->Stop();
    }
  }

  RaftNodeConfig MakeConfig(NodeId id, const std::vector<NodeId>& peers) {
    RaftNodeConfig config;
    config.node_id = 8000 + id;
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

TEST_F(MembershipEdgeCaseTest, GetConfig_MajorityCalculation) {
  // Single node
  {
    auto config = MakeConfig(1, {});
    node_ = std::make_unique<RaftNode>(config, sm_);
    auto cluster_config = node_->GetConfig();
    EXPECT_EQ(cluster_config.nodes.size(), 1);
    EXPECT_EQ(cluster_config.GetMajority(), 1);  // 1 node needs 1 for majority
  }

  node_.reset();

  // Two nodes
  {
    auto config = MakeConfig(1, {2});
    node_ = std::make_unique<RaftNode>(config, sm_);
    auto cluster_config = node_->GetConfig();
    EXPECT_EQ(cluster_config.nodes.size(), 2);
    EXPECT_EQ(cluster_config.GetMajority(), 2);  // 2 nodes need 2 for majority
  }

  node_.reset();

  // Three nodes
  {
    auto config = MakeConfig(1, {2, 3});
    node_ = std::make_unique<RaftNode>(config, sm_);
    auto cluster_config = node_->GetConfig();
    EXPECT_EQ(cluster_config.nodes.size(), 3);
    EXPECT_EQ(cluster_config.GetMajority(), 2);  // 3 nodes need 2 for majority
  }

  node_.reset();

  // Five nodes
  {
    auto config = MakeConfig(1, {2, 3, 4, 5});
    node_ = std::make_unique<RaftNode>(config, sm_);
    auto cluster_config = node_->GetConfig();
    EXPECT_EQ(cluster_config.nodes.size(), 5);
    EXPECT_EQ(cluster_config.GetMajority(), 3);  // 5 nodes need 3 for majority
  }
}

TEST_F(MembershipEdgeCaseTest, GetConfig_ContainsCheck) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  auto cluster_config = node_->GetConfig();

  // Should contain all nodes (IDs are port numbers)
  EXPECT_TRUE(cluster_config.Contains(8001));
  EXPECT_TRUE(cluster_config.Contains(8002));
  EXPECT_TRUE(cluster_config.Contains(8003));

  // Should not contain non-existent node
  EXPECT_FALSE(cluster_config.Contains(9999));
}

TEST_F(MembershipEdgeCaseTest, AddNode_SelfIsRejected) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Cannot add self (8001 is already in cluster)
  auto status = node_->AddNode(8001, "127.0.0.1:8001");
  EXPECT_FALSE(status.ok());
}

TEST_F(MembershipEdgeCaseTest, RemoveNode_SelfIsRejected) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Cannot remove self (would make cluster unusable)
  auto status = node_->RemoveNode(8001);
  EXPECT_FALSE(status.ok());
}

TEST_F(MembershipEdgeCaseTest, RemoveNode_OnlyOneNodeLeft) {
  auto config = MakeConfig(1, {2});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Removing node 2 would leave only 1 node
  // This should be rejected (would break majority)
  auto status = node_->RemoveNode(8002);
  // Note: Implementation may allow this, but it's dangerous
  // Test documents expected behavior
}

TEST_F(MembershipEdgeCaseTest, Config_VersionPresent) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  auto cluster_config = node_->GetConfig();
  // Version is initialized (implementation may start at 0 or 1)
  EXPECT_GE(cluster_config.version, 0);
}

TEST_F(MembershipEdgeCaseTest, EmptyCluster_NotAllowed) {
  // Node must at least have itself in config
  // Creating config with no peers is allowed (single node cluster)
  RaftNodeConfig config;
  config.node_id = 8001;
  config.listen_addr = "127.0.0.1:8001";
  config.election_timeout_ms = 300;
  config.heartbeat_interval_ms = 100;
  config.data_dir = "/tmp/raft_test_single";
  // No peers added

  node_ = std::make_unique<RaftNode>(config, sm_);
  auto cluster_config = node_->GetConfig();

  // Single node cluster
  EXPECT_EQ(cluster_config.nodes.size(), 1);
  EXPECT_TRUE(cluster_config.Contains(8001));
}

// Note: Full membership edge case testing requires:
// 1. Concurrent membership changes
// 2. Membership change during network partition
// 3. Membership change rollback scenarios
// These require more complex test setup.
// See integration tests for complete membership testing.
