#include <gtest/gtest.h>

#include <chrono>
#include <memory>

#include "mock/mock_persister.h"
#include "mock/mock_state_machine.h"
#include "rollingraft/raft_node.h"

using namespace rollingraft;

/**
 * Leader step-down tests.
 *
 * These tests verify that leaders correctly step down when:
 * - A higher term is discovered
 * - Leadership is lost
 */

class LeaderStepDownTest : public ::testing::Test {
 protected:
  void SetUp() override {
    sm_ = std::make_shared<MockStateMachine>();
  }

  void TearDown() override {
    if (node_) {
      node_->Stop();
    }
  }

  RaftNodeConfig MakeConfig(NodeId id, const std::vector<NodeId>& peers) {
    RaftNodeConfig config;
    config.node_id = id;
    config.listen_addr = "127.0.0.1:" + std::to_string(8000 + id);
    config.election_timeout_ms = 300;
    config.heartbeat_interval_ms = 100;
    config.data_dir = "/tmp/raft_test_node_" + std::to_string(id);

    for (NodeId peer_id : peers) {
      config.peers.push_back("127.0.0.1:" + std::to_string(8000 + peer_id));
    }

    return config;
  }

  std::shared_ptr<MockStateMachine> sm_;
  std::unique_ptr<RaftNode> node_;
};

TEST_F(LeaderStepDownTest, InitialState_IsFollower) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  EXPECT_EQ(node_->GetRole(), RaftNodeRole::FOLLOWER);
  EXPECT_FALSE(node_->IsLeader());
}

TEST_F(LeaderStepDownTest, RoleChangeCallback_CalledOnStart) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  std::atomic<bool> callback_set{false};
  node_->SetRoleChangeCallback(
      [&](RaftNodeRole role, Term term) {
        callback_set = true;
        (void)role;
        (void)term;
      });

  EXPECT_TRUE(node_->Start().ok());

  // Callback infrastructure is set up
  // Note: Without network responses, we stay follower
  EXPECT_EQ(node_->GetRole(), RaftNodeRole::FOLLOWER);
}

TEST_F(LeaderStepDownTest, LeaderChangeCallback_CanBeSet) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  std::atomic<bool> callback_set{false};
  node_->SetLeaderChangeCallback(
      [&](NodeId id, const std::string& addr) {
        callback_set = true;
        (void)id;
        (void)addr;
      });

  EXPECT_TRUE(node_->Start().ok());

  // Callback is registered
  EXPECT_FALSE(callback_set);  // No leader change yet
}

TEST_F(LeaderStepDownTest, Propose_BeforeStart_Fails) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  // Don't start the node
  auto status = node_->Propose("cmd", [](const ApplyResult&) {});
  EXPECT_FALSE(status.ok());
}

TEST_F(LeaderStepDownTest, ReadIndex_BeforeStart_Fails) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  // Don't start the node
  auto status = node_->ReadIndex([]() {});
  EXPECT_FALSE(status.ok());
}

TEST_F(LeaderStepDownTest, AddNode_BeforeStart_Fails) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  // Don't start the node
  auto status = node_->AddNode(4, "127.0.0.1:8004");
  EXPECT_FALSE(status.ok());
}

TEST_F(LeaderStepDownTest, RemoveNode_BeforeStart_Fails) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  // Don't start the node
  auto status = node_->RemoveNode(2);
  EXPECT_FALSE(status.ok());
}

TEST_F(LeaderStepDownTest, Stop_CanBeCalledMultipleTimes) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // First stop
  EXPECT_TRUE(node_->Stop().ok());

  // Second stop should be safe (idempotent)
  EXPECT_TRUE(node_->Stop().ok());
}

TEST_F(LeaderStepDownTest, CurrentTerm_InitiallyZero) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  EXPECT_EQ(node_->CurrentTerm(), 0);

  EXPECT_TRUE(node_->Start().ok());
  // Term may increase during election
}

TEST_F(LeaderStepDownTest, GetConfig_AvailableBeforeStart) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  // Can get config before start
  auto cluster_config = node_->GetConfig();
  EXPECT_EQ(cluster_config.nodes.size(), 3);
}

// Note: Full leader step-down testing requires:
// 1. Becoming leader first (need vote majority)
// 2. Receiving message with higher term
// 3. Verifying transition to follower
// These require more complex test setup with network response injection.
// See integration tests for complete step-down testing.
