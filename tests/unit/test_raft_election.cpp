#include <gtest/gtest.h>

#include <chrono>
#include <memory>

#include "mock/mock_persister.h"
#include "mock/mock_state_machine.h"
#include "rollingraft/raft_node.h"

using namespace rollingraft;

/**
 * Raft election tests.
 * 
 * These tests verify basic Raft node behavior.
 * Note: Full election testing requires network response injection.
 */

class RaftElectionTest : public ::testing::Test {
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

TEST_F(RaftElectionTest, InitialState_IsFollower) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  EXPECT_EQ(node_->GetRole(), RaftNodeRole::FOLLOWER);
  EXPECT_FALSE(node_->IsLeader());
  EXPECT_EQ(node_->CurrentTerm(), 0);
}

TEST_F(RaftElectionTest, Propose_BeforeStart_ReturnsError) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  auto status = node_->Propose("cmd", [](const ApplyResult&) {});
  EXPECT_FALSE(status.ok());
}

TEST_F(RaftElectionTest, Propose_AsFollower_ReturnsNotLeader) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Before election timeout, we're still follower
  auto status = node_->Propose("cmd", [](const ApplyResult&) {});
  EXPECT_FALSE(status.ok());
}

TEST_F(RaftElectionTest, StartStop_Success) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  EXPECT_TRUE(node_->Start().ok());
  EXPECT_TRUE(node_->Stop().ok());
}

TEST_F(RaftElectionTest, IsLeader_FalseInitially) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  EXPECT_FALSE(node_->IsLeader());
  EXPECT_EQ(node_->GetRole(), RaftNodeRole::FOLLOWER);
}

TEST_F(RaftElectionTest, GetLeaderAddr_EmptyInitially) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Initially no known leader
  auto leader_addr = node_->GetLeaderAddr();
  EXPECT_TRUE(leader_addr.empty());
}

TEST_F(RaftElectionTest, Persistence_StateRestored) {
  // Create a persister with existing state
  auto persister = std::make_unique<MockPersister>();
  persister->Open("/tmp/test");
  persister->SaveState({5, 2});  // term=5, voted_for=2

  auto config = MakeConfig(1, {2, 3});
  config.persister_factory = [&persister]() {
    return std::unique_ptr<Persister>(persister.release());
  };

  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Term should be restored
  EXPECT_EQ(node_->CurrentTerm(), 5);
}

TEST_F(RaftElectionTest, RoleChangeCallback_CanBeSet) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  bool called = false;
  node_->SetRoleChangeCallback([&](RaftNodeRole role, Term term) {
    called = true;
    (void)role;
    (void)term;
  });

  EXPECT_TRUE(node_->Start().ok());
  // Callback infrastructure is in place
  EXPECT_FALSE(called);  // No transition happened yet
}
