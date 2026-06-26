#include "rollingraft/raft_node.h"

#include "mock/mock_persister.h"
#include "mock/mock_state_machine.h"
#include "test_port.h"
#include <gtest/gtest.h>

using namespace rollingraft;

/**
 * PreVote and CheckQuorum unit tests.
 */

class PreVoteCheckQuorumTest : public ::testing::Test {
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

TEST_F(PreVoteCheckQuorumTest, InitialState_IsFollower) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  EXPECT_EQ(node_->GetRole(), RaftNodeRole::FOLLOWER);
  EXPECT_FALSE(node_->IsLeader());
}

TEST_F(PreVoteCheckQuorumTest, Propose_BeforeStart_ReturnsError) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  auto status = node_->Propose("cmd", [](const ApplyResult&) {});
  EXPECT_FALSE(status.ok());
}

TEST_F(PreVoteCheckQuorumTest, ReadIndex_BeforeStart_ReturnsError) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);

  auto status = node_->ReadIndex([]() {});
  EXPECT_FALSE(status.ok());
}

TEST_F(PreVoteCheckQuorumTest, AddNode_AsFollower_ReturnsNotLeader) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  auto status = node_->AddNode(4, "127.0.0.1:9004");
  EXPECT_FALSE(status.ok());
}
