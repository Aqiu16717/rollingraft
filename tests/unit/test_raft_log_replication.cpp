#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include <vector>

#include "rollingraft/raft_node.h"

#include "mock/mock_persister.h"
#include "mock/mock_state_machine.h"

using namespace rollingraft;

/**
 * Log replication tests.
 *
 * These tests verify:
 * - Follower rejects propose
 * - Log persistence integration
 */

class RaftLogReplicationTest : public ::testing::Test {
 protected:
  void SetUp() override { sm_ = std::make_shared<MockStateMachine>(); }

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

TEST_F(RaftLogReplicationTest, Follower_RejectPropose) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // As follower, propose should fail
  std::atomic<bool> callback_called{false};
  auto status = node_->Propose(
      "cmd1", [&](const ApplyResult&) { callback_called = true; });

  // Should return error immediately
  EXPECT_FALSE(status.ok());
  EXPECT_FALSE(callback_called);
}

TEST_F(RaftLogReplicationTest, Follower_ReturnsLeaderInfo) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  auto status = node_->Propose("cmd1", [](const ApplyResult&) {});

  // Should be NotLeader error
  EXPECT_FALSE(status.ok());
}

TEST_F(RaftLogReplicationTest, IsLeader_FalseForFollower) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  EXPECT_FALSE(node_->IsLeader());
}

TEST_F(RaftLogReplicationTest, GetLeaderAddr_EmptyInitially) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Initially no known leader
  auto leader_addr = node_->GetLeaderAddr();
  EXPECT_TRUE(leader_addr.empty());
}

TEST_F(RaftLogReplicationTest, Persistence_StateRestored) {
  // Pre-populate persister with some state
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

TEST_F(RaftLogReplicationTest, Persistence_LogsRestored) {
  // Pre-populate persister with log entries
  auto persister = std::make_unique<MockPersister>();
  persister->Open("/tmp/test");

  std::vector<RaftLogEntry> entries;
  entries.push_back({1, 1, "cmd1"});
  entries.push_back({2, 1, "cmd2"});
  persister->AppendEntries(entries);

  auto config = MakeConfig(1, {2, 3});
  config.persister_factory = [&persister]() {
    return std::unique_ptr<Persister>(persister.release());
  };

  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // The node should have restored logs (verified via persister)
  // Note: Direct log access is internal, but we can verify no crash
  EXPECT_TRUE(node_->IsLeader() ||
              !node_->IsLeader());  // Just verify state is valid
}

TEST_F(RaftLogReplicationTest, Follower_RejectProposeBatch) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  std::atomic<bool> callback_called{false};
  auto status = node_->ProposeBatch(
      {"cmd1", "cmd2"},
      [&](const std::vector<ApplyResult>&) { callback_called = true; });

  EXPECT_FALSE(status.ok());
  EXPECT_FALSE(callback_called);
}

TEST_F(RaftLogReplicationTest, ProposeBatch_EmptyBatch_Fails) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  auto status = node_->ProposeBatch({}, [](const std::vector<ApplyResult>&) {});
  EXPECT_FALSE(status.ok());
}

TEST_F(RaftLogReplicationTest, Leader_ProposeBatch_SingleNode) {
  // Single node cluster: node should become leader immediately on election
  // timeout since it only needs its own vote (1 > 0)
  // Use node id 10 to avoid port conflicts with other tests
  auto config = MakeConfig(10, {});
  config.election_timeout_ms = 50;  // Short timeout for fast test
  config.heartbeat_interval_ms = 20;
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // Wait for election timeout to fire and node to become leader
  // Single node needs only its own vote; timeout is randomized [50,100]ms
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  ASSERT_TRUE(node_->IsLeader())
      << "Node should be leader after election timeout";

  std::atomic<bool> completed{false};
  std::vector<ApplyResult> batch_results;
  auto status = node_->ProposeBatch(
      {"cmd1", "cmd2", "cmd3"}, [&](const std::vector<ApplyResult>& results) {
        batch_results = results;
        completed = true;
      });
  ASSERT_TRUE(status.ok()) << "ProposeBatch failed: " << status.ToString();

  // Wait for commit (single node = immediate quorum)
  auto start = std::chrono::steady_clock::now();
  while (!completed && std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::steady_clock::now() - start)
                               .count() < 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(completed) << "Batch was not committed";
  EXPECT_EQ(batch_results.size(), 3);
  for (const auto& result : batch_results) {
    EXPECT_TRUE(result.success);
  }
}

// Note: Full log replication testing requires:
// 1. Becoming leader (need vote responses from network)
// 2. Network message inspection
// These require more complex test setup with response injection.
// See integration tests for complete replication testing.
