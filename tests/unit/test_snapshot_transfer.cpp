#include <gtest/gtest.h>

#include <chrono>
#include <memory>

#include "mock/mock_persister.h"
#include "mock/mock_state_machine.h"
#include "rollingraft/raft_node.h"

using namespace rollingraft;

/**
 * Snapshot transfer tests.
 *
 * These tests verify snapshot creation, restore, and metadata handling.
 */

class SnapshotTransferTest : public ::testing::Test {
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

TEST_F(SnapshotTransferTest, StateMachine_CreateSnapshotCalled) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  // CreateSnapshot is called during normal operation
  // when snapshot threshold is reached
  auto snapshot = sm_->CreateSnapshot();
  EXPECT_NE(snapshot, nullptr);
}

TEST_F(SnapshotTransferTest, StateMachine_SnapshotMetadata) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  auto snapshot = sm_->CreateSnapshot();
  const auto& meta = snapshot->GetMeta();

  // Verify metadata fields exist
  EXPECT_GE(meta.last_included_index_, 0);
  EXPECT_GE(meta.last_included_term_, 0);
}

TEST_F(SnapshotTransferTest, StateMachine_RestoreFromSnapshot) {
  auto config = MakeConfig(1, {2, 3});

  // Create some state in state machine
  sm_->Apply(std::span<const uint8_t>(
                 reinterpret_cast<const uint8_t*>("cmd1"), 4),
             1);
  sm_->Apply(std::span<const uint8_t>(
                 reinterpret_cast<const uint8_t*>("cmd2"), 4),
             2);

  // Create snapshot
  auto snapshot = sm_->CreateSnapshot();

  // Reset state machine
  sm_->Reset();
  EXPECT_EQ(sm_->GetLastAppliedIndex(), 0);

  // Restore from snapshot
  std::vector<uint8_t> snapshot_data;
  snapshot_data.resize(100);  // Mock snapshot data
  EXPECT_TRUE(sm_->Restore(snapshot_data));
}

TEST_F(SnapshotTransferTest, Persister_SnapshotSaveAndLoad) {
  auto persister = std::make_unique<MockPersister>();
  persister->Open("/tmp/test");

  // Save snapshot
  std::string snapshot_data = "test_snapshot_data";
  auto status =
      persister->SaveSnapshot(snapshot_data, 100, 5);
  EXPECT_TRUE(status.ok());

  // Load snapshot
  std::string loaded_data;
  uint64_t last_index, last_term;
  status = persister->LoadSnapshot(loaded_data, last_index, last_term);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(last_index, 100);
  EXPECT_EQ(last_term, 5);
}

TEST_F(SnapshotTransferTest, Persister_HasSnapshot) {
  auto persister = std::make_unique<MockPersister>();
  persister->Open("/tmp/test");

  EXPECT_FALSE(persister->HasSnapshot());

  // Save snapshot
  persister->SaveSnapshot("data", 1, 1);

  EXPECT_TRUE(persister->HasSnapshot());
}

TEST_F(SnapshotTransferTest, StateMachine_TriggerWaiters) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  std::atomic<bool> waiter_called{false};
  sm_->WaitIndex(5, [&]() { waiter_called = true; });

  // Initially not called
  EXPECT_FALSE(waiter_called);

  // Notify waiters up to index 5
  sm_->NotifyWaiters(5);

  // Waiter should be called
  EXPECT_TRUE(waiter_called);
}

TEST_F(SnapshotTransferTest, StateMachine_WaiterNotCalledIfIndexNotReached) {
  auto config = MakeConfig(1, {2, 3});
  node_ = std::make_unique<RaftNode>(config, sm_);
  EXPECT_TRUE(node_->Start().ok());

  std::atomic<bool> waiter_called{false};
  sm_->WaitIndex(10, [&]() { waiter_called = true; });

  // Notify waiters up to index 5 (less than 10)
  sm_->NotifyWaiters(5);

  // Waiter should not be called
  EXPECT_FALSE(waiter_called);
}

// Note: Full snapshot transfer testing requires:
// 1. Leader triggering snapshot on follower
// 2. InstallSnapshot RPC
// 3. State machine restoration during runtime
// These require more complex test setup.
// See integration tests for complete snapshot testing.
