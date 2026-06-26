/**
 * @file test_hybrid_persister.cpp
 * @brief HybridPersister integration tests
 *
 * Tests WAL + StatePersister consistency and facade behavior.
 */

#include <filesystem>

#include "rollingraft/persister.h"

#include <gtest/gtest.h>

using namespace rollingraft;

class HybridPersisterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/rollingraft_hybrid_test_" + std::to_string(getpid()) + "_" +
                std::to_string(counter_++);
    std::filesystem::create_directories(test_dir_);

    persister_ = CreateLevelDBPersister();
    ASSERT_TRUE(persister_->Open(test_dir_).ok());
  }

  void TearDown() override {
    persister_->Close();
    std::filesystem::remove_all(test_dir_);
  }

  RaftLogEntry MakeEntry(uint64_t index, uint64_t term, const std::string& data) {
    RaftLogEntry entry;
    entry.index_ = index;
    entry.term_ = term;
    entry.data_ = data;
    return entry;
  }

  static int counter_;
  std::string test_dir_;
  std::unique_ptr<Persister> persister_;
};

int HybridPersisterTest::counter_ = 0;

TEST_F(HybridPersisterTest, ConsistentAfterAppendAndSnapshot) {
  // Append log entries 1..10
  std::vector<RaftLogEntry> entries;
  for (uint64_t i = 1; i <= 10; ++i) {
    entries.push_back(MakeEntry(i, 1, "data" + std::to_string(i)));
  }
  ASSERT_TRUE(persister_->AppendEntries(entries).ok());
  ASSERT_TRUE(persister_->Sync().ok());

  // Save snapshot at index 7
  std::string snapshot_data = "snapshot_at_index_7";
  ASSERT_TRUE(persister_->SaveSnapshot(snapshot_data, 7, 1).ok());

  // Verify last log index >= snapshot last index
  auto [last_log_index, last_log_term] = persister_->GetLastLogInfo();
  EXPECT_GE(last_log_index, 7);

  // Verify snapshot metadata
  std::string loaded_data;
  uint64_t loaded_index = 0, loaded_term = 0;
  ASSERT_TRUE(persister_->LoadSnapshot(loaded_data, loaded_index, loaded_term).ok());
  EXPECT_EQ(loaded_data, snapshot_data);
  EXPECT_EQ(loaded_index, 7);
  EXPECT_EQ(loaded_term, 1);
}

TEST_F(HybridPersisterTest, ReopenPreservesWALAndStateConsistency) {
  // Append entries and save state
  std::vector<RaftLogEntry> entries;
  for (uint64_t i = 1; i <= 5; ++i) {
    entries.push_back(MakeEntry(i, 2, "cmd" + std::to_string(i)));
  }
  ASSERT_TRUE(persister_->AppendEntries(entries).ok());
  ASSERT_TRUE(persister_->Sync().ok());

  PersistentState state;
  state.current_term = 42;
  state.voted_for = 7;
  ASSERT_TRUE(persister_->SaveState(state).ok());

  // Save snapshot at index 3
  ASSERT_TRUE(persister_->SaveSnapshot("snap", 3, 2).ok());

  // Reopen
  persister_->Close();
  persister_ = CreateLevelDBPersister();
  ASSERT_TRUE(persister_->Open(test_dir_).ok());

  // Verify state
  PersistentState loaded_state;
  ASSERT_TRUE(persister_->LoadState(loaded_state).ok());
  EXPECT_EQ(loaded_state.current_term, 42);
  EXPECT_EQ(loaded_state.voted_for, 7);

  // Verify log entries
  std::vector<RaftLogEntry> result;
  ASSERT_TRUE(persister_->GetEntries(1, 6, &result).ok());
  EXPECT_EQ(result.size(), 5);

  // Verify consistency
  auto [last_log_index, last_log_term] = persister_->GetLastLogInfo();
  EXPECT_GE(last_log_index, 3);

  std::string snap_data;
  uint64_t snap_index = 0, snap_term = 0;
  ASSERT_TRUE(persister_->LoadSnapshot(snap_data, snap_index, snap_term).ok());
  EXPECT_EQ(snap_index, 3);
  EXPECT_LE(snap_index, last_log_index);
}

TEST_F(HybridPersisterTest, TruncateSuffixMaintainsSnapshotConsistency) {
  // Append entries 1..10, snapshot at 8
  std::vector<RaftLogEntry> entries;
  for (uint64_t i = 1; i <= 10; ++i) {
    entries.push_back(MakeEntry(i, 1, "x"));
  }
  ASSERT_TRUE(persister_->AppendEntries(entries).ok());
  ASSERT_TRUE(persister_->SaveSnapshot("snap", 8, 1).ok());

  // Truncate suffix from 12 (no-op, beyond last index)
  ASSERT_TRUE(persister_->TruncateSuffix(12).ok());
  auto [last_log_index, _] = persister_->GetLastLogInfo();
  EXPECT_EQ(last_log_index, 10);

  // Truncate suffix from 6
  ASSERT_TRUE(persister_->TruncateSuffix(6).ok());
  auto [last_log_index2, __] = persister_->GetLastLogInfo();
  EXPECT_EQ(last_log_index2, 5);

  // Snapshot index (8) is now beyond last log index - this is architecturally
  // allowed; the raft node must decide how to handle it. We just verify the
  // persister reports the state correctly.
  std::string snap_data;
  uint64_t snap_index = 0, snap_term = 0;
  ASSERT_TRUE(persister_->LoadSnapshot(snap_data, snap_index, snap_term).ok());
  EXPECT_EQ(snap_index, 8);
}
