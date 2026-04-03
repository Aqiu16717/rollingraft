#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "mock/mock_persister.h"
#include "rollingraft/log_persister.h"

using namespace rollingraft;

class LogPersisterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_persister_ = std::make_unique<MockPersister>();
    mock_persister_ptr_ = mock_persister_.get();

    LogPersistenceConfig config;
    config.batch_size = 10;
    config.batch_interval_ms = 50;

    persister_ =
        std::make_unique<LogPersister>(std::move(mock_persister_), config);
  }

  void TearDown() override {
    // LogPersister destructor will call Stop() automatically
    persister_.reset();
  }

  RaftLogEntry MakeEntry(uint64_t index, uint64_t term,
                         const std::string& data) {
    RaftLogEntry entry;
    entry.index_ = index;
    entry.term_ = term;
    entry.data_ = data;
    return entry;
  }

  std::unique_ptr<MockPersister> mock_persister_;
  MockPersister* mock_persister_ptr_;
  std::unique_ptr<LogPersister> persister_;
};

TEST_F(LogPersisterTest, BasicStartStop) {
  EXPECT_NO_THROW(persister_->Start());
  EXPECT_NO_THROW(persister_->Stop());
}

TEST_F(LogPersisterTest, AppendIncreasesPendingCount) {
  persister_->Start();

  EXPECT_EQ(persister_->GetPendingCount(), 0);

  persister_->Append(MakeEntry(1, 1, "cmd1"));
  EXPECT_EQ(persister_->GetPendingCount(), 1);

  persister_->Append(MakeEntry(2, 1, "cmd2"));
  EXPECT_EQ(persister_->GetPendingCount(), 2);
}

TEST_F(LogPersisterTest, BatchFlushReducesIO) {
  persister_->Start();

  // Append 25 entries (batch_size is 10)
  for (int i = 1; i <= 25; ++i) {
    persister_->Append(MakeEntry(i, 1, "cmd" + std::to_string(i)));
  }

  // Force flush - this flushes all pending entries in one operation
  persister_->FlushSync();

  // Verify all entries are persisted
  EXPECT_EQ(mock_persister_ptr_->EntryCount(), 25);
  // Write count should be minimal (ideally 1-3 for 25 entries)
  EXPECT_LE(mock_persister_ptr_->GetWriteCount(), 3);
}

TEST_F(LogPersisterTest, RestoreReturnsEntries) {
  // Pre-populate mock persister
  std::vector<RaftLogEntry> entries;
  entries.push_back(MakeEntry(1, 1, "cmd1"));
  entries.push_back(MakeEntry(2, 1, "cmd2"));
  entries.push_back(MakeEntry(3, 1, "cmd3"));
  mock_persister_ptr_->AppendEntries(entries);

  // Restore from index 1
  auto restored = persister_->Restore(1);

  EXPECT_EQ(restored.size(), 3);
  EXPECT_EQ(restored[0].index_, 1);
  EXPECT_EQ(restored[2].index_, 3);
}

TEST_F(LogPersisterTest, FlushSyncWaitsForEmptyBuffer) {
  persister_->Start();

  // Append entries
  for (int i = 1; i <= 5; ++i) {
    persister_->Append(MakeEntry(i, 1, "cmd"));
  }

  EXPECT_GT(persister_->GetPendingCount(), 0);

  // Flush should block until buffer is empty
  auto status = persister_->FlushSync();

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(persister_->GetPendingCount(), 0);
  EXPECT_EQ(mock_persister_ptr_->EntryCount(), 5);
}

TEST_F(LogPersisterTest, HealthyByDefault) {
  EXPECT_TRUE(persister_->IsHealthy());
}

TEST_F(LogPersisterTest, UnhealthyAfterFailure) {
  persister_->Start();

  // Inject failure
  mock_persister_ptr_->InjectFailure("disk full");

  // Try to append and flush
  persister_->Append(MakeEntry(1, 1, "cmd"));
  persister_->FlushSync();

  // Should become unhealthy
  EXPECT_FALSE(persister_->IsHealthy());
  // Error message contains the original error
  EXPECT_NE(persister_->GetLastError().find("disk full"), std::string::npos);
}
