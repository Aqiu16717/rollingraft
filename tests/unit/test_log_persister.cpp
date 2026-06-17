#include <chrono>
#include <gtest/gtest.h>
#include <thread>

#include "rollingraft/log_persister.h"

#include "mock/mock_persister.h"

using namespace rollingraft;

class LogPersisterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_persister_ = std::make_unique<MockPersister>();
    mock_persister_ptr_ = mock_persister_.get();

    LogPersistenceConfig config;
    config.batch_size = 10;
    config.batch_interval_ms = 50;
    config.sync_policy = LogPersistenceConfig::SyncPolicy::kSyncEveryWrite;
    config.data_dir = "/tmp";

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

TEST_F(LogPersisterTest, CallbackFiresAfterFlush) {
  persister_->Start();

  bool callback_fired = false;
  Status callback_status;

  persister_->Append(MakeEntry(1, 1, "cmd"),
                     [&callback_fired, &callback_status](Status s) {
                       callback_fired = true;
                       callback_status = s;
                     });

  // Callback should not fire immediately
  EXPECT_FALSE(callback_fired);

  // Force flush
  persister_->FlushSync();

  // Callback should fire with OK
  EXPECT_TRUE(callback_fired);
  EXPECT_TRUE(callback_status.ok());
}

TEST_F(LogPersisterTest, CallbackFiresOnFailure) {
  persister_->Start();

  // Inject failure
  mock_persister_ptr_->InjectFailure("disk full");

  bool callback_fired = false;
  Status callback_status;

  persister_->Append(MakeEntry(1, 1, "cmd"),
                     [&callback_fired, &callback_status](Status s) {
                       callback_fired = true;
                       callback_status = s;
                     });

  // Force flush (will fail)
  persister_->FlushSync();

  // Callback should fire with error
  EXPECT_TRUE(callback_fired);
  EXPECT_FALSE(callback_status.ok());
}

TEST_F(LogPersisterTest, AppendSyncWaitsForFlush) {
  persister_->Start();

  auto status = persister_->AppendSync(MakeEntry(1, 1, "cmd"));

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(mock_persister_ptr_->EntryCount(), 1);
}

TEST_F(LogPersisterTest, AppendSyncReturnsErrorOnFailure) {
  persister_->Start();

  mock_persister_ptr_->InjectFailure("disk full");

  auto status = persister_->AppendSync(MakeEntry(1, 1, "cmd"));

  EXPECT_FALSE(status.ok());
}

TEST_F(LogPersisterTest, RecoversWhenDiskSpaceAvailable) {
  persister_->Start();

  // Inject failure and trigger unhealthy state
  mock_persister_ptr_->InjectFailure("disk full");
  persister_->Append(MakeEntry(1, 1, "cmd"));
  persister_->FlushSync();
  EXPECT_FALSE(persister_->IsHealthy());

  // Clear the underlying failure
  mock_persister_ptr_->ClearFailure();

  // Next append should recover automatically because /tmp has space
  persister_->Append(MakeEntry(2, 1, "cmd"));
  auto status = persister_->FlushSync();

  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(persister_->IsHealthy());
  EXPECT_EQ(mock_persister_ptr_->EntryCount(), 2);
}

TEST_F(LogPersisterTest, CheckDiskSpaceFailsWhenLimitTooHigh) {
  // Set an impossibly high disk space requirement
  LogPersistenceConfig config;
  config.batch_size = 10;
  config.batch_interval_ms = 50;
  config.data_dir = "/tmp";
  config.min_disk_space_bytes = UINT64_MAX;

  auto mock = std::make_unique<MockPersister>();
  auto p = std::make_unique<LogPersister>(std::move(mock), config);
  p->Start();

  p->Append(MakeEntry(1, 1, "cmd"));
  auto status = p->FlushSync();

  EXPECT_FALSE(status.ok());
  EXPECT_FALSE(p->IsHealthy());

  p->Stop();
}

TEST_F(LogPersisterTest, EntryContainsChecksum) {
  // Verify that entries have checksum calculated
  RaftLogEntry entry = MakeEntry(1, 1, "test data");

  // Initially checksum should be 0 (not calculated yet)
  EXPECT_EQ(entry.checksum_, 0);

  // After serialization in LevelDBPersister, checksum will be set
  // This test verifies the field exists and can be set
  entry.checksum_ = 0xDEADBEEF;
  EXPECT_EQ(entry.checksum_, 0xDEADBEEF);
}

// ---------------------------------------------------------------------------
// Group commit tests
// ---------------------------------------------------------------------------

class LogPersisterGroupCommitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_persister_ = std::make_unique<MockPersister>();
    mock_persister_ptr_ = mock_persister_.get();

    LogPersistenceConfig config;
    config.batch_size = 10;
    config.batch_interval_ms = 50;
    config.sync_policy = LogPersistenceConfig::SyncPolicy::kSyncAdaptive;
    config.group_commit_interval_ms = 100;
    config.group_commit_max_entries = 100;
    config.data_dir = "/tmp";

    persister_ =
        std::make_unique<LogPersister>(std::move(mock_persister_), config);
  }

  void TearDown() override { persister_.reset(); }

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

TEST_F(LogPersisterGroupCommitTest, CallbackFiresAfterSync) {
  persister_->Start();

  bool callback_fired = false;
  Status callback_status;

  persister_->Append(MakeEntry(1, 1, "cmd"),
                     [&callback_fired, &callback_status](Status s) {
                       callback_fired = true;
                       callback_status = s;
                     });

  // Callback should not fire immediately after flush.
  persister_->FlushSync();
  EXPECT_FALSE(callback_fired);
  EXPECT_EQ(mock_persister_ptr_->EntryCount(), 1);

  // Explicit sync should trigger the callback.
  auto status = persister_->Sync();
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(callback_fired);
  EXPECT_TRUE(callback_status.ok());
  EXPECT_EQ(mock_persister_ptr_->GetSyncCount(), 1);
}

TEST_F(LogPersisterGroupCommitTest, SyncBatchesMultipleEntries) {
  persister_->Start();

  for (int i = 1; i <= 5; ++i) {
    persister_->Append(MakeEntry(i, 1, "cmd"));
  }

  persister_->FlushSync();
  EXPECT_EQ(mock_persister_ptr_->EntryCount(), 5);
  EXPECT_EQ(mock_persister_ptr_->GetSyncCount(), 0);

  persister_->Sync();
  EXPECT_EQ(mock_persister_ptr_->GetSyncCount(), 1);
}

TEST_F(LogPersisterGroupCommitTest, CallbackFiresOnSyncFailure) {
  persister_->Start();

  bool callback_fired = false;
  Status callback_status;

  persister_->Append(MakeEntry(1, 1, "cmd"),
                     [&callback_fired, &callback_status](Status s) {
                       callback_fired = true;
                       callback_status = s;
                     });

  persister_->FlushSync();
  mock_persister_ptr_->InjectFailure("fsync failed");

  auto status = persister_->Sync();
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(callback_fired);
  EXPECT_FALSE(callback_status.ok());
  EXPECT_FALSE(persister_->IsHealthy());
}

TEST_F(LogPersisterGroupCommitTest, SyncByBatchSizeThreshold) {
  LogPersistenceConfig config;
  config.batch_size = 10;
  config.batch_interval_ms = 1000;  // Long interval
  config.sync_policy = LogPersistenceConfig::SyncPolicy::kSyncByBatchSize;
  config.group_commit_max_entries = 5;
  config.data_dir = "/tmp";

  auto mock = std::make_unique<MockPersister>();
  auto* mock_ptr = mock.get();
  auto p = std::make_unique<LogPersister>(std::move(mock), config);
  p->Start();

  // Append 5 entries and flush; this should trigger a sync.
  for (int i = 1; i <= 5; ++i) {
    p->Append(MakeEntry(i, 1, "cmd"));
  }

  // Wait for the background sync thread to sync.
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (mock_ptr->GetSyncCount() == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_EQ(mock_ptr->GetSyncCount(), 1);
  p->Stop();
}
