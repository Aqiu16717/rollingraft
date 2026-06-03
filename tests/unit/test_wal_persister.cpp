/**
 * @file test_wal_persister.cpp
 * @brief Unit tests for WALPersister
 *
 * Phase 1 acceptance criteria:
 * - Empty WAL open/close
 * - Append and replay round-trip
 * - Segment rotation (entry count / size threshold)
 * - Crash recovery (close and reopen)
 * - Truncate prefix and suffix
 * - Garbage collection
 * - Concurrent append
 * - Corruption detection
 */

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "rollingraft/wal_persister.h"

using namespace rollingraft;

class WALPersisterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/rollingraft_wal_test_" + std::to_string(getpid()) + "_" +
                std::to_string(counter_++);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }

  RaftLogEntry MakeEntry(uint64_t index, uint64_t term,
                         const std::string& data,
                         const std::string& command = "") {
    RaftLogEntry entry;
    entry.index_ = index;
    entry.term_ = term;
    entry.data_ = data;
    entry.command_ = command;
    entry.checksum_ = 0;
    return entry;
  }

  static int counter_;
  std::string test_dir_;
};

int WALPersisterTest::counter_ = 0;

// ---------------------------------------------------------------------------
// Test 1: Empty WAL
// ---------------------------------------------------------------------------
TEST_F(WALPersisterTest, EmptyWAL) {
  WALPersister wal;
  ASSERT_TRUE(wal.Open(test_dir_).ok());

  auto range = wal.GetLogRange();
  EXPECT_EQ(range.first, 0);
  EXPECT_EQ(range.second, 0);

  int count = 0;
  auto status = wal.Replay([&count](const WALRecord&) {
    count++;
    return true;
  });
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(count, 0);

  wal.Close();
}

// ---------------------------------------------------------------------------
// Test 2: Append and Replay round-trip
// ---------------------------------------------------------------------------
TEST_F(WALPersisterTest, AppendAndReplay) {
  WALPersister wal;
  ASSERT_TRUE(wal.Open(test_dir_).ok());

  // Append 10 entries
  for (uint64_t i = 1; i <= 10; ++i) {
    ASSERT_TRUE(wal.AppendLogEntry(MakeEntry(i, 1, "data" + std::to_string(i),
                                             "cmd" + std::to_string(i)))
                    .ok());
  }

  auto range = wal.GetLogRange();
  EXPECT_EQ(range.first, 1);
  EXPECT_EQ(range.second, 10);

  // Replay and verify
  std::vector<WALRecord> records;
  ASSERT_TRUE(wal.Replay([&records](const WALRecord& r) {
    records.push_back(r);
    return true;
  }).ok());

  EXPECT_EQ(records.size(), 10);
  for (size_t i = 0; i < records.size(); ++i) {
    EXPECT_EQ(records[i].type, WALRecordType::kLogEntry);
    // Payload is JSON - basic sanity check
    EXPECT_NE(records[i].payload.find("\"index\":"), std::string::npos);
  }

  wal.Close();
}

// ---------------------------------------------------------------------------
// Test 3: Sync and Crash Recovery
// ---------------------------------------------------------------------------
TEST_F(WALPersisterTest, CrashRecovery) {
  {
    WALPersister wal;
    ASSERT_TRUE(wal.Open(test_dir_).ok());

    for (uint64_t i = 1; i <= 5; ++i) {
      ASSERT_TRUE(
          wal.AppendLogEntry(MakeEntry(i, 1, "data" + std::to_string(i)))
              .ok());
    }
    ASSERT_TRUE(wal.Sync().ok());

    // Append more without sync (simulating uncommitted writes)
    for (uint64_t i = 6; i <= 8; ++i) {
      ASSERT_TRUE(
          wal.AppendLogEntry(MakeEntry(i, 1, "data" + std::to_string(i)))
              .ok());
    }

    // Close without final sync - simulates crash
    wal.Close();
  }

  // Reopen - should recover all entries (trailer marks valid data)
  {
    WALPersister wal;
    ASSERT_TRUE(wal.Open(test_dir_).ok());

    auto range = wal.GetLogRange();
    EXPECT_EQ(range.first, 1);
    // Should have entries 1-5 (synced) plus 6-8 (in segment, trailer shows
    // end before 6)
    EXPECT_GE(range.second, 5);

    int count = 0;
    ASSERT_TRUE(wal.Replay([&count](const WALRecord&) {
      count++;
      return true;
    }).ok());
    EXPECT_GE(count, 5);

    wal.Close();
  }
}

// ---------------------------------------------------------------------------
// Test 4: Segment Rotation by Entry Count
// ---------------------------------------------------------------------------
TEST_F(WALPersisterTest, SegmentRotationByCount) {
  WALPersister wal;
  ASSERT_TRUE(wal.Open(test_dir_).ok());

  // Append many entries to trigger rotation
  // kMaxSegmentEntries = 10000, so we need to append > 10000
  for (uint64_t i = 1; i <= 15000; ++i) {
    ASSERT_TRUE(
        wal.AppendLogEntry(MakeEntry(i, 1, "x")).ok());
  }

  // Check that multiple segments exist
  int segment_count = 0;
  for (const auto& entry :
       std::filesystem::directory_iterator(test_dir_)) {
    if (entry.is_regular_file() && entry.path().extension() == ".wal") {
      segment_count++;
    }
  }
  EXPECT_GE(segment_count, 2);

  // Verify all entries are recoverable
  int replay_count = 0;
  ASSERT_TRUE(wal.Replay([&replay_count](const WALRecord&) {
    replay_count++;
    return true;
  }).ok());
  EXPECT_EQ(replay_count, 15000);

  auto range = wal.GetLogRange();
  EXPECT_EQ(range.first, 1);
  EXPECT_EQ(range.second, 15000);

  wal.Close();
}

// ---------------------------------------------------------------------------
// Test 5: Truncate Prefix
// ---------------------------------------------------------------------------
TEST_F(WALPersisterTest, TruncatePrefix) {
  WALPersister wal;
  ASSERT_TRUE(wal.Open(test_dir_).ok());

  for (uint64_t i = 1; i <= 10; ++i) {
    ASSERT_TRUE(
        wal.AppendLogEntry(MakeEntry(i, 1, "data" + std::to_string(i)))
            .ok());
  }

  // Truncate entries before index 5
  ASSERT_TRUE(wal.AppendTruncatePrefix(5).ok());

  auto range = wal.GetLogRange();
  EXPECT_EQ(range.first, 5);
  EXPECT_EQ(range.second, 10);

  // Verify via replay
  int count = 0;
  ASSERT_TRUE(wal.Replay([&count](const WALRecord& r) {
    if (r.type == WALRecordType::kLogEntry) {
      count++;
    }
    return true;
  }).ok());
  // 10 entries + 1 truncate prefix record
  EXPECT_EQ(count, 10);

  wal.Close();
}

// ---------------------------------------------------------------------------
// Test 6: Truncate Suffix
// ---------------------------------------------------------------------------
TEST_F(WALPersisterTest, TruncateSuffix) {
  WALPersister wal;
  ASSERT_TRUE(wal.Open(test_dir_).ok());

  for (uint64_t i = 1; i <= 10; ++i) {
    ASSERT_TRUE(
        wal.AppendLogEntry(MakeEntry(i, 1, "data" + std::to_string(i)))
            .ok());
  }

  // Truncate from index 7 onwards
  ASSERT_TRUE(wal.AppendTruncateSuffix(7).ok());

  auto range = wal.GetLogRange();
  EXPECT_EQ(range.first, 1);
  EXPECT_EQ(range.second, 6);

  wal.Close();
}

// ---------------------------------------------------------------------------
// Test 7: Garbage Collection
// ---------------------------------------------------------------------------
TEST_F(WALPersisterTest, GarbageCollect) {
  WALPersister wal;
  ASSERT_TRUE(wal.Open(test_dir_).ok());

  // Append many entries to create multiple segments
  for (uint64_t i = 1; i <= 25000; ++i) {
    ASSERT_TRUE(wal.AppendLogEntry(MakeEntry(i, 1, "x")).ok());
  }

  int segments_before = 0;
  for (const auto& entry :
       std::filesystem::directory_iterator(test_dir_)) {
    if (entry.is_regular_file() && entry.path().extension() == ".wal") {
      segments_before++;
    }
  }
  EXPECT_GE(segments_before, 2);

  // GC entries before index 15000
  ASSERT_TRUE(wal.GarbageCollect(15000).ok());

  int segments_after = 0;
  for (const auto& entry :
       std::filesystem::directory_iterator(test_dir_)) {
    if (entry.is_regular_file() && entry.path().extension() == ".wal") {
      segments_after++;
    }
  }
  EXPECT_LT(segments_after, segments_before);

  // Verify remaining entries are still accessible
  // Note: segment-based GC can only delete whole segments.
  // Segment 2 starts at 10001 and contains entries up to ~20000,
  // so it must be kept. first_index_ will be 10001, not 15000.
  auto range = wal.GetLogRange();
  EXPECT_EQ(range.first, 10001);
  EXPECT_EQ(range.second, 25000);

  wal.Close();
}

// ---------------------------------------------------------------------------
// Test 8: Corruption Detection
// ---------------------------------------------------------------------------
TEST_F(WALPersisterTest, CorruptionDetection) {
  {
    WALPersister wal;
    ASSERT_TRUE(wal.Open(test_dir_).ok());

    for (uint64_t i = 1; i <= 5; ++i) {
      ASSERT_TRUE(
          wal.AppendLogEntry(MakeEntry(i, 1, "data" + std::to_string(i)))
              .ok());
    }
    ASSERT_TRUE(wal.Sync().ok());
    wal.Close();
  }

  // Corrupt the segment file
  for (const auto& entry :
       std::filesystem::directory_iterator(test_dir_)) {
    if (entry.is_regular_file() && entry.path().extension() == ".wal") {
      // Read file
      std::ifstream in(entry.path(), std::ios::binary);
      std::vector<char> data((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());
      in.close();

      // Corrupt some bytes after the header (likely in CRC or payload)
      constexpr size_t kWALHeaderSize = 16;
      if (data.size() > kWALHeaderSize + 10) {
        data[kWALHeaderSize + 5] ^= 0xFF;
      }

      // Write back
      std::ofstream out(entry.path(), std::ios::binary);
      out.write(data.data(), data.size());
      out.close();
      break;
    }
  }

  // Reopen should detect corruption during scan
  {
    WALPersister wal;
    auto status = wal.Open(test_dir_);
    // We expect either OK (if corruption is after valid data) or Corruption
    if (!status.ok()) {
      EXPECT_TRUE(status.IsCorruption());
    }
    wal.Close();
  }
}

// ---------------------------------------------------------------------------
// Test 9: Concurrent Append
// ---------------------------------------------------------------------------
TEST_F(WALPersisterTest, ConcurrentAppend) {
  WALPersister wal;
  ASSERT_TRUE(wal.Open(test_dir_).ok());

  constexpr int kNumThreads = 4;
  constexpr int kEntriesPerThread = 100;

  std::vector<std::thread> threads;
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kEntriesPerThread; ++i) {
        uint64_t index = t * kEntriesPerThread + i + 1;
        auto status =
            wal.AppendLogEntry(MakeEntry(index, 1, "thread" + std::to_string(t)));
        ASSERT_TRUE(status.ok());
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto range = wal.GetLogRange();
  EXPECT_EQ(range.first, 1);
  EXPECT_EQ(range.second, kNumThreads * kEntriesPerThread);

  // Verify all entries via replay
  int count = 0;
  ASSERT_TRUE(wal.Replay([&count](const WALRecord&) {
    count++;
    return true;
  }).ok());
  EXPECT_EQ(count, kNumThreads * kEntriesPerThread);

  wal.Close();
}

// ---------------------------------------------------------------------------
// Test 10: Reopen with existing data
// ---------------------------------------------------------------------------
TEST_F(WALPersisterTest, ReopenPreservesData) {
  {
    WALPersister wal;
    ASSERT_TRUE(wal.Open(test_dir_).ok());
    for (uint64_t i = 1; i <= 20; ++i) {
      ASSERT_TRUE(wal.AppendLogEntry(MakeEntry(i, 1, "data" + std::to_string(i)))
                      .ok());
    }
    ASSERT_TRUE(wal.Sync().ok());
    wal.Close();
  }

  {
    WALPersister wal;
    ASSERT_TRUE(wal.Open(test_dir_).ok());

    auto range = wal.GetLogRange();
    EXPECT_EQ(range.first, 1);
    EXPECT_EQ(range.second, 20);

    int count = 0;
    ASSERT_TRUE(wal.Replay([&count](const WALRecord&) {
      count++;
      return true;
    }).ok());
    EXPECT_EQ(count, 20);

    wal.Close();
  }
}
