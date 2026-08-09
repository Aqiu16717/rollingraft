/**
 * @file test_leveldb_persister.cpp
 * @brief Unit tests for LevelDBPersister with CRC32 validation
 */

#include <filesystem>
#include <fstream>

#include "rollingraft/persister.h"

#include <gtest/gtest.h>

using namespace rollingraft;

class LevelDBPersisterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a unique temp directory for each test
    test_dir_ =
        "/tmp/rollingraft_test_" + std::to_string(getpid()) + "_" + std::to_string(counter_++);
    std::filesystem::create_directories(test_dir_);

    persister_ = CreateLevelDBPersister();
    ASSERT_TRUE(persister_->Open(test_dir_).ok());
  }

  void TearDown() override {
    persister_->Close();
    // Clean up test directory
    std::filesystem::remove_all(test_dir_);
  }

  RaftLogEntry MakeEntry(uint64_t index, uint64_t term, const std::string& data) {
    RaftLogEntry entry;
    entry.index_ = index;
    entry.term_ = term;
    entry.data_ = data;
    return entry;
  }

  // Corrupt a byte in the LevelDB value at the given index
  void CorruptEntry(uint64_t /*index*/) {
    // This requires direct LevelDB access - we'll simulate by closing,
    // manually corrupting the file, and reopening
    persister_->Close();

    // Find and corrupt the LevelDB files
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.is_regular_file() && entry.path().extension() == ".ldb") {
        // Read file
        std::ifstream in(entry.path(), std::ios::binary);
        std::vector<char> data((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
        in.close();

        // Corrupt some bytes in the middle (likely to hit entry data)
        if (data.size() > 100) {
          data[data.size() / 2] ^= 0xFF;  // Flip bits
          data[data.size() / 2 + 1] ^= 0xFF;
        }

        // Write back
        std::ofstream out(entry.path(), std::ios::binary);
        out.write(data.data(), data.size());
        out.close();
        break;
      }
    }

    // Reopen
    persister_->Open(test_dir_);
  }

  static int counter_;
  std::string test_dir_;
  std::unique_ptr<Persister> persister_;
};

int LevelDBPersisterTest::counter_ = 0;

TEST_F(LevelDBPersisterTest, BasicAppendAndGet) {
  std::vector<RaftLogEntry> entries;
  entries.push_back(MakeEntry(1, 1, "cmd1"));
  entries.push_back(MakeEntry(2, 1, "cmd2"));

  ASSERT_TRUE(persister_->AppendEntries(entries).ok());

  RaftLogEntry entry;
  ASSERT_TRUE(persister_->GetEntry(1, entry).ok());
  EXPECT_EQ(entry.index_, 1);
  EXPECT_EQ(entry.term_, 1);
  EXPECT_EQ(entry.data_, "cmd1");
  // Checksum should be set after deserialization
  EXPECT_NE(entry.checksum_, 0);
}

TEST_F(LevelDBPersisterTest, GetEntriesRange) {
  std::vector<RaftLogEntry> entries;
  for (int i = 1; i <= 5; ++i) {
    entries.push_back(MakeEntry(i, 1, "cmd" + std::to_string(i)));
  }
  ASSERT_TRUE(persister_->AppendEntries(entries).ok());

  std::vector<RaftLogEntry> result;
  ASSERT_TRUE(persister_->GetEntries(2, 5, &result).ok());
  EXPECT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].index_, 2);
  EXPECT_EQ(result[2].index_, 4);
}

TEST_F(LevelDBPersisterTest, ChecksumVerifiedOnRead) {
  // Append an entry
  std::vector<RaftLogEntry> entries;
  entries.push_back(MakeEntry(1, 1, "test data"));
  ASSERT_TRUE(persister_->AppendEntries(entries).ok());

  // Read it back - should succeed
  RaftLogEntry entry;
  ASSERT_TRUE(persister_->GetEntry(1, entry).ok());
  EXPECT_EQ(entry.data_, "test data");
}

TEST_F(LevelDBPersisterTest, DetectsCorruptedData) {
  // Append multiple entries
  std::vector<RaftLogEntry> entries;
  for (int i = 1; i <= 10; ++i) {
    entries.push_back(MakeEntry(i, 1, "data" + std::to_string(i)));
  }
  ASSERT_TRUE(persister_->AppendEntries(entries).ok());

  // Get entries before corruption - should succeed
  std::vector<RaftLogEntry> result;
  ASSERT_TRUE(persister_->GetEntries(1, 11, &result).ok());
  EXPECT_EQ(result.size(), 10);

  // Close and corrupt WAL files (log entries now live in WAL, not LevelDB)
  persister_->Close();

  std::string wal_dir = test_dir_ + "/wal";
  for (const auto& file : std::filesystem::directory_iterator(wal_dir)) {
    if (file.is_regular_file()) {
      std::string ext = file.path().extension().string();
      if (ext == ".wal") {
        std::fstream fs(file.path(), std::ios::in | std::ios::out | std::ios::binary);
        if (fs) {
          fs.seekg(0, std::ios::end);
          auto size = fs.tellg();
          if (size > 50) {
            fs.seekp(size / 2);
            char byte;
            fs.read(&byte, 1);
            byte ^= 0xFF;
            fs.seekp(size / 2);
            fs.write(&byte, 1);
          }
          fs.close();
        }
        break;
      }
    }
  }

  // Reopen detects the WAL corruption and recovers by truncating the segment
  // at the corrupt record (Raft logs are rebuilt from the leader). Entries
  // before the corruption point remain readable.
  persister_ = CreateLevelDBPersister();
  auto status = persister_->Open(test_dir_);
  EXPECT_TRUE(status.ok()) << "Open should recover from WAL corruption: " << status.ToString();

  // GetLastLogInfo returns {last_index, term_of_last}.
  auto [last_index, last_term] = persister_->GetLastLogInfo();
  EXPECT_EQ(last_term, 1u) << "Term of the surviving tail entry must be intact";
  EXPECT_GE(last_index, 1u) << "At least the first entries must remain readable";
  EXPECT_LT(last_index, 10u) << "Entries at/after the corruption point are truncated";
}

TEST_F(LevelDBPersisterTest, StatePersistence) {
  PersistentState state;
  state.current_term = 42;
  state.voted_for = 5;

  ASSERT_TRUE(persister_->SaveState(state).ok());

  PersistentState loaded;
  ASSERT_TRUE(persister_->LoadState(loaded).ok());
  EXPECT_EQ(loaded.current_term, 42);
  EXPECT_EQ(loaded.voted_for, 5);
}

TEST_F(LevelDBPersisterTest, TruncateSuffix) {
  std::vector<RaftLogEntry> entries;
  for (int i = 1; i <= 5; ++i) {
    entries.push_back(MakeEntry(i, 1, "cmd" + std::to_string(i)));
  }
  ASSERT_TRUE(persister_->AppendEntries(entries).ok());

  // Truncate from index 3 onwards
  ASSERT_TRUE(persister_->TruncateSuffix(3).ok());

  // Should only have entries 1 and 2
  auto [last_index, last_term] = persister_->GetLastLogInfo();
  EXPECT_EQ(last_index, 2);
}

TEST_F(LevelDBPersisterTest, TruncatePrefix) {
  std::vector<RaftLogEntry> entries;
  for (int i = 1; i <= 5; ++i) {
    entries.push_back(MakeEntry(i, 1, "cmd" + std::to_string(i)));
  }
  ASSERT_TRUE(persister_->AppendEntries(entries).ok());

  // Truncate entries before index 3 (delete 1 and 2)
  ASSERT_TRUE(persister_->TruncatePrefix(3).ok());

  // Entry 1 and 2 should be gone
  RaftLogEntry entry;
  EXPECT_FALSE(persister_->GetEntry(1, entry).ok());
  EXPECT_FALSE(persister_->GetEntry(2, entry).ok());

  // Entry 3+ should still exist
  EXPECT_TRUE(persister_->GetEntry(3, entry).ok());
}

TEST_F(LevelDBPersisterTest, LargeData) {
  // Test with larger data payload
  std::string large_data(10000, 'x');

  std::vector<RaftLogEntry> entries;
  entries.push_back(MakeEntry(1, 1, large_data));
  ASSERT_TRUE(persister_->AppendEntries(entries).ok());

  RaftLogEntry entry;
  ASSERT_TRUE(persister_->GetEntry(1, entry).ok());
  EXPECT_EQ(entry.data_.size(), 10000);
  EXPECT_EQ(entry.data_, large_data);
}

TEST_F(LevelDBPersisterTest, BinaryData) {
  // Test with binary data containing null bytes
  std::string binary_data;
  binary_data.push_back(0x00);
  binary_data.push_back(static_cast<char>(0xFF));
  binary_data.push_back(0x42);
  binary_data.push_back(0x00);

  std::vector<RaftLogEntry> entries;
  entries.push_back(MakeEntry(1, 1, binary_data));
  ASSERT_TRUE(persister_->AppendEntries(entries).ok());

  RaftLogEntry entry;
  ASSERT_TRUE(persister_->GetEntry(1, entry).ok());
  EXPECT_EQ(entry.data_, binary_data);
}

TEST_F(LevelDBPersisterTest, SaveAndLoadSnapshot) {
  // Save a snapshot
  std::string snapshot_data = "snapshot_data_test";
  uint64_t last_index = 100;
  uint64_t last_term = 5;

  ASSERT_TRUE(persister_->SaveSnapshot(snapshot_data, last_index, last_term).ok());
  EXPECT_TRUE(persister_->HasSnapshot());

  // Load it back
  std::string loaded_data;
  uint64_t loaded_index, loaded_term;
  ASSERT_TRUE(persister_->LoadSnapshot(loaded_data, loaded_index, loaded_term).ok());

  EXPECT_EQ(loaded_data, snapshot_data);
  EXPECT_EQ(loaded_index, last_index);
  EXPECT_EQ(loaded_term, last_term);
}

TEST_F(LevelDBPersisterTest, SnapshotSha256Verification) {
  // Save a snapshot
  std::string snapshot_data = "important_state_machine_data";
  ASSERT_TRUE(persister_->SaveSnapshot(snapshot_data, 50, 3).ok());

  // Load should succeed with correct data
  std::string loaded_data;
  uint64_t loaded_index, loaded_term;
  ASSERT_TRUE(persister_->LoadSnapshot(loaded_data, loaded_index, loaded_term).ok());
  EXPECT_EQ(loaded_data, snapshot_data);
}

TEST_F(LevelDBPersisterTest, SnapshotCorruptionDetected) {
  // Save a snapshot
  std::string original_data = "original_snapshot_data";
  ASSERT_TRUE(persister_->SaveSnapshot(original_data, 100, 5).ok());

  // Close and corrupt the snapshot directly
  persister_->Close();

  // Corrupt the snapshot file
  for (const auto& file : std::filesystem::directory_iterator(test_dir_)) {
    if (file.is_regular_file()) {
      std::string ext = file.path().extension().string();
      if (ext == ".ldb" || ext == ".log") {
        std::fstream fs(file.path(), std::ios::in | std::ios::out | std::ios::binary);
        if (fs) {
          fs.seekg(0, std::ios::end);
          auto size = fs.tellg();
          if (size > 50) {
            // Corrupt some bytes in the middle
            fs.seekp(size / 2);
            char byte = static_cast<char>(0xFF);
            fs.write(&byte, 1);
          }
          fs.close();
        }
      }
    }
  }

  // Reopen and try to load
  persister_ = CreateLevelDBPersister();
  ASSERT_TRUE(persister_->Open(test_dir_).ok());

  // Load should fail or return empty data due to corruption
  std::string loaded_data;
  uint64_t loaded_index, loaded_term;
  auto status = persister_->LoadSnapshot(loaded_data, loaded_index, loaded_term);

  // Either load fails with integrity error, or data is empty
  if (status.ok()) {
    EXPECT_TRUE(loaded_data.empty());
  }
}

// Test that SetCompressionType can be called before Open
TEST_F(LevelDBPersisterTest, CompressionTypeNoCompression) {
  persister_->Close();
  std::filesystem::remove_all(test_dir_);
  std::filesystem::create_directories(test_dir_);

  persister_->SetCompressionType(Persister::kNoCompression);
  ASSERT_TRUE(persister_->Open(test_dir_).ok());

  auto entry = MakeEntry(1, 1, "hello");
  ASSERT_TRUE(persister_->AppendEntries({entry}).ok());

  std::vector<RaftLogEntry> out;
  ASSERT_TRUE(persister_->GetEntries(1, 2, &out).ok());
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].data_, "hello");
}

TEST_F(LevelDBPersisterTest, CompressionTypeSnappy) {
  persister_->Close();
  std::filesystem::remove_all(test_dir_);
  std::filesystem::create_directories(test_dir_);

  persister_->SetCompressionType(Persister::kSnappyCompression);
  ASSERT_TRUE(persister_->Open(test_dir_).ok());

  auto entry = MakeEntry(1, 1, "hello snappy");
  ASSERT_TRUE(persister_->AppendEntries({entry}).ok());

  std::vector<RaftLogEntry> out;
  ASSERT_TRUE(persister_->GetEntries(1, 2, &out).ok());
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].data_, "hello snappy");
}
