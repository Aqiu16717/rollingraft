/**
 * @file test_leveldb_persister_streaming.cpp
 * @brief Unit tests for LevelDBPersister streaming snapshot interface
 */

#include <filesystem>
#include <gtest/gtest.h>

#include "rollingraft/persister.h"

using namespace rollingraft;

class LevelDBPersisterStreamingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/rollingraft_streaming_test_" +
                std::to_string(getpid()) + "_" + std::to_string(counter_++);
    std::filesystem::create_directories(test_dir_);

    persister_ = CreateLevelDBPersister();
    ASSERT_TRUE(persister_->Open(test_dir_).ok());
  }

  void TearDown() override {
    persister_->Close();
    std::filesystem::remove_all(test_dir_);
  }

  static int counter_;
  std::string test_dir_;
  std::unique_ptr<Persister> persister_;
};

int LevelDBPersisterStreamingTest::counter_ = 0;

TEST_F(LevelDBPersisterStreamingTest, SaveSnapshotStream_BasicRoundTrip) {
  // 256KB data split into 64KB chunks
  constexpr size_t kTotalSize = 256 * 1024;
  constexpr size_t kChunkSize = 64 * 1024;
  std::string original_data(kTotalSize, 'A');
  for (size_t i = 0; i < kTotalSize; ++i) {
    original_data[i] = static_cast<char>(i % 256);
  }

  // Save via streaming
  size_t offset = 0;
  auto chunk_provider = [&](std::string& chunk) -> bool {
    if (offset >= original_data.size()) return false;
    size_t to_copy = std::min(kChunkSize, original_data.size() - offset);
    chunk = original_data.substr(offset, to_copy);
    offset += to_copy;
    return true;
  };

  auto status = persister_->SaveSnapshotStream(chunk_provider, 42, 7);
  ASSERT_TRUE(status.ok());

  // Load via streaming
  std::string loaded_data;
  uint64_t loaded_index = 0, loaded_term = 0;
  auto chunk_consumer = [&](const std::string& chunk) {
    loaded_data += chunk;
  };

  status = persister_->LoadSnapshotStream(chunk_consumer, loaded_index,
                                          loaded_term);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(loaded_index, 42);
  EXPECT_EQ(loaded_term, 7);
  EXPECT_EQ(loaded_data, original_data);
}

TEST_F(LevelDBPersisterStreamingTest,
       SaveSnapshotStream_OverwritesOldFormat) {
  // First save using old interface
  std::string old_data = "old_format_data";
  auto status = persister_->SaveSnapshot(old_data, 10, 1);
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(persister_->HasSnapshot());

  // Now save using streaming interface
  std::string new_data = "new_streaming_data";
  size_t offset = 0;
  auto chunk_provider = [&](std::string& chunk) -> bool {
    if (offset >= new_data.size()) return false;
    chunk = new_data.substr(offset);
    offset += chunk.size();
    return true;
  };

  status = persister_->SaveSnapshotStream(chunk_provider, 20, 2);
  ASSERT_TRUE(status.ok());

  // Verify old format is gone: LoadSnapshot should fail or return empty
  std::string loaded_old;
  uint64_t idx, term;
  status = persister_->LoadSnapshot(loaded_old, idx, term);
  // Old format key was deleted; LoadSnapshot should not find it
  EXPECT_FALSE(status.ok());

  // Load via streaming should work
  std::string loaded_new;
  auto chunk_consumer = [&](const std::string& chunk) {
    loaded_new += chunk;
  };
  status = persister_->LoadSnapshotStream(chunk_consumer, idx, term);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(loaded_new, new_data);
  EXPECT_EQ(idx, 20);
  EXPECT_EQ(term, 2);
}

TEST_F(LevelDBPersisterStreamingTest, LoadSnapshotStream_OldFormatFallback) {
  // Save using old interface
  std::string old_data = "backward_compat_data";
  auto status = persister_->SaveSnapshot(old_data, 30, 3);
  ASSERT_TRUE(status.ok());

  // Load via streaming should fall back to old format
  std::string loaded_data;
  uint64_t loaded_index = 0, loaded_term = 0;
  auto chunk_consumer = [&](const std::string& chunk) {
    loaded_data += chunk;
  };

  status = persister_->LoadSnapshotStream(chunk_consumer, loaded_index,
                                          loaded_term);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(loaded_data, old_data);
  EXPECT_EQ(loaded_index, 30);
  EXPECT_EQ(loaded_term, 3);
}

TEST_F(LevelDBPersisterStreamingTest, SaveSnapshotStream_EmptyData) {
  auto chunk_provider = [&](std::string& chunk) -> bool {
    (void)chunk;
    return false;
  };

  auto status = persister_->SaveSnapshotStream(chunk_provider, 50, 5);
  ASSERT_TRUE(status.ok());

  // Empty snapshot should not be considered present
  EXPECT_FALSE(persister_->HasSnapshot());
}

TEST_F(LevelDBPersisterStreamingTest, SaveSnapshotStream_HashVerification) {
  // Save data in chunks
  std::string data = "data_for_hash_verification_test";
  size_t offset = 0;
  auto chunk_provider = [&](std::string& chunk) -> bool {
    if (offset >= data.size()) return false;
    chunk = data.substr(offset);
    offset += chunk.size();
    return true;
  };

  auto status = persister_->SaveSnapshotStream(chunk_provider, 100, 10);
  ASSERT_TRUE(status.ok());

  // Load and verify integrity (hash check passes for valid data)
  std::string loaded;
  uint64_t idx = 0, term = 0;
  auto chunk_consumer = [&](const std::string& chunk) { loaded += chunk; };
  status = persister_->LoadSnapshotStream(chunk_consumer, idx, term);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(loaded, data);
}
