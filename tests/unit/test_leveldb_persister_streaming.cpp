/**
 * @file test_leveldb_persister_streaming.cpp
 * @brief Unit tests for LevelDBPersister streaming snapshot interface
 */

#include <filesystem>

#include "rollingraft/persister.h"

#include <gtest/gtest.h>
#include <leveldb/db.h>

using namespace rollingraft;

class LevelDBPersisterStreamingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/rollingraft_streaming_test_" + std::to_string(getpid()) + "_" +
                std::to_string(counter_++);
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
  auto chunk_consumer = [&](const std::string& chunk) { loaded_data += chunk; };

  status = persister_->LoadSnapshotStream(chunk_consumer, loaded_index, loaded_term);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(loaded_index, 42);
  EXPECT_EQ(loaded_term, 7);
  EXPECT_EQ(loaded_data, original_data);
}

TEST_F(LevelDBPersisterStreamingTest, SaveSnapshotStream_OverwritesOldFormat) {
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
  auto chunk_consumer = [&](const std::string& chunk) { loaded_new += chunk; };
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
  auto chunk_consumer = [&](const std::string& chunk) { loaded_data += chunk; };

  status = persister_->LoadSnapshotStream(chunk_consumer, loaded_index, loaded_term);
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

TEST_F(LevelDBPersisterStreamingTest, SaveSnapshotStream_FewerChunksThanBefore) {
  // First save with 4 chunks
  std::string data1(256, 'A');
  size_t offset1 = 0;
  auto chunk_provider1 = [&](std::string& chunk) -> bool {
    if (offset1 >= data1.size()) return false;
    chunk = data1.substr(offset1, 64);
    offset1 += chunk.size();
    return true;
  };
  auto status = persister_->SaveSnapshotStream(chunk_provider1, 10, 1);
  ASSERT_TRUE(status.ok());

  // Now save with only 2 chunks
  std::string data2(128, 'B');
  size_t offset2 = 0;
  auto chunk_provider2 = [&](std::string& chunk) -> bool {
    if (offset2 >= data2.size()) return false;
    chunk = data2.substr(offset2, 64);
    offset2 += chunk.size();
    return true;
  };
  status = persister_->SaveSnapshotStream(chunk_provider2, 20, 2);
  ASSERT_TRUE(status.ok());

  // Load should only get 2 chunks (new data), not 4
  std::string loaded;
  uint64_t idx = 0, term = 0;
  auto chunk_consumer = [&](const std::string& chunk) { loaded += chunk; };
  status = persister_->LoadSnapshotStream(chunk_consumer, idx, term);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(loaded, data2);
  EXPECT_EQ(idx, 20);
  EXPECT_EQ(term, 2);
}
TEST_F(LevelDBPersisterStreamingTest, SaveSnapshotStream_AtomicReplace_Normal) {
  // Save an initial streaming snapshot.
  std::string data_a(128, 'A');
  size_t offset_a = 0;
  auto provider_a = [&](std::string& chunk) -> bool {
    if (offset_a >= data_a.size()) return false;
    chunk = data_a.substr(offset_a, 32);
    offset_a += chunk.size();
    return true;
  };
  ASSERT_TRUE(persister_->SaveSnapshotStream(provider_a, 10, 1).ok());

  // Replace it with a new streaming snapshot.
  std::string data_b(128, 'B');
  size_t offset_b = 0;
  auto provider_b = [&](std::string& chunk) -> bool {
    if (offset_b >= data_b.size()) return false;
    chunk = data_b.substr(offset_b, 32);
    offset_b += chunk.size();
    return true;
  };
  ASSERT_TRUE(persister_->SaveSnapshotStream(provider_b, 20, 2).ok());

  // New snapshot must be loadable.
  std::string loaded;
  uint64_t idx = 0, term = 0;
  auto consumer = [&](const std::string& chunk) { loaded += chunk; };
  ASSERT_TRUE(persister_->LoadSnapshotStream(consumer, idx, term).ok());
  EXPECT_EQ(loaded, data_b);
  EXPECT_EQ(idx, 20);
  EXPECT_EQ(term, 2);
}

TEST_F(LevelDBPersisterStreamingTest, SaveSnapshotStream_AtomicReplace_PreservesOldOnInterruption) {
  // Save an initial streaming snapshot.
  std::string data_a(128, 'A');
  size_t offset_a = 0;
  auto provider_a = [&](std::string& chunk) -> bool {
    if (offset_a >= data_a.size()) return false;
    chunk = data_a.substr(offset_a, 32);
    offset_a += chunk.size();
    return true;
  };
  ASSERT_TRUE(persister_->SaveSnapshotStream(provider_a, 10, 1).ok());

  // Attempt to replace it, but the provider throws after two chunks.
  std::string data_b(128, 'B');
  size_t offset_b = 0;
  int calls = 0;
  auto provider_b = [&](std::string& chunk) -> bool {
    if (offset_b >= data_b.size()) return false;
    chunk = data_b.substr(offset_b, 32);
    offset_b += chunk.size();
    ++calls;
    if (calls == 2) {
      throw std::runtime_error("simulated chunk provider failure");
    }
    return true;
  };
  auto status = persister_->SaveSnapshotStream(provider_b, 20, 2);
  EXPECT_FALSE(status.ok());

  // Old snapshot must still be loadable and intact.
  std::string loaded;
  uint64_t idx = 0, term = 0;
  auto consumer = [&](const std::string& chunk) { loaded += chunk; };
  ASSERT_TRUE(persister_->LoadSnapshotStream(consumer, idx, term).ok());
  EXPECT_EQ(loaded, data_a);
  EXPECT_EQ(idx, 10);
  EXPECT_EQ(term, 1);
}

TEST_F(LevelDBPersisterStreamingTest, SaveSnapshotStream_AtomicReplace_HashVerificationFailure) {
  // Save an initial streaming snapshot.
  std::string data_a(128, 'A');
  size_t offset_a = 0;
  auto provider_a = [&](std::string& chunk) -> bool {
    if (offset_a >= data_a.size()) return false;
    chunk = data_a.substr(offset_a, 32);
    offset_a += chunk.size();
    return true;
  };
  ASSERT_TRUE(persister_->SaveSnapshotStream(provider_a, 10, 1).ok());

  // Replace it with a new streaming snapshot.
  std::string data_b(128, 'B');
  size_t offset_b = 0;
  auto provider_b = [&](std::string& chunk) -> bool {
    if (offset_b >= data_b.size()) return false;
    chunk = data_b.substr(offset_b, 32);
    offset_b += chunk.size();
    return true;
  };
  ASSERT_TRUE(persister_->SaveSnapshotStream(provider_b, 20, 2).ok());

  // Close persister so we can open LevelDB directly and corrupt a chunk.
  persister_->Close();
  {
    leveldb::DB* db_ptr = nullptr;
    leveldb::Options options;
    leveldb::Status s = leveldb::DB::Open(options, test_dir_, &db_ptr);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::unique_ptr<leveldb::DB> db(db_ptr);

    // Corrupt the first chunk of the new snapshot.
    std::string corrupted = "corrupted_chunk_data";
    s = db->Put(leveldb::WriteOptions(), "snapshot:chunk:0", corrupted);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Reopen persister and verify loading fails due to hash mismatch.
  ASSERT_TRUE(persister_->Open(test_dir_).ok());
  std::string loaded;
  uint64_t idx = 0, term = 0;
  auto consumer = [&](const std::string& chunk) { loaded += chunk; };
  auto status = persister_->LoadSnapshotStream(consumer, idx, term);
  EXPECT_FALSE(status.ok());
}
TEST_F(LevelDBPersisterStreamingTest, SaveSnapshotStream_EmptyData_ClearsExistingSnapshot) {
  // Save an initial streaming snapshot.
  std::string data(128, 'A');
  size_t offset = 0;
  auto provider = [&](std::string& chunk) -> bool {
    if (offset >= data.size()) return false;
    chunk = data.substr(offset, 32);
    offset += chunk.size();
    return true;
  };
  ASSERT_TRUE(persister_->SaveSnapshotStream(provider, 10, 1).ok());
  EXPECT_TRUE(persister_->HasSnapshot());

  // Now save an empty streaming snapshot; legacy behavior is to clear
  // the existing snapshot.
  auto empty_provider = [&](std::string& /*chunk*/) -> bool { return false; };
  ASSERT_TRUE(persister_->SaveSnapshotStream(empty_provider, 20, 2).ok());
  EXPECT_FALSE(persister_->HasSnapshot());
}
