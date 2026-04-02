#pragma once

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "rollingraft/persister.h"
#include "rollingraft/raft_log.h"

namespace rollingraft {

/**
 * Mock persister for testing.
 * In-memory storage with failure injection support.
 */
class MockPersister : public Persister {
 public:
  MockPersister() = default;
  ~MockPersister() override = default;

  // Non-copyable, non-movable
  MockPersister(const MockPersister&) = delete;
  MockPersister& operator=(const MockPersister&) = delete;

  Status Open(const std::string& dir) override;
  void Close() override;

  Status SaveState(const PersistentState& state) override;
  Status LoadState(PersistentState& state) override;

  Status AppendEntries(const std::vector<RaftLogEntry>& entries) override;
  Status GetEntries(uint64_t start, uint64_t end,
                    std::vector<RaftLogEntry>* out) override;
  Status GetEntry(uint64_t index, RaftLogEntry& entry) override;
  Status TruncateSuffix(uint64_t from_index) override;
  Status TruncatePrefix(uint64_t before_index) override;
  std::pair<uint64_t, uint64_t> GetLastLogInfo() override;

  Status SaveSnapshot(const std::string& snapshot_data, uint64_t last_index,
                      uint64_t last_term) override;
  Status LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                      uint64_t& last_term) override;
  bool HasSnapshot() const override;

  // Test control interface

  /**
   * Inject a failure for next operation.
   */
  void InjectFailure(const std::string& error_msg);

  /**
   * Clear injected failure.
   */
  void ClearFailure();

  /**
   * Get number of entries in storage.
   */
  size_t EntryCount() const;

  /**
   * Get number of write operations.
   */
  size_t GetWriteCount() const { return write_count_; }

  /**
   * Reset all data.
   */
  void Reset();

 private:
  bool CheckFailure();

  std::map<uint64_t, RaftLogEntry> logs_;
  PersistentState state_{0, -1};
  std::string snapshot_data_;
  uint64_t snapshot_last_index_ = 0;
  uint64_t snapshot_last_term_ = 0;
  mutable std::mutex mutex_;

  std::string failure_msg_;
  size_t write_count_ = 0;
};

}  // namespace rollingraft
