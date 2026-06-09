/**
 * @file hybrid_persister.h
 * @brief Hybrid persister facade combining WALPersister + StatePersister
 *
 * Implements the Persister interface by delegating:
 * - Log operations (AppendEntries, GetEntries, GetEntry, TruncateSuffix,
 *   TruncatePrefix, GetLastLogInfo, Sync) -> WALPersister
 * - State operations (SaveState, LoadState) -> StatePersister
 * - Snapshot operations -> StatePersister
 *
 * Storage layout:
 *   <data_dir>/wal/    -> WAL segment files
 *   <data_dir>/state/  -> LevelDB for PersistentState + snapshots
 */

#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "rollingraft/persister.h"
#include "rollingraft/state_persister.h"
#include "rollingraft/status.h"
#include "rollingraft/wal_persister.h"

namespace rollingraft {

/**
 * Hybrid persister facade.
 *
 * Thread-safety: All public methods are thread-safe.
 */
class HybridPersister : public Persister {
 public:
  HybridPersister();
  ~HybridPersister() override;

  // Non-copyable, non-movable
  HybridPersister(const HybridPersister&) = delete;
  HybridPersister& operator=(const HybridPersister&) = delete;

  void SetSyncOnWrite(bool sync) override;
  void SetCompressionType(CompressionType type) override;

  Status Open(const std::string& data_dir) override;
  void Close() override;

  Status SaveState(const PersistentState& state) override;
  Status LoadState(PersistentState& state) override;

  Status AppendEntries(const std::vector<RaftLogEntry>& entries) override;
  Status Sync() override;
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
  Status SaveSnapshotStream(
      const std::function<bool(std::string& chunk)>& chunk_provider,
      uint64_t last_index, uint64_t last_term) override;
  Status LoadSnapshotStream(
      const std::function<void(const std::string& chunk)>& chunk_consumer,
      uint64_t& last_index, uint64_t& last_term) override;
  bool HasSnapshot() const override;

 private:
  mutable std::mutex mtx_;
  bool opened_ = false;
  std::string data_dir_;

  std::unique_ptr<WALPersister> wal_;
  std::unique_ptr<StatePersister> state_;
};

/** Create a Hybrid persister (WAL + StatePersister). */
std::unique_ptr<Persister> CreateHybridPersister();

}  // namespace rollingraft
