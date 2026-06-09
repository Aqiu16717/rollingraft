/**
 * @file state_persister.h
 * @brief Lightweight metadata + snapshot persister backed by LevelDB
 *
 * StatePersister is responsible for:
 * - PersistentState (current_term, voted_for)
 * - Snapshot storage (monolithic and streaming)
 *
 * Log entries are intentionally NOT handled here; they belong to WALPersister.
 *
 * Key prefix strategy:
 *   state:term         -> 8 bytes (uint64_t)
 *   state:voted_for    -> 8 bytes (int64_t)
 *   snapshot:data      -> old-format snapshot bytes
 *   snapshot:meta      -> metadata (last_index + last_term + chunk_count)
 *   snapshot:hash      -> SHA-256 checksum
 *   snapshot:chunk:<n> -> streaming chunk data
 */

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "rollingraft/persister.h"
#include "rollingraft/status.h"
#include "rollingraft/types.h"

namespace leveldb {
class DB;
}

namespace rollingraft {

class StatePersister {
 public:
  StatePersister();
  ~StatePersister();

  // Non-copyable, non-movable
  StatePersister(const StatePersister&) = delete;
  StatePersister& operator=(const StatePersister&) = delete;

  void SetSyncOnWrite(bool sync);
  void SetCompressionType(Persister::CompressionType type);

  /**
   * Open or create the state database.
   *
   * @param data_dir Directory for LevelDB files (e.g. <data_dir>/state)
   * @return Status::OK() on success
   */
  Status Open(const std::string& data_dir);

  /** Close the database and release handles. */
  void Close();

  /** Save PersistentState durably. */
  Status SaveState(const PersistentState& state);

  /** Load the last saved PersistentState. */
  Status LoadState(PersistentState& state);

  /** Save a monolithic snapshot. */
  Status SaveSnapshot(const std::string& snapshot_data, uint64_t last_index,
                      uint64_t last_term);

  /** Load a monolithic snapshot. */
  Status LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                      uint64_t& last_term);

  /** Save a streaming snapshot in chunks. */
  Status SaveSnapshotStream(
      const std::function<bool(std::string& chunk)>& chunk_provider,
      uint64_t last_index, uint64_t last_term);

  /** Load a streaming snapshot in chunks. */
  Status LoadSnapshotStream(
      const std::function<void(const std::string& chunk)>& chunk_consumer,
      uint64_t& last_index, uint64_t& last_term);

  /** Return true if any snapshot (old or new format) exists. */
  bool HasSnapshot() const;

  /** Return the last snapshot index (0 if no snapshot). */
  uint64_t GetSnapshotLastIndex() const;

  /** Return the last snapshot term (0 if no snapshot). */
  uint64_t GetSnapshotLastTerm() const;

 private:
  void DeleteSnapshotDataLocked();
  void LoadStateFromDB();

  mutable std::recursive_mutex mutex_;
  std::unique_ptr<leveldb::DB> db_;
  PersistentState cached_state_;

  uint64_t snapshot_last_index_ = 0;
  uint64_t snapshot_last_term_ = 0;

  bool sync_on_write_ = false;
  Persister::CompressionType compression_type_ = Persister::kSnappyCompression;
};

}  // namespace rollingraft
