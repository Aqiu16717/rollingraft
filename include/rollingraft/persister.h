/**
 * @file persister.h
 * @brief Persistent storage interface for Raft state
 *
 * Defines the Persister interface for storing Raft metadata,
 * log entries, and snapshots. Implementations can use different
 * storage backends (LevelDB, SQLite, etc.).
 */

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <rollingraft/raft_log.h>
#include <rollingraft/status.h>
#include <rollingraft/types.h>

namespace rollingraft {

/**
 * Persistent Raft state that must survive crashes.
 *
 * This metadata is critical for safety and must be fsync'd
 * before responding to RPCs during elections.
 */
struct PersistentState {
  Term current_term = 0;   // Latest term server has seen
  NodeId voted_for = -1;   // Candidate ID voted for in current term (-1 = none)
};

/**
 * Abstract persistence interface.
 *
 * Implement this interface to provide custom persistent storage.
 * All operations must be thread-safe as they may be called from
 * multiple threads concurrently.
 *
 * Durability requirements:
 * - SaveState() must be synchronous (fsync) before returning
 * - AppendEntries() should be batched for performance
 */
class Persister {
 public:
  virtual ~Persister() = default;

  // ==================== Lifecycle ====================

  /**
   * Open or create persistent storage.
   *
   * @param data_dir Directory path for storing data
   * @return Status::OK() on success
   */
  virtual Status Open(const std::string& data_dir) = 0;

  /** Close persistent storage and release resources. */
  virtual void Close() = 0;

  // ==================== Metadata Operations ====================

  /**
   * Save persistent state (synchronous write).
   *
   * Called when:
   * - Becoming candidate (increment term, vote for self)
   * - Receiving RPC with higher term (revert to follower)
   * - Voting for another candidate
   *
   * @param state Term and vote information to persist
   * @return Status::OK() after durable write
   */
  virtual Status SaveState(const PersistentState& state) = 0;

  /**
   * Load persistent state.
   *
   * @param state Output parameter for loaded state
   * @return Status::OK() (returns defaults if no state exists)
   */
  virtual Status LoadState(PersistentState& state) = 0;

  // ==================== Log Operations ====================

  /**
   * Append log entries (batch operation).
   *
   * Called when:
   * - Leader receives client commands
   * - Follower receives AppendEntries RPC
   *
   * @param entries Log entries to append
   * @return Status::OK() on success
   */
  virtual Status AppendEntries(const std::vector<RaftLogEntry>& entries) = 0;

  /**
   * Get log entries in range [start, end).
   *
   * Used by leader to send AppendEntries to followers.
   *
   * @param start First index to retrieve (inclusive)
   * @param end Last index to retrieve (exclusive)
   * @param out Output vector for entries
   * @return Status::OK() on success
   */
  virtual Status GetEntries(uint64_t start, uint64_t end,
                            std::vector<RaftLogEntry>* out) = 0;

  /**
   * Get a single log entry.
   *
   * @param index Log entry index
   * @param entry Output parameter for entry
   * @return Status::OK() if found
   */
  virtual Status GetEntry(uint64_t index, RaftLogEntry& entry) = 0;

  /**
   * Truncate log entries from index onwards.
   *
   * Called when log conflict is detected (follower's log is newer
   * than leader's). Deletes all entries in [from_index, ...].
   *
   * @param from_index First index to delete
   * @return Status::OK() on success
   */
  virtual Status TruncateSuffix(uint64_t from_index) = 0;

  /**
   * Delete old log entries before snapshot.
   *
   * Called after snapshot is created to reclaim space.
   * Deletes all entries in [1, before_index).
   *
   * @param before_index Delete entries before this index
   * @return Status::OK() on success
   */
  virtual Status TruncatePrefix(uint64_t before_index) = 0;

  /**
   * Get information about the last log entry.
   *
   * @return Pair of (last_index, last_term)
   */
  virtual std::pair<uint64_t, uint64_t> GetLastLogInfo() = 0;

  // ==================== Snapshot Operations ====================

  /**
   * Save snapshot data and metadata.
   *
   * @param snapshot_data Serialized state machine snapshot
   * @param last_index Last log index included in snapshot
   * @param last_term Last log term included in snapshot
   * @return Status::OK() on success
   */
  virtual Status SaveSnapshot(const std::string& snapshot_data,
                              uint64_t last_index, uint64_t last_term) {
    (void)snapshot_data;
    (void)last_index;
    (void)last_term;
    return Status::OK();  // Default no-op
  }

  /**
   * Load snapshot data and metadata.
   *
   * @param snapshot_data Output for serialized snapshot
   * @param last_index Output for last included index
   * @param last_term Output for last included term
   * @return Status::OK() if snapshot exists
   */
  virtual Status LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                              uint64_t& last_term) {
    (void)snapshot_data;
    (void)last_index;
    (void)last_term;
    return Status::OK();  // Default no-op
  }

  /**
   * Check if a snapshot exists.
   * @return true if snapshot is available
   */
  virtual bool HasSnapshot() const { return false; }

  /**
   * Configure whether writes should fsync to disk.
   *
   * Implementations that support synchronous writes (e.g. LevelDB)
   * can override this. Default is no-op.
   *
   * @param sync true to enable fsync on every write
   */
  virtual void SetSyncOnWrite(bool sync) { (void)sync; }
};

/** Factory function type for creating Persister instances. */
using PersisterFactory = std::function<std::unique_ptr<Persister>()>;

/** Create a LevelDB-backed persister. */
std::unique_ptr<Persister> CreateLevelDBPersister();

}  // namespace rollingraft
