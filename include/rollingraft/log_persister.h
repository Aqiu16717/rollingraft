/**
 * @file log_persister.h
 * @brief Batched log persistence coordinator
 *
 * Coordinates between in-memory RaftLog and persistent storage.
 * Provides batching for performance while ensuring durability
 * for critical operations.
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rollingraft/persister.h"
#include "rollingraft/raft_log.h"
#include "rollingraft/status.h"

namespace rollingraft {

/**
 * Configuration for log persistence behavior.
 *
 * Controls batching, sync behavior, and resource limits.
 */
struct LogPersistenceConfig {
  /** Number of entries to batch before writing to disk. */
  size_t batch_size = 100;

  /** Maximum time to wait before flushing (milliseconds). */
  uint32_t batch_interval_ms = 10;

  /** Whether to sync on critical operations (leader first log). */
  bool sync_on_critical = true;

  /** Minimum disk space required (bytes). Default: 100MB. */
  uint64_t min_disk_space_bytes = 100 * 1024 * 1024;
};

/**
 * Coordinates log persistence with batching.
 *
 * Design goals:
 * - Batch writes for performance
 * - Sync writes for safety at critical points
 * - Automatic recovery on startup
 * - Background flush thread for non-blocking operation
 *
 * Thread-safety: All public methods are thread-safe.
 */
class LogPersister {
 public:
  /**
   * Construct a LogPersister.
   *
   * @param persister Underlying persistent storage
   * @param config Persistence configuration
   */
  LogPersister(std::unique_ptr<Persister> persister,
               LogPersistenceConfig config = {});

  ~LogPersister();

  // Non-copyable, non-movable
  LogPersister(const LogPersister&) = delete;
  LogPersister& operator=(const LogPersister&) = delete;
  LogPersister(LogPersister&&) = delete;
  LogPersister& operator=(LogPersister&&) = delete;

  /** Start the background flush thread. */
  void Start();

  /** Stop the background thread and flush remaining entries. */
  void Stop();

  /**
   * Append a log entry asynchronously.
   *
   * The entry is buffered and written to disk later by the
   * background thread or on next batch flush.
   *
   * @param entry The log entry to append
   */
  void Append(const RaftLogEntry& entry);

  /**
   * Force flush all buffered entries to disk.
   *
   * Blocks until all pending entries are persisted.
   *
   * @return Status indicating success or failure
   */
  Status FlushSync();

  /**
   * Trigger an asynchronous flush (non-blocking).
   *
   * Wakes up the background thread to flush immediately.
   */
  void TriggerFlush();

  /**
   * Restore log entries from persistent storage.
   *
   * @param start_index Index to start restoring from (typically after snapshot)
   * @return Vector of restored entries
   */
  std::vector<RaftLogEntry> Restore(uint64_t start_index);

  /** Get the number of pending entries in the buffer. */
  size_t GetPendingCount() const;

  /** Check if the persister is healthy (no disk errors). */
  bool IsHealthy() const;

  /** Get the last error message. */
  std::string GetLastError() const;

 private:
  /** Background thread main loop. */
  void BackgroundFlushLoop();

  /**
   * Perform a single flush operation.
   * @return true if successful
   */
  bool DoFlush();

  /** Write a batch of entries to storage. */
  Status WriteBatch(const std::vector<RaftLogEntry>& entries);

  /** Check if disk space is sufficient. */
  Status CheckDiskSpace();

 private:
  struct PendingEntry {
    RaftLogEntry entry;
    bool needs_confirm = false;  // For future sync support
  };

  std::unique_ptr<Persister> persister_;
  LogPersistenceConfig config_;

  // Buffer for pending entries
  std::vector<PendingEntry> buffer_;
  mutable std::mutex buffer_mutex_;

  // Background thread control
  std::atomic<bool> running_{false};
  std::thread flush_thread_;
  std::condition_variable flush_cv_;

  // Error tracking
  std::atomic<bool> healthy_{true};
  std::string last_error_;
  mutable std::mutex error_mutex_;

  // Statistics
  std::atomic<uint64_t> total_flushed_{0};
  std::atomic<uint64_t> total_flush_ops_{0};
};

}  // namespace rollingraft
