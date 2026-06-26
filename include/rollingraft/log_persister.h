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
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rollingraft/metrics.h"
#include "rollingraft/persister.h"
#include "rollingraft/raft_log.h"
#include "rollingraft/status.h"

namespace rollingraft {

// Forward declaration for private implementation detail.
class GroupCommitController;

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

  /** Whether to sync on critical operations (leader first log).
   *
   * Deprecated: use sync_policy instead. When sync_policy is
   * kSyncEveryWrite this field is effectively true; otherwise it is ignored.
   */
  bool sync_on_critical = true;

  /**
   * Group commit / durable sync policy.
   */
  enum class SyncPolicy {
    /** No group commit; Sync() after every AppendEntries(). */
    kSyncEveryWrite,
    /** Sync at most every group_commit_interval_ms. */
    kSyncByInterval,
    /** Sync when unsynced entries or bytes exceed threshold. */
    kSyncByBatchSize,
    /** Sync when interval OR batch-size threshold is reached first. */
    kSyncAdaptive,
  };

  /**
   * Group commit sync policy.
   *
   * - kSyncEveryWrite: no group commit; each flush is followed by Sync().
   * - kSyncByInterval: background sync at fixed interval.
   * - kSyncByBatchSize: background sync when unsynced data exceeds threshold.
   * - kSyncAdaptive: interval OR batch-size, whichever fires first.
   */
  SyncPolicy sync_policy = SyncPolicy::kSyncAdaptive;

  /**
   * Group commit interval (milliseconds).
   *
   * Used by kSyncByInterval and kSyncAdaptive. Set to 0 to mean
   * "as fast as possible" (not recommended for production).
   */
  uint32_t group_commit_interval_ms = 50;

  /** Maximum unsynced entries before forcing fsync (kSyncByBatchSize / adaptive). */
  size_t group_commit_max_entries = 1000;

  /** Maximum unsynced bytes before forcing fsync (kSyncByBatchSize / adaptive). */
  size_t group_commit_max_bytes = 4 * 1024 * 1024;

  /** Maximum time a caller may wait for explicit Sync() to complete. */
  std::chrono::milliseconds sync_timeout = std::chrono::seconds(5);

  /** Minimum disk space required (bytes). Default: 100MB. */
  uint64_t min_disk_space_bytes = 100 * 1024 * 1024;

  /** Directory to check for disk space. Empty = skip check. */
  std::string data_dir;

  /**
   * Optional executor for async work scheduling.
   * If provided, TruncatePrefixAsync posts work to this executor.
   * If empty, TruncatePrefixAsync falls back to synchronous execution.
   */
  using Executor = std::function<void(std::function<void()>)>;
  Executor executor;

  /**
   * Optional executor for durable callbacks.
   *
   * When group commit is enabled, callbacks are fired after fsync. If this
   * executor is set, callback batches are posted to it instead of running
   * inline on the sync thread. The executor must outlive LogPersister.
   */
  Executor durable_callback_executor;
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
  explicit LogPersister(std::shared_ptr<Persister> persister,
                        LogPersistenceConfig config = {},
                        MetricsRegistry* metrics = nullptr);

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

  /** Callback type invoked when an entry has been durably synced. */
  using FlushCallback = std::function<void(Status)>;

  /**
   * Append a log entry asynchronously.
   *
   * The entry is buffered, flushed, and then synced according to the
   * configured sync policy. The optional callback is invoked when the entry
   * is guaranteed to be durable (after fsync for group commit, or after
   * AppendEntries + Sync for kSyncEveryWrite).
   *
   * @param entry The log entry to append
   * @param callback Optional callback invoked when the entry is durable
   */
  void Append(const RaftLogEntry& entry, FlushCallback callback = nullptr);

  /**
   * Append a log entry and block until it is durably persisted.
   *
   * @param entry The log entry to append
   * @param timeout Maximum time to wait for flush
   * @return Status indicating success or failure
   */
  Status AppendSync(
      const RaftLogEntry& entry,
      std::chrono::milliseconds timeout = std::chrono::seconds(5));

  /**
   * Force flush all buffered entries to disk.
   *
   * Blocks until all pending entries are persisted.
   *
   * @return Status indicating success or failure
   */
  Status FlushSync();

  /**
   * Force flush with a custom timeout.
   *
   * @param timeout Maximum time to wait for flush
   * @return Status indicating success or failure
   */
  Status FlushSync(std::chrono::milliseconds timeout);

  /**
   * Trigger an asynchronous flush (non-blocking).
   *
   * Wakes up the background thread to flush immediately.
   */
  void TriggerFlush();

  /**
   * Delete persisted log entries before the given index.
   *
   * Drains the write buffer first, then delegates to the underlying
   * persister. Safe to call concurrently with Append().
   *
   * @param before_index Delete entries with index < before_index
   * @return Status::OK() on success
   */
  Status TruncatePrefix(uint64_t before_index);

  /** Callback type invoked when async truncation completes. */
  using TruncateCallback = std::function<void(Status)>;

  /**
   * Delete persisted log entries asynchronously.
   *
   * Drains the write buffer synchronously, then schedules the actual
   * truncation via the configured executor (or caller thread if none).
   * Safe to call while holding Raft manager locks.
   *
   * @param before_index Delete entries with index < before_index
   * @param callback Optional callback invoked when truncation completes
   */
  void TruncatePrefixAsync(uint64_t before_index,
                           TruncateCallback callback = nullptr);

  /**
   * Restore log entries from persistent storage.
   *
   * @param start_index Index to start restoring from (typically after snapshot)
   * @return Vector of restored entries
   */
  std::vector<RaftLogEntry> Restore(uint64_t start_index);

  /** Get the number of pending entries in the buffer. */
  size_t GetPendingCount() const;

  /**
   * Explicitly sync all pending writes to durable storage.
   *
   * Blocks until all flushed-but-not-yet-synced data is durable.
   * No-op if group commit is not enabled and the persister already
   * syncs inline.
   */
  Status Sync();

  /** Check if the persister is healthy (no disk errors). */
  bool IsHealthy() const;

  /** Get the last error message. */
  std::string GetLastError() const;

 private:
  /** Background flush thread main loop. */
  void BackgroundFlushLoop();

  /** Background sync thread main loop (for group commit). */
  void BackgroundSyncLoop();

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
    FlushCallback callback;
  };

  std::shared_ptr<Persister> persister_;
  LogPersistenceConfig config_;

  // Buffer for pending entries
  std::vector<PendingEntry> buffer_;
  mutable std::mutex buffer_mutex_;

  // Background thread control
  std::atomic<bool> running_{false};
  std::thread flush_thread_;
  std::thread sync_thread_;  // For group commit
  std::condition_variable flush_cv_;
  std::condition_variable sync_cv_;
  mutable std::mutex controller_mutex_;
  bool flush_in_progress_ = false;

  // Group commit controller (null when sync_policy == kSyncEveryWrite)
  std::unique_ptr<GroupCommitController> group_commit_controller_;

  // Metrics registry (optional, forwarded to group commit controller)
  MetricsRegistry* metrics_ = nullptr;

  // Error tracking
  std::atomic<bool> healthy_{true};
  std::string last_error_;
  mutable std::mutex error_mutex_;

  // Statistics
  std::atomic<uint64_t> total_flushed_{0};
  std::atomic<uint64_t> total_flush_ops_{0};

  // Async truncation state
  struct AsyncState {
    std::atomic<bool> shutdown{false};
    std::atomic<size_t> pending_count{0};
    std::shared_ptr<Persister> persister;
    std::mutex cv_mutex;
    std::condition_variable cv;
  };
  std::shared_ptr<AsyncState> async_state_;
};

}  // namespace rollingraft
