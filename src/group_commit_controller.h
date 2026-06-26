/**
 * @file group_commit_controller.h
 * @brief Internal group commit / async WAL sync coordinator
 *
 * This is a private implementation detail of LogPersister. It is not part of
 * the public API and should not be included by user code.
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <vector>

#include "rollingraft/log_persister.h"
#include "rollingraft/metrics.h"
#include "rollingraft/status.h"

namespace rollingraft {

/**
 * Coordinates flush/sync separation and durable callbacks for LogPersister.
 *
 * Every batch flushed by LogPersister is registered with the controller and
 * assigned a monotonically increasing CommitEpoch. A background sync worker
 * periodically asks the controller for the range of epochs to sync, issues a
 * single Persister::Sync(), and reports success or failure back to the
 * controller. The controller then fires durable callbacks for all epochs that
 * have become durable (success) or poisons pending epochs (failure).
 *
 * Thread-safety: all public methods are thread-safe.
 */
class GroupCommitController {
 public:
  using DurableCallback = std::function<void(Status)>;

  explicit GroupCommitController(const LogPersistenceConfig& config,
                                 MetricsRegistry* metrics = nullptr,
                                 const std::map<std::string, std::string>& metric_labels = {});

  /**
   * Register a newly flushed batch.
   *
   * Assigns the next CommitEpoch to this batch and enqueues its callbacks.
   * On success, @p callbacks is moved from. On failure, @p callbacks is left
   * untouched so the caller can fire failure callbacks.
   *
   * @param entry_count Number of entries in the batch
   * @param byte_size   Approximate on-disk byte size of the batch
   * @param callbacks   Per-entry durable callbacks (may contain nullptr)
   * @param error       Output error if controller is unhealthy
   * @return Assigned epoch, or 0 on error
   */
  uint64_t RegisterFlushedBatch(size_t entry_count, size_t byte_size,
                                std::vector<DurableCallback>& callbacks, Status& error);

  /**
   * Acquire the inclusive epoch range that the next sync must cover.
   *
   * Returns std::nullopt if there is nothing to sync or if a sync is already
   * in flight. On success, the caller must eventually call either
   * OnSyncSuccess(end_epoch) or OnSyncFailure(end_epoch, ...).
   *
   * The returned range is always contiguous from durable_epoch_ + 1 to the
   * latest pending epoch.
   */
  std::optional<std::pair<uint64_t, uint64_t>> AcquireSyncRange();

  /** Called after a successful Persister::Sync(). */
  void OnSyncSuccess(uint64_t end_epoch);

  /** Called after a failed Persister::Sync(). */
  void OnSyncFailure(uint64_t end_epoch, Status error);

  /**
   * Determine whether the sync worker should sync immediately.
   *
   * @param now Current time point (usually steady_clock::now())
   */
  bool ShouldSyncNow(std::chrono::steady_clock::time_point now) const;

  /**
   * Compute the next sleep duration for the sync worker.
   *
   * @param now Current time point
   */
  std::chrono::milliseconds NextSyncDelay(std::chrono::steady_clock::time_point now) const;

  /** Current controller statistics. */
  struct Stats {
    uint64_t pending_epochs = 0;
    uint64_t unsynced_entries = 0;
    uint64_t unsynced_bytes = 0;
    uint64_t durable_epoch = 0;
    uint64_t next_epoch = 0;
    uint64_t sync_in_progress_end = 0;
    bool healthy = true;
  };
  Stats GetStats() const;

  /** True if no sync failure has occurred. */
  bool IsHealthy() const;

  /** Request an immediate sync regardless of interval/batch thresholds. */
  void RequestSync();

 private:
  struct PendingEpoch {
    uint64_t epoch;
    size_t entry_count;
    size_t byte_size;
    std::chrono::steady_clock::time_point flush_time;
    std::vector<DurableCallback> callbacks;
  };

  bool IsSyncPolicyEnabled() const;
  size_t UnsyncedEntryCountLocked() const;
  size_t UnsyncedByteCountLocked() const;
  void UpdateMetricsLocked() const;

  LogPersistenceConfig config_;
  MetricsRegistry* metrics_;
  std::map<std::string, std::string> metric_labels_;

  mutable std::mutex mutex_;

  uint64_t next_epoch_ = 1;
  uint64_t durable_epoch_ = 0;
  uint64_t sync_in_progress_end_ = 0;
  std::deque<PendingEpoch> pending_;
  bool healthy_ = true;
  bool sync_requested_ = false;
  Status last_error_;

  std::chrono::steady_clock::time_point last_sync_time_;
};

}  // namespace rollingraft
