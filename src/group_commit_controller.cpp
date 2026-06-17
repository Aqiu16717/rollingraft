/**
 * @file group_commit_controller.cpp
 * @brief GroupCommitController implementation
 */

#include "group_commit_controller.h"

#include <algorithm>
#include <utility>

#include "rollingraft/logger.h"

namespace rollingraft {

namespace {

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

}  // namespace

GroupCommitController::GroupCommitController(const LogPersistenceConfig& config)
    : config_(config), last_sync_time_(steady_clock::now()) {}

uint64_t GroupCommitController::RegisterFlushedBatch(
    size_t entry_count,
    size_t byte_size,
    std::vector<DurableCallback>& callbacks,
    Status& error) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!healthy_) {
    error = last_error_.ok() ? Status::Error("Group commit controller is unhealthy")
                             : last_error_;
    return 0;
  }

  uint64_t epoch = next_epoch_++;
  pending_.push_back(
      PendingEpoch{epoch, entry_count, byte_size, std::move(callbacks)});

  LOG_DEBUG("GroupCommit: registered epoch={}, entries={}, bytes={}, pending={}",
            epoch, entry_count, byte_size, pending_.size());

  return epoch;
}

std::optional<std::pair<uint64_t, uint64_t>>
GroupCommitController::AcquireSyncRange() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!healthy_) {
    return std::nullopt;
  }

  if (sync_in_progress_end_ != 0) {
    // A sync is already in flight.
    return std::nullopt;
  }

  if (pending_.empty()) {
    return std::nullopt;
  }

  uint64_t begin = durable_epoch_ + 1;
  uint64_t end = pending_.back().epoch;
  sync_in_progress_end_ = end;

  LOG_DEBUG("GroupCommit: acquired sync range [{}, {}]", begin, end);
  return std::make_pair(begin, end);
}

void GroupCommitController::OnSyncSuccess(uint64_t end_epoch) {
  std::vector<std::pair<std::vector<DurableCallback>, Status>> to_fire;

  {
    std::lock_guard<std::mutex> lock(mutex_);

    if (sync_in_progress_end_ == 0 || end_epoch != sync_in_progress_end_) {
      LOG_WARN("GroupCommit: unexpected OnSyncSuccess({}), in_progress_end={}",
               end_epoch, sync_in_progress_end_);
      return;
    }

    last_sync_time_ = steady_clock::now();
    sync_in_progress_end_ = 0;
    sync_requested_ = false;

    while (!pending_.empty() && pending_.front().epoch <= end_epoch) {
      auto& front = pending_.front();
      durable_epoch_ = front.epoch;
      to_fire.emplace_back(std::move(front.callbacks), Status::OK());
      pending_.pop_front();
    }

    LOG_DEBUG("GroupCommit: sync success up to epoch={}, durable_epoch={}",
              end_epoch, durable_epoch_);
  }

  for (auto& [callbacks, status] : to_fire) {
    for (auto& cb : callbacks) {
      if (cb) {
        cb(status);
      }
    }
  }
}

void GroupCommitController::OnSyncFailure(uint64_t end_epoch, Status error) {
  std::vector<std::pair<std::vector<DurableCallback>, Status>> to_fire;

  {
    std::lock_guard<std::mutex> lock(mutex_);

    if (sync_in_progress_end_ == 0 || end_epoch != sync_in_progress_end_) {
      LOG_WARN("GroupCommit: unexpected OnSyncFailure({}), in_progress_end={}",
               end_epoch, sync_in_progress_end_);
      return;
    }

    healthy_ = false;
    last_error_ = error;
    sync_in_progress_end_ = 0;
    sync_requested_ = false;

    LOG_ERROR("GroupCommit: sync failed at epoch={}, error={}", end_epoch,
              error.ToString());

    while (!pending_.empty()) {
      auto& front = pending_.front();
      to_fire.emplace_back(std::move(front.callbacks), error);
      pending_.pop_front();
    }
  }

  for (auto& [callbacks, status] : to_fire) {
    for (auto& cb : callbacks) {
      if (cb) {
        cb(status);
      }
    }
  }
}

bool GroupCommitController::ShouldSyncNow(
    std::chrono::steady_clock::time_point now) const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!healthy_ || pending_.empty() || sync_in_progress_end_ != 0) {
    return false;
  }

  if (!IsSyncPolicyEnabled()) {
    // When group commit is disabled, we never trigger background sync.
    // Sync happens inline in the flush path.
    return false;
  }

  if (sync_requested_) {
    return true;
  }

  auto policy = config_.sync_policy;

  if (policy == LogPersistenceConfig::SyncPolicy::kSyncByInterval ||
      policy == LogPersistenceConfig::SyncPolicy::kSyncAdaptive) {
    auto elapsed = duration_cast<milliseconds>(now - last_sync_time_).count();
    if (elapsed >= static_cast<int64_t>(config_.group_commit_interval_ms)) {
      return true;
    }
  }

  if (policy == LogPersistenceConfig::SyncPolicy::kSyncByBatchSize ||
      policy == LogPersistenceConfig::SyncPolicy::kSyncAdaptive) {
    if (UnsyncedEntryCountLocked() >= config_.group_commit_max_entries) {
      return true;
    }
    if (UnsyncedByteCountLocked() >= config_.group_commit_max_bytes) {
      return true;
    }
  }

  return false;
}

std::chrono::milliseconds GroupCommitController::NextSyncDelay(
    std::chrono::steady_clock::time_point now) const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!IsSyncPolicyEnabled() || pending_.empty()) {
    return std::chrono::milliseconds(config_.group_commit_interval_ms);
  }

  if (config_.sync_policy == LogPersistenceConfig::SyncPolicy::kSyncByBatchSize) {
    // No interval pressure; just poll periodically to check thresholds.
    return std::chrono::milliseconds(1);
  }

  auto elapsed = duration_cast<milliseconds>(now - last_sync_time_).count();
  int64_t remaining =
      static_cast<int64_t>(config_.group_commit_interval_ms) - elapsed;
  if (remaining <= 0) {
    return std::chrono::milliseconds(0);
  }
  return std::chrono::milliseconds(remaining);
}

GroupCommitController::Stats GroupCommitController::GetStats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  Stats stats;
  stats.pending_epochs = pending_.size();
  stats.unsynced_entries = UnsyncedEntryCountLocked();
  stats.unsynced_bytes = UnsyncedByteCountLocked();
  stats.durable_epoch = durable_epoch_;
  stats.next_epoch = next_epoch_;
  stats.sync_in_progress_end = sync_in_progress_end_;
  stats.healthy = healthy_;
  return stats;
}

bool GroupCommitController::IsHealthy() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return healthy_;
}

void GroupCommitController::RequestSync() {
  std::lock_guard<std::mutex> lock(mutex_);
  sync_requested_ = true;
}

bool GroupCommitController::IsSyncPolicyEnabled() const {
  return config_.sync_policy != LogPersistenceConfig::SyncPolicy::kSyncEveryWrite;
}

size_t GroupCommitController::UnsyncedEntryCountLocked() const {
  size_t total = 0;
  for (const auto& p : pending_) {
    total += p.entry_count;
  }
  return total;
}

size_t GroupCommitController::UnsyncedByteCountLocked() const {
  size_t total = 0;
  for (const auto& p : pending_) {
    total += p.byte_size;
  }
  return total;
}

}  // namespace rollingraft
