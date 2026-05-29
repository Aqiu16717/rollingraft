/**
 * @file log_persister.cpp
 * @brief Batched log persistence implementation
 *
 * Implements asynchronous batched writes to persistent storage.
 * Uses a background thread for flushing buffered entries.
 */

#include "rollingraft/log_persister.h"

#include <chrono>
#include <cstring>
#include <future>
#include <iterator>

#include "rollingraft/logger.h"

#if defined(__unix__) || defined(__unix) || \
    (defined(__APPLE__) && defined(__MACH__))
#define ROLLINGRAFT_POSIX
#include <sys/statvfs.h>
#endif

namespace rollingraft {

LogPersister::LogPersister(std::shared_ptr<Persister> persister,
                           LogPersistenceConfig config)
    : persister_(std::move(persister)),
      config_(config),
      async_state_(std::make_shared<AsyncState>()) {
  async_state_->persister = persister_;
}

LogPersister::~LogPersister() {
  async_state_->shutdown.store(true, std::memory_order_release);

  if (running_) {
    Stop();
  }

  // Wait unconditionally for all pending async truncations to complete.
  // AsyncState holds a shared_ptr<Persister>, so the Persister object
  // stays alive until the last executor lambda releases its reference.
  std::unique_lock<std::mutex> lock(async_state_->cv_mutex);
  async_state_->cv.wait(lock, [this] {
    return async_state_->pending_count.load(std::memory_order_acquire) == 0;
  });
}

void LogPersister::Start() {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  if (running_) {
    return;
  }

  running_ = true;
  healthy_ = true;

  bool group_commit = config_.group_commit_interval_ms > 0;
  if (persister_ && config_.sync_on_critical && !group_commit) {
    persister_->SetSyncOnWrite(true);
  }

  flush_thread_ = std::thread(&LogPersister::BackgroundFlushLoop, this);
  if (group_commit) {
    sync_thread_ = std::thread(&LogPersister::BackgroundSyncLoop, this);
  }

  LOG_INFO("LogPersister started (batch_size={}, interval={}ms, sync={}, group_commit={}ms)",
           config_.batch_size, config_.batch_interval_ms,
           config_.sync_on_critical, config_.group_commit_interval_ms);
}

void LogPersister::Stop() {
  {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    if (!running_) {
      return;
    }
    running_ = false;
  }

  flush_cv_.notify_all();

  if (flush_thread_.joinable()) {
    flush_thread_.join();
  }
  if (sync_thread_.joinable()) {
    sync_thread_.join();
  }

  // Final flush and sync before stopping
  DoFlush();
  if (config_.group_commit_interval_ms > 0 && persister_) {
    auto sync_status = persister_->Sync();
    if (!sync_status.ok()) {
      LOG_ERROR("Final group commit sync failed: {}", sync_status.ToString());
    }
  }

  LOG_INFO("LogPersister stopped (total_flushed={}, total_ops={})",
           total_flushed_.load(), total_flush_ops_.load());
}

void LogPersister::Append(const RaftLogEntry& entry, FlushCallback callback) {
  std::unique_lock<std::mutex> lock(buffer_mutex_);

  if (!healthy_) {
    // Try to recover if disk space is now available
    auto space_status = CheckDiskSpace();
    if (space_status.ok()) {
      healthy_ = true;
      {
        std::lock_guard<std::mutex> err_lock(error_mutex_);
        last_error_.clear();
      }
      LOG_INFO("LogPersister recovered from disk-full state");
    } else {
      LOG_WARN("LogPersister is unhealthy, dropping append for index {}",
               entry.index_);
      std::string error;
      {
        std::lock_guard<std::mutex> err_lock(error_mutex_);
        error = last_error_;
      }
      auto cb = std::move(callback);
      lock.unlock();
      if (cb) {
        cb(Status::Error("Persister is unhealthy: " + error));
      }
      return;
    }
  }

  PendingEntry pending;
  pending.entry = entry;
  pending.callback = std::move(callback);
  buffer_.push_back(std::move(pending));

  // Trigger flush if batch is full
  if (buffer_.size() >= config_.batch_size) {
    flush_cv_.notify_one();
  }
}

Status LogPersister::AppendSync(const RaftLogEntry& entry,
                                std::chrono::milliseconds timeout) {
  auto shared_promise = std::make_shared<std::promise<Status>>();
  auto future = shared_promise->get_future();

  Append(entry, [shared_promise](Status s) { shared_promise->set_value(s); });

  if (future.wait_for(timeout) == std::future_status::timeout) {
    return Status::Error("AppendSync timeout");
  }

  return future.get();
}

Status LogPersister::FlushSync() { return FlushSync(std::chrono::seconds(5)); }

Status LogPersister::FlushSync(std::chrono::milliseconds timeout) {
  // Wait for current buffer to be flushed
  TriggerFlush();

  // Wait for flush to complete (with timeout)
  std::unique_lock<std::mutex> lock(buffer_mutex_);
  bool flushed = flush_cv_.wait_for(lock, timeout, [this] {
    return (buffer_.empty() && !flush_in_progress_) || !healthy_;
  });

  if (!flushed) {
    return Status::Error("Flush timeout");
  }

  if (!healthy_) {
    std::lock_guard<std::mutex> err_lock(error_mutex_);
    return Status::Error("Flush failed: " + last_error_);
  }

  return Status::OK();
}

void LogPersister::TriggerFlush() {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  flush_cv_.notify_one();
}

Status LogPersister::TruncatePrefix(uint64_t before_index) {
  // Ensure all buffered entries are persisted before truncation.
  // Use a shorter timeout to avoid blocking Raft event loop.
  auto status = FlushSync(std::chrono::seconds(1));
  if (!status.ok()) {
    return status;
  }

  if (!persister_) {
    return Status::Error("Persister is null");
  }

  return persister_->TruncatePrefix(before_index);
}

void LogPersister::TruncatePrefixAsync(uint64_t before_index,
                                       TruncateCallback callback) {
  // 1. Check shutdown
  if (async_state_->shutdown.load(std::memory_order_acquire)) {
    if (callback) {
      callback(Status::Error("LogPersister is shut down"));
    }
    return;
  }

  // 2. Increment pending_count BEFORE any long operation.
  // This ensures ~LogPersister() cannot return while FlushSync
  // or the async task is still in flight.
  async_state_->pending_count.fetch_add(1, std::memory_order_release);

  // 3. Drain buffer synchronously (fast — typically empty or small batch)
  auto status = FlushSync(std::chrono::seconds(1));
  if (!status.ok()) {
    if (callback) {
      callback(status);
    }
    async_state_->pending_count.fetch_sub(1, std::memory_order_release);
    async_state_->cv.notify_all();
    return;
  }

  // 4. Re-check shutdown (may have been set during FlushSync)
  if (async_state_->shutdown.load(std::memory_order_acquire)) {
    if (callback) {
      callback(Status::Error("LogPersister is shut down"));
    }
    async_state_->pending_count.fetch_sub(1, std::memory_order_release);
    async_state_->cv.notify_all();
    return;
  }

  if (!persister_) {
    if (callback) {
      callback(Status::Error("Persister is null"));
    }
    async_state_->pending_count.fetch_sub(1, std::memory_order_release);
    async_state_->cv.notify_all();
    return;
  }

  // 5. Capture shared state by value (keeps AsyncState + Persister alive)
  auto state = async_state_;  // shared_ptr copy
  auto cb = std::move(callback);

  auto do_truncate = [state, before_index, cb]() mutable {
    if (state->shutdown.load(std::memory_order_acquire)) {
      if (cb) {
        cb(Status::Error("Truncation cancelled: LogPersister shut down"));
      }
      state->pending_count.fetch_sub(1, std::memory_order_release);
      state->cv.notify_all();
      return;
    }

    auto status = state->persister->TruncatePrefix(before_index);
    if (cb) {
      cb(status);
    }
    state->pending_count.fetch_sub(1, std::memory_order_release);
    state->cv.notify_all();
  };

  // 6. Dispatch
  if (config_.executor) {
    config_.executor(std::move(do_truncate));
  } else {
    // No executor configured — run synchronously on caller thread.
    // This path is used in unit tests without ASIO infrastructure.
    do_truncate();
  }
}

std::vector<RaftLogEntry> LogPersister::Restore(uint64_t start_index) {
  std::vector<RaftLogEntry> entries;

  if (!persister_) {
    LOG_ERROR("Cannot restore: persister is null");
    return entries;
  }

  // Get last log info from persister
  auto [last_index, last_term] = persister_->GetLastLogInfo();
  LOG_INFO("Restoring logs from {} to {} (last_index={}, last_term={})",
           start_index, last_index, last_index, last_term);

  if (last_index < start_index) {
    // Nothing to restore
    return entries;
  }

  // Read entries in batches to avoid memory issues
  const size_t kBatchSize = 1000;
  for (uint64_t batch_start = start_index; batch_start <= last_index;
       batch_start += kBatchSize) {
    uint64_t batch_end = std::min(batch_start + kBatchSize, last_index + 1);

    std::vector<RaftLogEntry> batch;
    auto status = persister_->GetEntries(batch_start, batch_end, &batch);

    if (!status.ok()) {
      LOG_ERROR("Failed to restore entries [{}-{}): {}", batch_start, batch_end,
                status.ToString());
      break;
    }

    entries.insert(entries.end(), batch.begin(), batch.end());
  }

  LOG_INFO("Restored {} log entries from persistent storage", entries.size());
  return entries;
}

Status LogPersister::Sync() {
  if (!persister_) {
    return Status::OK();
  }
  return persister_->Sync();
}

size_t LogPersister::GetPendingCount() const {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  return buffer_.size();
}

bool LogPersister::IsHealthy() const { return healthy_.load(); }

std::string LogPersister::GetLastError() const {
  std::lock_guard<std::mutex> lock(error_mutex_);
  return last_error_;
}

void LogPersister::BackgroundFlushLoop() {
  LOG_DEBUG("LogPersister background flush thread started");

  while (running_) {
    std::unique_lock<std::mutex> lock(buffer_mutex_);

    // Wait until we need to flush (timeout or notification)
    auto timeout = std::chrono::milliseconds(config_.batch_interval_ms);
    flush_cv_.wait_for(lock, timeout, [this] {
      return !running_ || buffer_.size() >= config_.batch_size;
    });

    // Release lock before flushing
    lock.unlock();

    // Perform the flush (without sync — sync is handled by BackgroundSyncLoop)
    if (!buffer_.empty()) {
      DoFlush();
    }
  }

  LOG_DEBUG("LogPersister background flush thread stopped");
}

void LogPersister::BackgroundSyncLoop() {
  LOG_DEBUG("LogPersister background sync thread started");

  auto interval = std::chrono::milliseconds(config_.group_commit_interval_ms);
  while (running_) {
    std::this_thread::sleep_for(interval);
    if (!running_) break;

    if (persister_) {
      auto sync_status = persister_->Sync();
      if (!sync_status.ok()) {
        LOG_ERROR("LogPersister group commit sync failed: {}",
                  sync_status.ToString());
      } else {
        LOG_DEBUG("LogPersister group commit synced");
      }
    }
  }

  LOG_DEBUG("LogPersister background sync thread stopped");
}

bool LogPersister::DoFlush() {
  // Move entries from buffer to local batch
  std::vector<PendingEntry> batch;
  {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    if (buffer_.empty()) {
      return true;
    }
    batch.swap(buffer_);
    flush_in_progress_ = true;
  }

  // Check disk space before writing (also attempts recovery if unhealthy)
  auto space_status = CheckDiskSpace();
  if (space_status.ok() && !healthy_) {
    healthy_ = true;
    {
      std::lock_guard<std::mutex> err_lock(error_mutex_);
      last_error_.clear();
    }
    LOG_INFO("LogPersister recovered from disk-full state during flush");
  }
  if (!space_status.ok()) {
    LOG_ERROR("Disk space check failed: {}", space_status.ToString());
    {
      std::lock_guard<std::mutex> err_lock(error_mutex_);
      last_error_ = space_status.ToString();
    }
    healthy_ = false;

    // Put entries back to buffer
    {
      std::lock_guard<std::mutex> lock(buffer_mutex_);
      buffer_.insert(buffer_.end(), std::make_move_iterator(batch.begin()),
                     std::make_move_iterator(batch.end()));
      flush_in_progress_ = false;
    }
    flush_cv_.notify_all();  // Wake waiters so they see !healthy_
    return false;
  }

  // Extract entries for writing
  std::vector<RaftLogEntry> entries;
  entries.reserve(batch.size());
  for (auto& pending : batch) {
    entries.push_back(pending.entry);  // Copy instead of move to preserve
                                       // batch entries on retry
  }

  // Write to storage
  auto status = WriteBatch(entries);
  if (!status.ok()) {
    LOG_ERROR("Failed to flush {} entries: {}", entries.size(),
              status.ToString());
    {
      std::lock_guard<std::mutex> err_lock(error_mutex_);
      last_error_ = status.ToString();
    }
    healthy_ = false;

    // Notify callbacks of failure
    for (auto& pending : batch) {
      if (pending.callback) {
        auto cb = std::move(pending.callback);
        pending.callback = nullptr;
        cb(Status::Error("Flush failed: " + last_error_));
      }
    }

    // Put entries back to buffer for retry
    {
      std::lock_guard<std::mutex> lock(buffer_mutex_);
      buffer_.insert(buffer_.end(), std::make_move_iterator(batch.begin()),
                     std::make_move_iterator(batch.end()));
      flush_in_progress_ = false;
    }
    flush_cv_.notify_all();  // Wake waiters so they see !healthy_
    return false;
  }

  // Success
  total_flushed_ += entries.size();
  ++total_flush_ops_;

  // Notify callbacks of success
  for (auto& pending : batch) {
    if (pending.callback) {
      auto cb = std::move(pending.callback);
      pending.callback = nullptr;
      cb(Status::OK());
    }
  }

  {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    flush_in_progress_ = false;
  }

  LOG_DEBUG("Flushed {} log entries (total_flushed={})", entries.size(),
            total_flushed_.load());

  // Wake up any threads waiting for buffer to drain (e.g. FlushSync)
  flush_cv_.notify_all();

  return true;
}

Status LogPersister::WriteBatch(const std::vector<RaftLogEntry>& entries) {
  if (!persister_) {
    return Status::Error("Persister is null");
  }

  if (entries.empty()) {
    return Status::OK();
  }

  return persister_->AppendEntries(entries);
}

Status LogPersister::CheckDiskSpace() {
#ifdef ROLLINGRAFT_POSIX
  if (config_.data_dir.empty()) {
    return Status::OK();
  }

  struct statvfs buf;
  if (statvfs(config_.data_dir.c_str(), &buf) != 0) {
    return Status::Error("Cannot check disk space for " + config_.data_dir);
  }

  uint64_t available =
      static_cast<uint64_t>(buf.f_bavail) * static_cast<uint64_t>(buf.f_frsize);
  if (available < config_.min_disk_space_bytes) {
    return Status::Error(
        "Insufficient disk space: available=" + std::to_string(available) +
        " required=" + std::to_string(config_.min_disk_space_bytes));
  }
#endif
  return Status::OK();
}

}  // namespace rollingraft
