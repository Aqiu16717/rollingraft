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

#include "rollingraft/logger.h"

namespace rollingraft {

LogPersister::LogPersister(std::unique_ptr<Persister> persister,
                           LogPersistenceConfig config)
    : persister_(std::move(persister)), config_(config) {}

LogPersister::~LogPersister() {
  if (running_) {
    Stop();
  }
}

void LogPersister::Start() {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  if (running_) {
    return;
  }

  running_ = true;
  healthy_ = true;

  if (persister_ && config_.sync_on_critical) {
    persister_->SetSyncOnWrite(true);
  }

  flush_thread_ = std::thread(&LogPersister::BackgroundFlushLoop, this);

  LOG_INFO("LogPersister started (batch_size={}, interval={}ms, sync={})",
           config_.batch_size, config_.batch_interval_ms,
           config_.sync_on_critical);
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

  // Final flush before stopping
  DoFlush();

  LOG_INFO("LogPersister stopped (total_flushed={}, total_ops={})",
           total_flushed_.load(), total_flush_ops_.load());
}

void LogPersister::Append(const RaftLogEntry& entry,
                           FlushCallback callback) {
  std::lock_guard<std::mutex> lock(buffer_mutex_);

  if (!healthy_) {
    LOG_WARN("LogPersister is unhealthy, dropping append for index {}",
             entry.index_);
    if (callback) {
      callback(Status::Error("Persister is unhealthy: " + last_error_));
    }
    return;
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

Status LogPersister::FlushSync() {
  return FlushSync(std::chrono::seconds(5));
}

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
  LOG_DEBUG("LogPersister background thread started");

  while (running_) {
    std::unique_lock<std::mutex> lock(buffer_mutex_);

    // Wait until we need to flush (timeout or notification)
    auto timeout = std::chrono::milliseconds(config_.batch_interval_ms);
    flush_cv_.wait_for(lock, timeout, [this] {
      return !running_ || buffer_.size() >= config_.batch_size;
    });

    // Release lock before flushing
    lock.unlock();

    // Perform the flush
    if (!buffer_.empty()) {
      DoFlush();
    }
  }

  LOG_DEBUG("LogPersister background thread stopped");
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

  // Check disk space
  auto space_status = CheckDiskSpace();
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
      buffer_.insert(buffer_.end(), batch.begin(), batch.end());
      flush_in_progress_ = false;
    }
    flush_cv_.notify_all();  // Wake waiters so they see !healthy_
    return false;
  }

  // Extract entries for writing
  std::vector<RaftLogEntry> entries;
  entries.reserve(batch.size());
  for (auto& pending : batch) {
    entries.push_back(std::move(pending.entry));
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
        cb(Status::Error("Flush failed: " + last_error_));
      }
    }

    // Put entries back to buffer for retry
    {
      std::lock_guard<std::mutex> lock(buffer_mutex_);
      buffer_.insert(buffer_.end(), batch.begin(), batch.end());
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
  // For now, skip disk space check on non-POSIX systems
  // This can be implemented later for specific platforms
  return Status::OK();
}

}  // namespace rollingraft
