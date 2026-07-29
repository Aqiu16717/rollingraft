/**
 * @file asio_timer_service.cpp
 * @brief Asio-based timer service implementation
 */

#include "asio_timer_service.h"

#include "rollingraft/logger.h"

namespace rollingraft {

AsioTimerService::AsioTimerService()
    : owns_io_context_(true), io_ptr_(&io_context_), strand_(io_context_) {}

AsioTimerService::AsioTimerService(asio::io_context& external_io)
    : owns_io_context_(false), io_ptr_(&external_io), strand_(external_io) {}

AsioTimerService::~AsioTimerService() {
  if (running_) {
    Stop();
  }
}

void AsioTimerService::Start() {
  if (running_.exchange(true)) {
    return;
  }

  LOG_INFO("Starting AsioTimerService...");

  if (owns_io_context_) {
    io_thread_exited_.store(false, std::memory_order_relaxed);
    work_guard_ = std::make_unique<asio::executor_work_guard<asio::io_context::executor_type>>(
        io_context_.get_executor());
    io_thread_ = std::thread([this]() {
      try {
        io_context_.run();
      } catch (const std::exception& e) {
        LOG_ERROR("TimerService IO error: {}", e.what());
      }
      io_thread_exited_.store(true, std::memory_order_release);
    });
  }

  LOG_INFO("AsioTimerService started");
}

void AsioTimerService::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG_INFO("Stopping AsioTimerService...");

  // Cancel all timers first
  // Copy timers outside lock, then cancel without holding lock
  // to avoid deadlock if cancel() immediately invokes handler
  std::unordered_map<TimerId, std::shared_ptr<Timer>> timers_copy;
  {
    std::lock_guard<std::mutex> lock(timers_mutex_);
    timers_copy = timers_;
  }
  for (auto& [id, timer] : timers_copy) {
    if (timer && timer->asio_timer && io_ptr_) {
      asio::post(*io_ptr_, [timer]() {
        try {
          timer->asio_timer->cancel();
        } catch (...) {
          // Ignore errors during cleanup
        }
      });
    }
  }

  if (owns_io_context_) {
    // Stop io_context before joining thread
    work_guard_.reset();
    io_context_.stop();
    // Post a no-op to wake up any thread blocked in kevent/epoll
    // (macOS kqueue may not wake on stop() alone)
    try {
      asio::post(io_context_, []() {});
    } catch (const std::exception& e) {
      LOG_WARN("AsioTimerService no-op post failed: {}", e.what());
    }

    if (io_thread_.joinable()) {
      // Join unconditionally. The previous poll-then-detach could leave a
      // still-running io thread behind: it would keep executing queued
      // callbacks that hold group keep-alive references, delaying group
      // destruction past store teardown (and risking use-after-free). If the
      // thread cannot exit here, that is a real bug to fix, not to mask.
      io_thread_.join();
    }
  }

  // Clear timers after stopping
  {
    std::lock_guard<std::mutex> lock(timers_mutex_);
    timers_.clear();
  }

  LOG_INFO("AsioTimerService stopped");
}

TimerId AsioTimerService::SetTimeout(std::chrono::milliseconds delay,
                                     std::function<void()> callback) {
  if (!running_) {
    LOG_ERROR("TimerService not started");
    return kInvalidTimerId;
  }

  auto timer = std::make_shared<Timer>();
  timer->callback = std::move(callback);
  timer->is_interval = false;
  timer->asio_timer = std::make_unique<asio::steady_timer>(*io_ptr_, delay);

  TimerId id;
  {
    std::lock_guard<std::mutex> lock(timers_mutex_);
    id = next_timer_id_++;
    timers_[id] = timer;
  }

  timer->asio_timer->async_wait(asio::bind_executor(strand_, [this, id, timer](std::error_code ec) {
    if (!ec) {
      if (timer->callback) {
        try {
          timer->callback();
        } catch (const std::exception& e) {
          LOG_ERROR("Timer callback exception: {}", e.what());
        } catch (...) {
          LOG_ERROR("Timer callback unknown exception");
        }
      }
    }
    std::lock_guard<std::mutex> lock(timers_mutex_);
    timers_.erase(id);
  }));

  return id;
}

TimerId AsioTimerService::SetInterval(std::chrono::milliseconds interval,
                                      std::function<void()> callback) {
  if (!running_) {
    LOG_ERROR("TimerService not started");
    return kInvalidTimerId;
  }

  auto timer = std::make_shared<Timer>();
  timer->callback = std::move(callback);
  timer->is_interval = true;
  timer->interval = interval;
  timer->asio_timer = std::make_unique<asio::steady_timer>(*io_ptr_, interval);

  TimerId id;
  {
    std::lock_guard<std::mutex> lock(timers_mutex_);
    id = next_timer_id_++;
    timers_[id] = timer;
  }

  // Use a shared_ptr to manage handler lifecycle
  auto handler = std::make_shared<std::function<void(std::error_code)>>();
  *handler = [this, id, timer, interval, handler](std::error_code ec) {
    if (ec) {
      if (ec != asio::error::operation_aborted) {
        LOG_DEBUG("Interval timer error: {}", ec.message());
      }
      std::lock_guard<std::mutex> lock(timers_mutex_);
      timers_.erase(id);
      return;
    }

    if (timer->callback && running_) {
      try {
        timer->callback();
      } catch (const std::exception& e) {
        LOG_ERROR("Interval timer callback exception: {}", e.what());
      } catch (...) {
        LOG_ERROR("Interval timer callback unknown exception");
      }
    }

    if (running_) {
      try {
        timer->asio_timer->expires_after(interval);
        timer->asio_timer->async_wait(asio::bind_executor(strand_, *handler));
      } catch (const std::exception& e) {
        LOG_ERROR("Failed to reschedule interval timer: {}", e.what());
      }
    }
  };

  timer->asio_timer->async_wait(asio::bind_executor(strand_, *handler));

  return id;
}

bool AsioTimerService::CancelTimer(TimerId timer_id) {
  if (timer_id == kInvalidTimerId) {
    return false;
  }

  std::shared_ptr<Timer> timer;
  {
    std::lock_guard<std::mutex> lock(timers_mutex_);
    auto it = timers_.find(timer_id);
    if (it == timers_.end()) {
      return false;
    }
    timer = it->second;
    timers_.erase(it);
  }

  if (timer && timer->asio_timer && io_ptr_) {
    // Post cancel through strand to serialize with handler execution.
    // asio::steady_timer is not thread-safe; concurrent cancel() and
    // expires_after()/async_wait() from different threads cause data race.
    // The strand guarantees sequential execution of all timer ops.
    asio::post(strand_, [timer]() {
      try {
        timer->asio_timer->cancel();
      } catch (...) {
        // Ignore errors during cancel
      }
    });
  }
  return true;
}

bool AsioTimerService::IsTimerActive(TimerId timer_id) const {
  if (timer_id == kInvalidTimerId) {
    return false;
  }
  std::lock_guard<std::mutex> lock(timers_mutex_);
  return timers_.find(timer_id) != timers_.end();
}

// static
std::unique_ptr<TimerService> TimerService::CreateDefault() {
  return std::make_unique<AsioTimerService>();
}

}  // namespace rollingraft
