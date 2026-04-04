/**
 * @file asio_timer_service.cpp
 * @brief Asio-based timer service implementation
 */

#include "asio_timer_service.h"
#include "rollingraft/logger.h"

namespace rollingraft {

AsioTimerService::AsioTimerService() : owns_io_context_(true), io_ptr_(&io_context_) {}

AsioTimerService::AsioTimerService(asio::io_context& external_io)
    : owns_io_context_(false), io_ptr_(&external_io) {}

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
    work_guard_ = std::make_unique<asio::executor_work_guard<asio::io_context::executor_type>>(
        io_context_.get_executor());
    io_thread_ = std::thread([this]() {
      try {
        io_context_.run();
      } catch (const std::exception& e) {
        LOG_ERROR("TimerService IO error: {}", e.what());
      }
    });
  }

  LOG_INFO("AsioTimerService started");
}

void AsioTimerService::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG_INFO("Stopping AsioTimerService...");

  {
    std::lock_guard<std::mutex> lock(timers_mutex_);
    for (auto& [id, timer] : timers_) {
      timer->asio_timer->cancel();
    }
    timers_.clear();
  }

  if (owns_io_context_) {
    work_guard_.reset();
    io_context_.stop();
    if (io_thread_.joinable()) {
      io_thread_.join();
    }
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

  timer->asio_timer->async_wait([this, id, timer](std::error_code ec) {
    if (!ec) {
      if (timer->callback) {
        timer->callback();
      }
    }
    std::lock_guard<std::mutex> lock(timers_mutex_);
    timers_.erase(id);
  });

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

  std::function<void(std::error_code)> handler;
  handler = [this, id, timer, &handler](std::error_code ec) {
    if (ec) {
      std::lock_guard<std::mutex> lock(timers_mutex_);
      timers_.erase(id);
      return;
    }

    if (timer->callback) {
      timer->callback();
    }

    if (running_) {
      timer->asio_timer->expires_after(timer->interval);
      timer->asio_timer->async_wait(handler);
    }
  };

  timer->asio_timer->async_wait(handler);

  return id;
}

bool AsioTimerService::CancelTimer(TimerId timer_id) {
  if (timer_id == kInvalidTimerId) {
    return false;
  }

  std::lock_guard<std::mutex> lock(timers_mutex_);
  auto it = timers_.find(timer_id);
  if (it == timers_.end()) {
    return false;
  }

  it->second->asio_timer->cancel();
  timers_.erase(it);
  return true;
}

bool AsioTimerService::IsTimerActive(TimerId timer_id) const {
  std::lock_guard<std::mutex> lock(timers_mutex_);
  return timers_.find(timer_id) != timers_.end();
}

// static
std::unique_ptr<TimerService> TimerService::CreateDefault() {
  return std::make_unique<AsioTimerService>();
}

}  // namespace rollingraft
