#include "mock_timer.h"

#include <algorithm>

namespace rollingraft {

void MockTimerService::Stop() {
  std::lock_guard<std::mutex> lock(mutex_);
  timers_.clear();
}

TimerId MockTimerService::SetTimeout(std::chrono::milliseconds delay,
                                     std::function<void()> callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  TimerId id = next_id_++;
  timers_[id] = {current_time_ + delay, std::chrono::milliseconds(0),
                 std::move(callback), false};
  return id;
}

TimerId MockTimerService::SetInterval(std::chrono::milliseconds interval,
                                      std::function<void()> callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  TimerId id = next_id_++;
  timers_[id] = {current_time_ + interval, interval, std::move(callback),
                 false};
  return id;
}

bool MockTimerService::CancelTimer(TimerId id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = timers_.find(id);
  if (it == timers_.end() || it->second.cancelled) {
    return false;
  }
  it->second.cancelled = true;
  return true;
}

bool MockTimerService::IsTimerActive(TimerId id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = timers_.find(id);
  return it != timers_.end() && !it->second.cancelled;
}

void MockTimerService::Advance(std::chrono::milliseconds delta) {
  std::vector<std::function<void()>> callbacks;
  std::vector<TimerId> to_reschedule;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    current_time_ += delta;

    for (auto& [id, timer] : timers_) {
      if (timer.cancelled) continue;
      if (timer.deadline <= current_time_) {
        callbacks.push_back(timer.callback);
        if (timer.interval.count() > 0) {
          to_reschedule.push_back(id);
        } else {
          timer.cancelled = true;
        }
      }
    }

    // Reschedule interval timers
    for (TimerId id : to_reschedule) {
      auto it = timers_.find(id);
      if (it != timers_.end()) {
        it->second.deadline += it->second.interval;
      }
    }
  }

  // Execute callbacks outside the lock
  for (auto& cb : callbacks) {
    if (cb) cb();
  }
}

size_t MockTimerService::TimerCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t count = 0;
  for (const auto& [id, timer] : timers_) {
    if (!timer.cancelled) ++count;
  }
  return count;
}

}  // namespace rollingraft
