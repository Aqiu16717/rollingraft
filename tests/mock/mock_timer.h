#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <vector>

#include "rollingraft/timer_service.h"

namespace rollingraft {

/**
 * Mock timer service for testing.
 * Allows manual control of time advancement.
 */
class MockTimerService : public TimerService {
 public:
  MockTimerService() = default;
  ~MockTimerService() override = default;

  // Non-copyable, non-movable
  MockTimerService(const MockTimerService&) = delete;
  MockTimerService& operator=(const MockTimerService&) = delete;

  void Start() override {}
  void Stop() override;

  TimerId SetTimeout(std::chrono::milliseconds delay, std::function<void()> callback) override;

  TimerId SetInterval(std::chrono::milliseconds interval, std::function<void()> callback) override;

  bool CancelTimer(TimerId id) override;

  bool IsTimerActive(TimerId id) const override;

  // Test control interface

  /**
   * Advance time by the specified amount.
   * Triggers all timers whose deadline has been reached.
   */
  void Advance(std::chrono::milliseconds delta);

  /**
   * Get current simulated time.
   */
  std::chrono::milliseconds CurrentTime() const { return current_time_; }

  /**
   * Get number of active timers.
   */
  size_t TimerCount() const;

 private:
  struct TimerInfo {
    std::chrono::milliseconds deadline;
    std::chrono::milliseconds interval;  // 0 for one-shot
    std::function<void()> callback;
    bool cancelled = false;
  };

  std::map<TimerId, TimerInfo> timers_;
  mutable std::mutex mutex_;
  std::atomic<TimerId> next_id_{1};
  std::chrono::milliseconds current_time_{0};
};

}  // namespace rollingraft
