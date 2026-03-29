#pragma once

#include <functional>
#include <rollingraft/status.h>

namespace rollingraft {

// Timer identifier
using TimerId = uint64_t;

// Invalid timer ID
constexpr TimerId kInvalidTimerId = 0;

class TimerService {
 public:
  virtual ~TimerService() = default;

  // ========== 生命周期 ==========

  // Start timer service
  // Must be started before calling any Set* methods
  virtual void Start() = 0;

  // Stop timer service
  // Cancel all pending timers, wait for executing callbacks to complete
  virtual void Stop() = 0;

  // ========== Timer Operations ==========

  // Set one-shot timer
  // @param delay: delay duration
  // @param callback: timeout callback
  // @return: timer ID for cancellation
  virtual TimerId SetTimeout(std::chrono::milliseconds delay,
                             std::function<void()> callback) = 0;

  // Set periodic timer
  // @param interval: interval duration
  // @param callback: callback triggered on each interval
  // @return: timer ID
  virtual TimerId SetInterval(std::chrono::milliseconds interval,
                              std::function<void()> callback) = 0;

  // Cancel timer
  // @param timer_id: timer ID to cancel
  // @return: true if cancelled successfully, false if already triggered or not found
  virtual bool CancelTimer(TimerId timer_id) = 0;

  // Check if timer exists
  virtual bool IsTimerActive(TimerId timer_id) const = 0;

  // ========== Factory Methods ==========

  // Create default implementation (Asio-based)
  static std::unique_ptr<TimerService> CreateDefault();
};

}  // namespace rollingraft
