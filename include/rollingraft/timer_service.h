/**
 * @file timer_service.h
 * @brief Abstract timer service interface
 *
 * Provides timer functionality for Raft timeouts (election, heartbeat).
 * Implementations can use different timer backends (Asio, etc.).
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

#include <rollingraft/status.h>

namespace rollingraft {

/** Timer identifier type. */
using TimerId = uint64_t;

/** Invalid timer ID constant. */
constexpr TimerId kInvalidTimerId = 0;

/**
 * Abstract timer service interface.
 *
 * Manages one-shot and periodic timers for Raft protocol timeouts.
 * Used for election timeouts and heartbeat intervals.
 *
 * Thread-safety: Implementations must be thread-safe.
 */
class TimerService {
 public:
  virtual ~TimerService() = default;

  // ==================== Lifecycle ====================

  /**
   * Current time as seen by this timer service.
   *
   * Defaults to the real wall clock. Simulated timer services (deterministic
   * tests) override this so that Raft timeout deadlines advance with the
   * simulated clock; without it, tick callbacks fire on simulated time while
   * deadline comparisons run on real time, and elections never trigger on
   * fast machines.
   */
  virtual std::chrono::steady_clock::time_point Now() const {
    return std::chrono::steady_clock::now();
  }

  /**
   * Start the timer service.
   *
   * Must be called before using any timer methods.
   */
  virtual void Start() = 0;

  /**
   * Stop the timer service.
   *
   * Cancels all pending timers and waits for executing callbacks.
   */
  virtual void Stop() = 0;

  // ==================== Timer Operations ====================

  /**
   * Set a one-shot timer.
   *
   * @param delay Duration to wait before triggering
   * @param callback Function to call when timer expires
   * @return Timer ID for cancellation
   */
  virtual TimerId SetTimeout(std::chrono::milliseconds delay, std::function<void()> callback) = 0;

  /**
   * Set a periodic timer.
   *
   * @param interval Duration between triggers
   * @param callback Function to call on each interval
   * @return Timer ID for cancellation
   */
  virtual TimerId SetInterval(std::chrono::milliseconds interval,
                              std::function<void()> callback) = 0;

  /**
   * Cancel a pending timer.
   *
   * @param timer_id Timer ID to cancel
   * @return True if cancelled, false if already triggered or not found
   */
  virtual bool CancelTimer(TimerId timer_id) = 0;

  /**
   * Check if a timer is still active.
   *
   * @param timer_id Timer ID to check
   * @return True if timer exists and hasn't triggered
   */
  virtual bool IsTimerActive(TimerId timer_id) const = 0;

  // ==================== Factory Methods ====================

  /**
   * Create default timer service implementation (Asio-based).
   * @return Unique pointer to timer service
   */
  static std::unique_ptr<TimerService> CreateDefault();
};

}  // namespace rollingraft
