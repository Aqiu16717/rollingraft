/**
 * @file asio_timer_service.h
 * @brief Asio-based timer service implementation
 *
 * Provides one-shot and periodic timers using Asio's steady_timer.
 * Thread-safe: all public methods can be called from any thread.
 *
 * Two usage modes:
 * 1. Internal io_context: Creates and manages its own io_context
 * 2. External io_context: Uses provided io_context for integration
 */
#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>

#include <asio.hpp>

#include "rollingraft/timer_service.h"

namespace rollingraft {

/**
 * Asio-based implementation of TimerService interface.
 *
 * Features:
 * - One-shot timers (SetTimeout)
 * - Periodic timers (SetInterval)
 * - Thread-safe timer cancellation
 * - Automatic resource cleanup on Stop()
 *
 * Implementation notes:
 * - Uses steady_timer for monotonic time (not affected by system clock changes)
 * - Interval timers reschedule themselves after callback execution
 * - All timer operations are serialized through io_context strand
 */
class AsioTimerService : public TimerService {
 public:
  /**
   * Create timer service with internal io_context.
   * Start() must be called to begin processing timers.
   */
  AsioTimerService();

  /**
   * Create timer service using external io_context.
   * Useful for integrating with existing Asio event loops.
   * @param external_io io_context to use for timer scheduling
   */
  explicit AsioTimerService(asio::io_context& external_io);

  ~AsioTimerService() override;

  /**
   * Start the timer service.
   * Spawns internal thread if using internal io_context.
   * @note Idempotent: safe to call multiple times
   */
  void Start() override;

  /**
   * Stop the timer service.
   * Cancels all active timers and stops the io_context.
   * Pending callbacks may not be invoked.
   */
  void Stop() override;

  /**
   * Schedule a one-shot timer.
   * @param delay Time to wait before invoking callback
   * @param callback Function to invoke after delay
   * @return TimerId unique identifier for cancellation
   * @note Callback is invoked from io_context thread
   */
  TimerId SetTimeout(std::chrono::milliseconds delay, std::function<void()> callback) override;

  /**
   * Schedule a periodic timer.
   * @param interval Time between callback invocations
   * @param callback Function to invoke periodically
   * @return TimerId unique identifier for cancellation
   * @note First invocation after interval, then repeats
   * @note Callback duration affects next interval timing
   */
  TimerId SetInterval(std::chrono::milliseconds interval, std::function<void()> callback) override;

  /**
   * Cancel a scheduled timer.
   * @param timer_id Id returned by SetTimeout/SetInterval
   * @return true if timer was active and cancelled
   * @note Callback will not be invoked after successful cancel
   */
  bool CancelTimer(TimerId timer_id) override;

  /**
   * Check if timer is still active.
   * @param timer_id Id to check
   * @return true if timer exists and has not fired/cancelled
   */
  bool IsTimerActive(TimerId timer_id) const override;

  /**
   * Get the underlying io_context (if using external or internal mode).
   * @return Pointer to io_context, or nullptr if not started
   */
  asio::io_context* GetIoContext() const { return io_ptr_; }

 private:
  struct Timer {
    // Underlying Asio timer
    std::unique_ptr<asio::steady_timer> asio_timer;
    // User callback
    std::function<void()> callback;
    // True for periodic
    bool is_interval;
    // Period for intervals
    std::chrono::milliseconds interval;
  };
  // Monotonically increasing timer ids
  TimerId next_timer_id_ = 1;

  // Timer storage - mutable for IsTimerActive const correctness
  std::unordered_map<TimerId, std::shared_ptr<Timer>> timers_;
  mutable std::mutex timers_mutex_;

  // io_context ownership mode
  // True if we created io_context_
  bool owns_io_context_ = true;
  // Internal io_context (if owned)
  asio::io_context io_context_;
  // Pointer to io_context_ or external
  asio::io_context* io_ptr_ = nullptr;

  // Work guard prevents io_context from exiting when no timers
  std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>> work_guard_;
  // Background thread for internal io_context
  std::thread io_thread_;
  // Service state flag
  std::atomic<bool> running_{false};
  // Tracks whether io_thread has exited run() (for join timeout)
  std::atomic<bool> io_thread_exited_{false};

  // Strand serializes all timer operations (async_wait, cancel, post)
  // to prevent data race when using external multi-threaded io_context.
  asio::io_context::strand strand_;
};

}  // namespace rollingraft
