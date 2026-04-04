#pragma once

#include "rollingraft/timer_service.h"
#include <asio.hpp>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <atomic>

namespace rollingraft {

class AsioTimerService : public TimerService {
 public:
  AsioTimerService();
  explicit AsioTimerService(asio::io_context& external_io);
  ~AsioTimerService() override;

  void Start() override;
  void Stop() override;

  TimerId SetTimeout(std::chrono::milliseconds delay,
                     std::function<void()> callback) override;
  TimerId SetInterval(std::chrono::milliseconds interval,
                      std::function<void()> callback) override;
  bool CancelTimer(TimerId timer_id) override;
  bool IsTimerActive(TimerId timer_id) const override;

 private:
  struct Timer {
    std::unique_ptr<asio::steady_timer> asio_timer;
    std::function<void()> callback;
    bool is_interval;
    std::chrono::milliseconds interval;
  };

  TimerId next_timer_id_ = 1;
  std::unordered_map<TimerId, std::shared_ptr<Timer>> timers_;
  mutable std::mutex timers_mutex_;

  bool owns_io_context_ = true;
  asio::io_context io_context_;
  asio::io_context* io_ptr_ = nullptr;
  std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>> work_guard_;
  std::thread io_thread_;
  std::atomic<bool> running_{false};
};

}  // namespace rollingraft
