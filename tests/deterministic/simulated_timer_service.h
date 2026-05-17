#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include "rollingraft/timer_service.h"
namespace rollingraft {
class SimulatedClock;
class SimulatedTimerService : public TimerService {
 public:
  explicit SimulatedTimerService(SimulatedClock* clock);
  TimerId StartTimer(uint64_t delay_ms, std::function<void()> callback) override;
  void CancelTimer(TimerId id) override;
 private:
  SimulatedClock* clock_;
  std::atomic<TimerId> next_id_{1};
  std::unordered_map<TimerId, std::function<void()>> timers_;
  mutable std::mutex timers_mtx_;
};
}  // namespace rollingraft
