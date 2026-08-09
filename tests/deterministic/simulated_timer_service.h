#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "rollingraft/timer_service.h"

namespace rollingraft {
class SimulatedClock;

class SimulatedTimerService : public TimerService {
 public:
  explicit SimulatedTimerService(SimulatedClock* clock);

  void Start() override {}
  void Stop() override;

  // Report the simulated clock as a steady_clock::time_point so Raft timeout
  // deadlines advance deterministically with the test clock.
  std::chrono::steady_clock::time_point Now() const override;

  TimerId SetTimeout(std::chrono::milliseconds delay, std::function<void()> callback) override;
  TimerId SetInterval(std::chrono::milliseconds interval, std::function<void()> callback) override;
  bool CancelTimer(TimerId id) override;
  bool IsTimerActive(TimerId id) const override;

 private:
  struct State {
    SimulatedClock* clock = nullptr;
    std::atomic<TimerId> next_id{1};
    std::unordered_map<TimerId, std::function<void()>> timers;
    mutable std::mutex timers_mtx;
  };
  std::shared_ptr<State> state_;
};

}  // namespace rollingraft
