#include "simulated_timer_service.h"
#include "simulated_clock.h"
namespace rollingraft {
SimulatedTimerService::SimulatedTimerService(SimulatedClock* clock) : clock_(clock) {}
TimerId SimulatedTimerService::StartTimer(uint64_t delay_ms, std::function<void()> callback) {
  TimerId id = next_id_.fetch_add(1);
  { std::lock_guard<std::mutex> lock(timers_mtx_); timers_[id] = callback; }
  clock_->After(delay_ms, [this, id]() {
    std::function<void()> cb;
    { std::lock_guard<std::mutex> lock(timers_mtx_);
      auto it = timers_.find(id);
      if (it != timers_.end()) { cb = std::move(it->second); timers_.erase(it); } }
    if (cb) cb();
  });
  return id;
}
void SimulatedTimerService::CancelTimer(TimerId id) {
  std::lock_guard<std::mutex> lock(timers_mtx_); timers_.erase(id);
}
}  // namespace rollingraft
