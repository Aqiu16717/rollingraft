#include "simulated_clock.h"
namespace rollingraft {
void SimulatedClock::Advance(uint64_t delta_ms) { RunUntil(current_time_ms_.load() + delta_ms); }
void SimulatedClock::RunUntilIdle() { RunUntil(current_time_ms_.load()); }
void SimulatedClock::RunUntil(TimePoint target) {
  current_time_ms_.store(target);
  std::vector<std::function<void()>> callbacks;
  { std::lock_guard<std::mutex> lock(mtx_);
    auto it = scheduled_callbacks_.begin();
    while (it != scheduled_callbacks_.end() && it->first <= target) {
      callbacks.push_back(std::move(it->second)); it = scheduled_callbacks_.erase(it);
    } }
  for (auto& cb : callbacks) if (cb) cb();
}
void SimulatedClock::At(TimePoint when, std::function<void()> callback) {
  std::lock_guard<std::mutex> lock(mtx_); scheduled_callbacks_.emplace(when, std::move(callback));
}
void SimulatedClock::After(uint64_t delay_ms, std::function<void()> callback) {
  At(current_time_ms_.load() + delay_ms, std::move(callback));
}
void SimulatedClock::CancelAll() {
  std::lock_guard<std::mutex> lock(mtx_); scheduled_callbacks_.clear();
}
}  // namespace rollingraft
