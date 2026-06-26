#include "simulated_timer_service.h"

#include "simulated_clock.h"

namespace rollingraft {

SimulatedTimerService::SimulatedTimerService(SimulatedClock* clock)
    : state_(std::make_shared<State>()) {
  state_->clock = clock;
}

void SimulatedTimerService::Stop() {
  if (!state_) return;
  std::lock_guard<std::mutex> lock(state_->timers_mtx);
  state_->timers.clear();
  // Release shared state so pending clock callbacks with weak_ptr no-op.
  state_.reset();
}

TimerId SimulatedTimerService::SetTimeout(std::chrono::milliseconds delay,
                                          std::function<void()> callback) {
  if (!state_) return 0;
  TimerId id = state_->next_id.fetch_add(1);
  uint64_t delay_ms = static_cast<uint64_t>(delay.count());
  {
    std::lock_guard<std::mutex> lock(state_->timers_mtx);
    state_->timers[id] = std::move(callback);
  }
  auto weak = std::weak_ptr<State>(state_);
  state_->clock->After(delay_ms, [weak, id]() {
    auto state = weak.lock();
    if (!state) return;
    std::function<void()> cb;
    {
      std::lock_guard<std::mutex> lock(state->timers_mtx);
      auto it = state->timers.find(id);
      if (it != state->timers.end()) {
        cb = std::move(it->second);
        state->timers.erase(it);
      }
    }
    if (cb) cb();
  });
  return id;
}

TimerId SimulatedTimerService::SetInterval(std::chrono::milliseconds interval,
                                           std::function<void()> callback) {
  if (!state_) return 0;
  TimerId id = state_->next_id.fetch_add(1);
  uint64_t interval_ms = static_cast<uint64_t>(interval.count());
  {
    std::lock_guard<std::mutex> lock(state_->timers_mtx);
    state_->timers[id] = callback;
  }
  auto weak = std::weak_ptr<State>(state_);
  auto recurring = std::make_shared<std::function<void()>>();
  *recurring = [weak, id, interval_ms, callback, recurring]() {
    callback();
    auto state = weak.lock();
    if (!state) return;
    {
      std::lock_guard<std::mutex> lock(state->timers_mtx);
      if (state->timers.find(id) != state->timers.end()) {
        state->clock->After(interval_ms, *recurring);
      }
    }
  };
  state_->clock->After(interval_ms, *recurring);
  return id;
}

bool SimulatedTimerService::CancelTimer(TimerId id) {
  if (!state_) return false;
  std::lock_guard<std::mutex> lock(state_->timers_mtx);
  return state_->timers.erase(id) > 0;
}

bool SimulatedTimerService::IsTimerActive(TimerId id) const {
  if (!state_) return false;
  std::lock_guard<std::mutex> lock(state_->timers_mtx);
  return state_->timers.find(id) != state_->timers.end();
}

}  // namespace rollingraft
