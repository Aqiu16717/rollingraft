#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <vector>
namespace rollingraft {
class SimulatedClock {
 public:
  using TimePoint = uint64_t;
  TimePoint Now() const { return current_time_ms_.load(); }
  void Advance(uint64_t delta_ms);
  void RunUntilIdle();
  void RunUntil(TimePoint target);
  void At(TimePoint when, std::function<void()> callback);
  void After(uint64_t delay_ms, std::function<void()> callback);
  void CancelAll();
 private:
  std::atomic<uint64_t> current_time_ms_{0};
  std::mutex mtx_;
  std::multimap<uint64_t, std::function<void()>> scheduled_callbacks_;
};
}  // namespace rollingraft
