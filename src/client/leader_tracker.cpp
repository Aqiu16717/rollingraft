/**
 * @file leader_tracker.cpp
 * @brief LeaderTracker implementation
 */

#include "leader_tracker.h"

namespace rollingraft {

LeaderTracker::LeaderTracker(std::chrono::milliseconds ttl) : ttl_(ttl) {}

std::optional<std::string> LeaderTracker::GetLeader() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (leader_addr_.empty()) {
    return std::nullopt;
  }
  auto now = std::chrono::steady_clock::now();
  if (now - last_update_ > ttl_) {
    return std::nullopt;
  }
  return leader_addr_;
}

void LeaderTracker::UpdateLeader(const std::string& addr) {
  std::lock_guard<std::mutex> lock(mutex_);
  leader_addr_ = addr;
  last_update_ = std::chrono::steady_clock::now();
}

void LeaderTracker::ClearLeader() {
  std::lock_guard<std::mutex> lock(mutex_);
  leader_addr_.clear();
}

bool LeaderTracker::IsLeaderStale() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (leader_addr_.empty()) {
    return true;
  }
  auto now = std::chrono::steady_clock::now();
  return now - last_update_ > ttl_;
}

}  // namespace rollingraft
