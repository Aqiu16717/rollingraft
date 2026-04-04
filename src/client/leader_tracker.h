/**
 * @file leader_tracker.h
 * @brief Leader address caching with TTL
 */

#pragma once

#include <chrono>
#include <mutex>
#include <optional>
#include <string>

namespace rollingraft {

/**
 * Tracks the current leader with TTL-based expiration.
 *
 * Thread-safe.
 */
class LeaderTracker {
 public:
  explicit LeaderTracker(std::chrono::milliseconds ttl);

  /** Get cached leader if not expired. */
  std::optional<std::string> GetLeader() const;

  /** Update leader address. */
  void UpdateLeader(const std::string& addr);

  /** Clear cached leader. */
  void ClearLeader();

  /** Check if leader info is stale. */
  bool IsLeaderStale() const;

 private:
  mutable std::mutex mutex_;
  std::string leader_addr_;
  std::chrono::steady_clock::time_point last_update_;
  std::chrono::milliseconds ttl_;
};

}  // namespace rollingraft
