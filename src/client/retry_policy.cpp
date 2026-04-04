/**
 * @file retry_policy.cpp
 * @brief RetryPolicy implementation
 */

#include "retry_policy.h"

#include <algorithm>
#include <cmath>
#include <random>

namespace rollingraft {

RetryPolicy::RetryPolicy(int max_retries,
                         std::chrono::milliseconds initial_delay,
                         std::chrono::milliseconds max_delay,
                         double backoff_multiplier)
    : max_retries_(max_retries),
      initial_delay_(initial_delay),
      max_delay_(max_delay),
      backoff_multiplier_(backoff_multiplier) {}

bool RetryPolicy::IsRetryableError(const Status& error) {
  // NotLeader is retryable (we'll redirect to the correct leader)
  if (error.IsNotLeader()) {
    return true;
  }

  // Use error code classification instead of string matching
  auto code = error.GetErrorCode();
  if (code == Status::Code::kRequestVoteError ||
      code == Status::Code::kAppendEntriesError ||
      code == Status::Code::kInstallSnapshotError) {
    return true;
  }

  // Generic errors may be retryable - check message content as fallback
  std::string msg = error.GetMessage();
  if (msg.find("network") != std::string::npos ||
      msg.find("Network") != std::string::npos ||
      msg.find("timeout") != std::string::npos ||
      msg.find("Timeout") != std::string::npos ||
      msg.find("connect") != std::string::npos ||
      msg.find("Connect") != std::string::npos) {
    return true;
  }

  return false;
}

bool RetryPolicy::ShouldRetry(int attempt_count) const {
  return attempt_count < max_retries_;
}

std::chrono::milliseconds RetryPolicy::GetDelay(int attempt_count) const {
  // Exponential backoff: delay = initial * multiplier^attempt
  double factor = std::pow(backoff_multiplier_, attempt_count);
  auto base_delay = std::chrono::milliseconds(static_cast<int>(
      initial_delay_.count() * factor));

  // Cap at max delay (before jitter so final delay stays within bounds)
  base_delay = std::min(base_delay, max_delay_);

  // Add jitter: random value between 0 and min(base_delay/2, max_delay/4)
  // This prevents thundering herd while ensuring we don't exceed max_delay
  thread_local std::mt19937 gen(std::random_device{}());
  int jitter_max = std::min(base_delay.count() / 2, max_delay_.count() / 4);
  if (jitter_max < 1) jitter_max = 1;
  std::uniform_int_distribution<> dis(0, jitter_max);
  auto jitter = std::chrono::milliseconds(dis(gen));

  auto final_delay = base_delay + jitter;
  
  // Final safety cap
  return std::min(final_delay, max_delay_);
}

}  // namespace rollingraft
