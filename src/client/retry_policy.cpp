/**
 * @file retry_policy.cpp
 * @brief RetryPolicy implementation
 */

#include "retry_policy.h"

#include <algorithm>
#include <cmath>

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
  // Network errors are retryable
  if (error.ToString().find("Network") != std::string::npos) {
    return true;
  }
  // Timeout errors are retryable
  if (error.ToString().find("timeout") != std::string::npos ||
      error.ToString().find("Timeout") != std::string::npos) {
    return true;
  }
  // NotLeader is retryable (we'll redirect)
  if (error.ToString().find("Not leader") != std::string::npos ||
      error.ToString().find("not leader") != std::string::npos) {
    return true;
  }
  // Connection errors are retryable
  if (error.ToString().find("connect") != std::string::npos ||
      error.ToString().find("Connect") != std::string::npos) {
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
  auto delay = std::chrono::milliseconds(static_cast<int>(
      initial_delay_.count() * factor));

  // Cap at max delay
  return std::min(delay, max_delay_);
}

}  // namespace rollingraft
