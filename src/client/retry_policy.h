/**
 * @file retry_policy.h
 * @brief Retry logic with exponential backoff
 */

#pragma once

#include <chrono>

#include "rollingraft/status.h"

namespace rollingraft {

/**
 * Determines retry behavior for failed requests.
 */
class RetryPolicy {
 public:
  RetryPolicy(int max_retries,
              std::chrono::milliseconds initial_delay,
              std::chrono::milliseconds max_delay,
              double backoff_multiplier);

  /** Check if error is retryable. */
  static bool IsRetryableError(const Status& error);

  /** Check if should retry based on attempt count. */
  bool ShouldRetry(int attempt_count) const;

  /** Get delay for next retry attempt. */
  std::chrono::milliseconds GetDelay(int attempt_count) const;

 private:
  int max_retries_;
  std::chrono::milliseconds initial_delay_;
  std::chrono::milliseconds max_delay_;
  double backoff_multiplier_;
};

}  // namespace rollingraft
