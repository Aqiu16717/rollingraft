/**
 * @file test_retry_policy.cpp
 * @brief Unit tests for RetryPolicy class
 */

#include <chrono>

#include "rollingraft/status.h"

#include "client/retry_policy.h"
#include <gtest/gtest.h>

using namespace rollingraft;

/**
 * RetryPolicy tests.
 *
 * These tests verify exponential backoff and error classification.
 */

class RetryPolicyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Default: 3 retries, 100ms initial, 1000ms max, 2x multiplier
    policy_ = std::make_unique<RetryPolicy>(3, std::chrono::milliseconds(100),
                                            std::chrono::milliseconds(1000), 2.0);
  }

  std::unique_ptr<RetryPolicy> policy_;
};

// ========== ShouldRetry Tests ==========

TEST_F(RetryPolicyTest, ShouldRetry_ZeroAttempts_ReturnsTrue) {
  EXPECT_TRUE(policy_->ShouldRetry(0));
}

TEST_F(RetryPolicyTest, ShouldRetry_AtMaxRetriesMinusOne_ReturnsTrue) {
  EXPECT_TRUE(policy_->ShouldRetry(2));  // max_retries=3, so 0,1,2 are valid
}

TEST_F(RetryPolicyTest, ShouldRetry_AtMaxRetries_ReturnsFalse) {
  EXPECT_FALSE(policy_->ShouldRetry(3));  // 3 >= 3
}

TEST_F(RetryPolicyTest, ShouldRetry_BeyondMaxRetries_ReturnsFalse) {
  EXPECT_FALSE(policy_->ShouldRetry(5));
}

// ========== GetDelay - Exponential Backoff Tests ==========

TEST_F(RetryPolicyTest, GetDelay_Attempt0_ReturnsInitialDelay) {
  auto delay = policy_->GetDelay(0);
  // With jitter: should be between 100ms and 150ms
  EXPECT_GE(delay.count(), 100);
  EXPECT_LE(delay.count(), 150);
}

TEST_F(RetryPolicyTest, GetDelay_Attempt1_Doubles) {
  auto delay = policy_->GetDelay(1);
  // Base: 100ms * 2^1 = 200ms, with jitter: 200-300ms
  EXPECT_GE(delay.count(), 200);
  EXPECT_LE(delay.count(), 300);
}

TEST_F(RetryPolicyTest, GetDelay_Attempt2_Quadruples) {
  auto delay = policy_->GetDelay(2);
  // Base: 100ms * 2^2 = 400ms, with jitter: 400-600ms
  EXPECT_GE(delay.count(), 400);
  EXPECT_LE(delay.count(), 600);
}

TEST_F(RetryPolicyTest, GetDelay_Attempt3_RespectsMaxDelay) {
  auto delay = policy_->GetDelay(3);
  // Would be 800ms but max is 1000ms, with jitter: up to 1500ms but capped
  EXPECT_LE(delay.count(), 1000);
}

TEST_F(RetryPolicyTest, GetDelay_LargeAttempt_RespectsMaxDelay) {
  auto delay = policy_->GetDelay(10);
  EXPECT_LE(delay.count(), 1000);
}

// ========== Jitter Tests ==========

TEST_F(RetryPolicyTest, GetDelay_Jitter_AddsVariation) {
  // Collect multiple delays to verify jitter adds randomness
  std::vector<int> delays;
  for (int i = 0; i < 20; ++i) {
    delays.push_back(policy_->GetDelay(0).count());
  }

  // Not all delays should be identical (jitter should vary)
  bool has_variation = false;
  for (size_t i = 1; i < delays.size(); ++i) {
    if (delays[i] != delays[0]) {
      has_variation = true;
      break;
    }
  }
  EXPECT_TRUE(has_variation);
}

TEST_F(RetryPolicyTest, GetDelay_Jitter_InValidRange) {
  for (int attempt = 0; attempt < 5; ++attempt) {
    for (int i = 0; i < 10; ++i) {
      auto delay = policy_->GetDelay(attempt);
      int base = 100 * (1 << attempt);  // 100 * 2^attempt
      if (base > 1000) base = 1000;

      // Delay should be between base and base + base/2 (with jitter)
      EXPECT_GE(delay.count(), base);
      EXPECT_LE(delay.count(), base + base / 2);
    }
  }
}

// ========== IsRetryableError Tests ==========

TEST_F(RetryPolicyTest, IsRetryableError_NotLeader_ReturnsTrue) {
  Status error = Status::NotLeader(1, "127.0.0.1:8001");
  EXPECT_TRUE(RetryPolicy::IsRetryableError(error));
}

TEST_F(RetryPolicyTest, IsRetryableError_NetworkError_ReturnsTrue) {
  Status error = Status::Error("Network connection failed");
  EXPECT_TRUE(RetryPolicy::IsRetryableError(error));
}

TEST_F(RetryPolicyTest, IsRetryableError_Timeout_ReturnsTrue) {
  Status error = Status::Error("Request timeout");
  EXPECT_TRUE(RetryPolicy::IsRetryableError(error));
}

TEST_F(RetryPolicyTest, IsRetryableError_ConnectionError_ReturnsTrue) {
  Status error = Status::Error("Failed to connect to server");
  EXPECT_TRUE(RetryPolicy::IsRetryableError(error));
}

TEST_F(RetryPolicyTest, IsRetryableError_RPCError_ReturnsTrue) {
  Status error1 = Status::RequestVoteError("timeout");
  EXPECT_TRUE(RetryPolicy::IsRetryableError(error1));

  Status error2 = Status::AppendEntriesError("disconnected");
  EXPECT_TRUE(RetryPolicy::IsRetryableError(error2));

  Status error3 = Status::InstallSnapshotError("timeout");
  EXPECT_TRUE(RetryPolicy::IsRetryableError(error3));
}

TEST_F(RetryPolicyTest, IsRetryableError_GenericError_ReturnsFalse) {
  Status error = Status::Error("Invalid argument");
  EXPECT_FALSE(RetryPolicy::IsRetryableError(error));
}

TEST_F(RetryPolicyTest, IsRetryableError_OK_ReturnsFalse) {
  Status ok = Status::OK();
  EXPECT_FALSE(RetryPolicy::IsRetryableError(ok));
}

// ========== Different Configurations ==========

TEST(RetryPolicyConfigTest, ZeroMaxRetries_NeverRetries) {
  RetryPolicy policy(0, std::chrono::milliseconds(100), std::chrono::milliseconds(1000), 2.0);

  EXPECT_FALSE(policy.ShouldRetry(0));
  EXPECT_FALSE(policy.ShouldRetry(1));
}

TEST(RetryPolicyConfigTest, LinearBackoff_1xMultiplier) {
  RetryPolicy policy(3, std::chrono::milliseconds(100), std::chrono::milliseconds(1000), 1.0);

  // With 1x multiplier, delay should always be 100ms + jitter
  for (int attempt = 0; attempt < 5; ++attempt) {
    auto delay = policy.GetDelay(attempt);
    EXPECT_GE(delay.count(), 100);
    EXPECT_LE(delay.count(), 150);
  }
}

TEST(RetryPolicyConfigTest, AggressiveBackoff_3xMultiplier) {
  RetryPolicy policy(5, std::chrono::milliseconds(10), std::chrono::milliseconds(10000), 3.0);

  // 10ms, 30ms, 90ms, 270ms, 810ms...
  auto d0 = policy.GetDelay(0);
  auto d1 = policy.GetDelay(1);
  auto d2 = policy.GetDelay(2);

  EXPECT_GE(d1.count(), d0.count() * 2);  // At least 2x increase
  EXPECT_GE(d2.count(), d1.count() * 2);
}
