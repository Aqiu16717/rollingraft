/**
 * @file test_leader_tracker.cpp
 * @brief Unit tests for LeaderTracker class
 */

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "client/leader_tracker.h"

using namespace rollingraft;

/**
 * LeaderTracker tests.
 *
 * These tests verify leader address caching with TTL.
 */

class LeaderTrackerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Default TTL of 1 second for tests
    tracker_ = std::make_unique<LeaderTracker>(std::chrono::milliseconds(1000));
  }

  std::unique_ptr<LeaderTracker> tracker_;
};

// ========== Basic Get/Update Tests ==========

TEST_F(LeaderTrackerTest, GetLeader_InitiallyEmpty) {
  auto leader = tracker_->GetLeader();
  EXPECT_FALSE(leader.has_value());
}

TEST_F(LeaderTrackerTest, UpdateLeader_ThenGetReturnsValue) {
  tracker_->UpdateLeader("127.0.0.1:8001");
  
  auto leader = tracker_->GetLeader();
  EXPECT_TRUE(leader.has_value());
  EXPECT_EQ(*leader, "127.0.0.1:8001");
}

TEST_F(LeaderTrackerTest, UpdateLeader_OverwritesPrevious) {
  tracker_->UpdateLeader("127.0.0.1:8001");
  tracker_->UpdateLeader("127.0.0.1:8002");
  
  auto leader = tracker_->GetLeader();
  EXPECT_EQ(*leader, "127.0.0.1:8002");
}

// ========== Clear Tests ==========

TEST_F(LeaderTrackerTest, ClearLeader_RemovesValue) {
  tracker_->UpdateLeader("127.0.0.1:8001");
  tracker_->ClearLeader();
  
  auto leader = tracker_->GetLeader();
  EXPECT_FALSE(leader.has_value());
}

TEST_F(LeaderTrackerTest, ClearLeader_OnEmpty_NoCrash) {
  tracker_->ClearLeader();  // Should not crash
  EXPECT_FALSE(tracker_->GetLeader().has_value());
}

// ========== TTL Tests ==========

TEST_F(LeaderTrackerTest, GetLeader_AfterTTLExpired_ReturnsNullopt) {
  // Use short TTL
  tracker_ = std::make_unique<LeaderTracker>(std::chrono::milliseconds(50));
  
  tracker_->UpdateLeader("127.0.0.1:8001");
  
  // Wait for TTL to expire
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  
  auto leader = tracker_->GetLeader();
  EXPECT_FALSE(leader.has_value());
}

TEST_F(LeaderTrackerTest, GetLeader_BeforeTTLExpired_ReturnsValue) {
  tracker_ = std::make_unique<LeaderTracker>(std::chrono::milliseconds(500));
  
  tracker_->UpdateLeader("127.0.0.1:8001");
  
  // Wait a bit but not past TTL
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  auto leader = tracker_->GetLeader();
  EXPECT_TRUE(leader.has_value());
  EXPECT_EQ(*leader, "127.0.0.1:8001");
}

TEST_F(LeaderTrackerTest, UpdateLeader_ResetsTTL) {
  tracker_ = std::make_unique<LeaderTracker>(std::chrono::milliseconds(100));
  
  tracker_->UpdateLeader("127.0.0.1:8001");
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  
  // Update again - resets TTL
  tracker_->UpdateLeader("127.0.0.1:8001");
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  
  // Should still be valid (total 120ms but last update was 60ms ago)
  auto leader = tracker_->GetLeader();
  EXPECT_TRUE(leader.has_value());
}

// ========== IsLeaderStale Tests ==========

TEST_F(LeaderTrackerTest, IsLeaderStale_InitiallyTrue) {
  EXPECT_TRUE(tracker_->IsLeaderStale());
}

TEST_F(LeaderTrackerTest, IsLeaderStale_AfterUpdate_False) {
  tracker_->UpdateLeader("127.0.0.1:8001");
  EXPECT_FALSE(tracker_->IsLeaderStale());
}

TEST_F(LeaderTrackerTest, IsLeaderStale_AfterTTL_True) {
  tracker_ = std::make_unique<LeaderTracker>(std::chrono::milliseconds(50));
  
  tracker_->UpdateLeader("127.0.0.1:8001");
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  
  EXPECT_TRUE(tracker_->IsLeaderStale());
}

TEST_F(LeaderTrackerTest, IsLeaderStale_AfterClear_True) {
  tracker_->UpdateLeader("127.0.0.1:8001");
  tracker_->ClearLeader();
  
  EXPECT_TRUE(tracker_->IsLeaderStale());
}

// ========== Concurrency Tests ==========

TEST_F(LeaderTrackerTest, Concurrent_ReadsAndWrites) {
  const int num_threads = 4;
  const int ops_per_thread = 100;
  
  std::vector<std::thread> threads;
  
  // Some threads update
  for (int i = 0; i < num_threads / 2; ++i) {
    threads.emplace_back([this, i, ops_per_thread]() {
      for (int j = 0; j < ops_per_thread; ++j) {
        tracker_->UpdateLeader("127.0.0.1:" + std::to_string(8000 + i));
      }
    });
  }
  
  // Some threads read
  for (int i = num_threads / 2; i < num_threads; ++i) {
    threads.emplace_back([this, ops_per_thread]() {
      for (int j = 0; j < ops_per_thread; ++j) {
        tracker_->GetLeader();
        tracker_->IsLeaderStale();
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  // Should not crash and have valid state
  auto leader = tracker_->GetLeader();
  // Leader may or may not have value depending on timing
  (void)leader;
}

// ========== Edge Cases ==========

TEST_F(LeaderTrackerTest, UpdateLeader_EmptyString_ClearsLeader) {
  // First set a valid leader
  tracker_->UpdateLeader("127.0.0.1:8001");
  EXPECT_TRUE(tracker_->GetLeader().has_value());
  
  // Empty string should clear the leader
  tracker_->UpdateLeader("");
  
  EXPECT_FALSE(tracker_->GetLeader().has_value());
}

TEST_F(LeaderTrackerTest, UpdateLeader_LongAddress_Handled) {
  std::string long_addr(1000, 'a');
  tracker_->UpdateLeader(long_addr);
  
  auto leader = tracker_->GetLeader();
  EXPECT_EQ(*leader, long_addr);
}

TEST_F(LeaderTrackerTest, ZeroTTL_AlwaysStale) {
  tracker_ = std::make_unique<LeaderTracker>(std::chrono::milliseconds(0));
  
  tracker_->UpdateLeader("127.0.0.1:8001");
  
  // Immediately stale with 0 TTL
  EXPECT_TRUE(tracker_->IsLeaderStale());
  EXPECT_FALSE(tracker_->GetLeader().has_value());
}
