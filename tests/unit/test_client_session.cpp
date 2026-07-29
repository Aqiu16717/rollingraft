/**
 * @file test_client_session.cpp
 * @brief Unit tests for ClientSessionManager and session-based Propose
 */

#include <atomic>
#include <thread>
#include <vector>

#include "rollingraft/client_session_manager.h"

#include <gtest/gtest.h>

using namespace rollingraft;

class ClientSessionManagerTest : public ::testing::Test {};

TEST_F(ClientSessionManagerTest, DefaultValues) {
  ClientSessionManager mgr;
  EXPECT_EQ(mgr.Size(), 0);
}

TEST_F(ClientSessionManagerTest, RecordAndRetrieve) {
  ClientSessionManager mgr;
  SessionResult result;
  result.success = true;
  result.response = "hello";
  result.applied_index = 42;

  mgr.RecordResult(1, 10, result);
  EXPECT_EQ(mgr.Size(), 1);

  SessionResult cached;
  EXPECT_TRUE(mgr.IsDuplicate(1, 10, cached));
  EXPECT_TRUE(cached.success);
  EXPECT_EQ(cached.response, "hello");
  EXPECT_EQ(cached.applied_index, 42);
}

TEST_F(ClientSessionManagerTest, NewRequestNotDuplicate) {
  ClientSessionManager mgr;
  SessionResult cached;
  EXPECT_FALSE(mgr.IsDuplicate(1, 10, cached));
}

TEST_F(ClientSessionManagerTest, OldSeqReturnsCached) {
  ClientSessionManager mgr;
  SessionResult result;
  result.success = true;
  result.response = "v10";
  mgr.RecordResult(1, 10, result);

  SessionResult cached;
  EXPECT_TRUE(mgr.IsDuplicate(1, 5, cached));
  EXPECT_EQ(cached.response, "v10");
}

TEST_F(ClientSessionManagerTest, HigherSeqNotDuplicate) {
  ClientSessionManager mgr;
  SessionResult result;
  result.success = true;
  result.response = "v10";
  mgr.RecordResult(1, 10, result);

  SessionResult cached;
  EXPECT_FALSE(mgr.IsDuplicate(1, 15, cached));
}

TEST_F(ClientSessionManagerTest, DifferentSessionsIndependent) {
  ClientSessionManager mgr;
  SessionResult r1, r2;
  r1.success = true;
  r1.response = "A";
  r2.success = true;
  r2.response = "B";
  mgr.RecordResult(1, 1, r1);
  mgr.RecordResult(2, 1, r2);

  SessionResult cached;
  EXPECT_TRUE(mgr.IsDuplicate(1, 1, cached));
  EXPECT_EQ(cached.response, "A");

  EXPECT_TRUE(mgr.IsDuplicate(2, 1, cached));
  EXPECT_EQ(cached.response, "B");
}

TEST_F(ClientSessionManagerTest, LRUEviction) {
  ClientSessionManager mgr(3, 0);  // max 3 sessions
  SessionResult r;
  r.success = true;
  mgr.RecordResult(1, 1, r);
  mgr.RecordResult(2, 1, r);
  mgr.RecordResult(3, 1, r);
  EXPECT_EQ(mgr.Size(), 3);

  // This should evict session 1 (LRU)
  mgr.RecordResult(4, 1, r);
  EXPECT_EQ(mgr.Size(), 3);

  SessionResult cached;
  EXPECT_FALSE(mgr.IsDuplicate(1, 1, cached));
  EXPECT_TRUE(mgr.IsDuplicate(2, 1, cached));
  EXPECT_TRUE(mgr.IsDuplicate(3, 1, cached));
  EXPECT_TRUE(mgr.IsDuplicate(4, 1, cached));
}

TEST_F(ClientSessionManagerTest, TouchUpdatesLRU) {
  ClientSessionManager mgr(2, 0);  // max 2 sessions
  SessionResult r;
  r.success = true;
  mgr.RecordResult(1, 1, r);
  mgr.RecordResult(2, 1, r);

  // Touch session 1 (move to front)
  SessionResult cached;
  mgr.IsDuplicate(1, 1, cached);

  // Record session 3, should evict session 2 (now LRU)
  mgr.RecordResult(3, 1, r);
  EXPECT_FALSE(mgr.IsDuplicate(2, 1, cached));
  EXPECT_TRUE(mgr.IsDuplicate(1, 1, cached));
  EXPECT_TRUE(mgr.IsDuplicate(3, 1, cached));
}

TEST_F(ClientSessionManagerTest, EvictExpired) {
  ClientSessionManager mgr(100, 50);  // 50ms TTL
  SessionResult r;
  r.success = true;
  mgr.RecordResult(1, 1, r);
  EXPECT_EQ(mgr.Size(), 1);

  // Immediately: not expired
  EXPECT_EQ(mgr.EvictExpired(), 0);
  EXPECT_EQ(mgr.Size(), 1);

  // Wait for TTL to pass
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(mgr.EvictExpired(), 1);
  EXPECT_EQ(mgr.Size(), 0);
}

TEST_F(ClientSessionManagerTest, Clear) {
  ClientSessionManager mgr;
  SessionResult r;
  r.success = true;
  mgr.RecordResult(1, 1, r);
  mgr.RecordResult(2, 1, r);
  EXPECT_EQ(mgr.Size(), 2);

  mgr.Clear();
  EXPECT_EQ(mgr.Size(), 0);
}

TEST_F(ClientSessionManagerTest, ConcurrentReadsAndWrites) {
  ClientSessionManager mgr;
  std::atomic<int> failures{0};

  std::thread writer([&]() {
    SessionResult r;
    r.success = true;
    for (int i = 0; i < 100; ++i) {
      mgr.RecordResult(i % 10, i, r);
    }
  });

  std::vector<std::thread> readers;
  for (int t = 0; t < 4; ++t) {
    readers.emplace_back([&]() {
      SessionResult cached;
      for (int i = 0; i < 100; ++i) {
        mgr.IsDuplicate(i % 10, i, cached);
      }
    });
  }

  writer.join();
  for (auto& t : readers) {
    t.join();
  }
  EXPECT_EQ(failures, 0);
}
