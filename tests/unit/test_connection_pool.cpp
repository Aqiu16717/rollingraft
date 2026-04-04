/**
 * @file test_connection_pool.cpp
 * @brief Unit tests for ConnectionPool class
 */

#include <gtest/gtest.h>

#include <asio.hpp>
#include <chrono>
#include <thread>

#include "client/connection_pool.h"

using namespace rollingraft;

/**
 * ConnectionPool tests.
 *
 * These tests verify connection management.
 * Note: Tests requiring real network I/O are marked as integration tests.
 */

class ConnectionPoolTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = std::make_unique<ConnectionPool>(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    pool_.reset();
  }

  std::unique_ptr<ConnectionPool> pool_;
};

// ========== Basic Connection Tests ==========

TEST_F(ConnectionPoolTest, GetConnection_InvalidAddress_ReturnsNull) {
  auto conn = pool_->GetConnection("invalid_no_port");
  EXPECT_EQ(conn, nullptr);
}

TEST_F(ConnectionPoolTest, GetConnection_InvalidPort_ReturnsNull) {
  auto conn = pool_->GetConnection("127.0.0.1:abc");
  EXPECT_EQ(conn, nullptr);
}

TEST_F(ConnectionPoolTest, GetConnection_NoServer_ReturnsNull) {
  // No server listening on this port - should timeout quickly
  auto conn = pool_->GetConnection("127.0.0.1:59999");
  EXPECT_EQ(conn, nullptr);
}

// ========== IsHealthy Tests ==========

TEST_F(ConnectionPoolTest, IsHealthy_NoConnection_ReturnsFalse) {
  EXPECT_FALSE(pool_->IsHealthy("127.0.0.1:59998"));
}

TEST_F(ConnectionPoolTest, IsHealthy_AfterFailedConnect_ReturnsFalse) {
  // Try to connect to non-existent server
  auto conn = pool_->GetConnection("127.0.0.1:59997");
  EXPECT_EQ(conn, nullptr);
  
  // Should still be unhealthy
  EXPECT_FALSE(pool_->IsHealthy("127.0.0.1:59997"));
}

// ========== CloseConnection Tests ==========

TEST_F(ConnectionPoolTest, CloseConnection_NonExistent_NoCrash) {
  pool_->CloseConnection("127.0.0.1:59996");  // Should not crash
}

// ========== CloseAll Tests ==========

TEST_F(ConnectionPoolTest, CloseAll_NoConnections_NoCrash) {
  // Should not crash even with no connections
  pool_->CloseAll();
  SUCCEED();
}

// ========== Timeout Tests ==========

TEST_F(ConnectionPoolTest, GetConnection_ShortTimeout_ReturnsNull) {
  // Use very short timeout
  auto short_timeout_pool = std::make_unique<ConnectionPool>(
      std::chrono::milliseconds(1));
  
  // Try to connect to an address that won't respond
  auto conn = short_timeout_pool->GetConnection("127.0.0.1:59995");
  
  // Should timeout and return null
  EXPECT_EQ(conn, nullptr);
}

// ========== Concurrent Access Tests ==========

TEST_F(ConnectionPoolTest, Concurrent_IsHealthyChecks) {
  // Multiple threads checking IsHealthy on different addresses
  std::vector<std::thread> threads;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([this, i]() {
      std::string addr = "127.0.0.1:" + std::to_string(60000 + i);
      for (int j = 0; j < 100; ++j) {
        pool_->IsHealthy(addr);
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  SUCCEED();
}

TEST_F(ConnectionPoolTest, Concurrent_CloseAll) {
  // Multiple threads calling operations while CloseAll is called
  std::atomic<bool> running{true};
  
  std::vector<std::thread> threads;
  for (int i = 0; i < 5; ++i) {
    threads.emplace_back([this, &running, i]() {
      std::string addr = "127.0.0.1:" + std::to_string(61000 + i);
      while (running) {
        pool_->IsHealthy(addr);
      }
    });
  }
  
  // Let threads run briefly
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  
  // Close all while threads are checking
  pool_->CloseAll();
  
  running = false;
  for (auto& t : threads) {
    t.join();
  }
  
  SUCCEED();
}
