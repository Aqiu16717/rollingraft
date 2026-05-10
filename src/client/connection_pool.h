/**
 * @file connection_pool.h
 * @brief Simple connection management for client
 */

#pragma once

#include <asio.hpp>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace rollingraft {

/**
 * Manages TCP connections to servers.
 *
 * v1: Simple implementation - one connection per server, lazy connect,
 * auto-reconnect on failure.
 */
class ConnectionPool {
 public:
  explicit ConnectionPool(std::chrono::milliseconds connect_timeout);
  ~ConnectionPool();

  // Non-copyable, non-movable
  ConnectionPool(const ConnectionPool&) = delete;
  ConnectionPool& operator=(const ConnectionPool&) = delete;

  /**
   * Get or create connection to server.
   * @param addr Server address (host:port)
   * @return Socket pointer, or nullptr on failure
   */
  std::shared_ptr<asio::ip::tcp::socket> GetConnection(const std::string& addr);

  /** Close and remove connection. */
  void CloseConnection(const std::string& addr);

  /** Close all connections. */
  void CloseAll();

  /** Check if server is reachable. */
  bool IsHealthy(const std::string& addr) const;

 private:
  std::shared_ptr<asio::ip::tcp::socket> CreateConnection(
      const std::string& addr);

  asio::io_context io_context_;
  asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
  std::thread io_thread_;

  std::chrono::milliseconds connect_timeout_;

  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::weak_ptr<asio::ip::tcp::socket>>
      connections_;
};

}  // namespace rollingraft
