/**
 * @file connection_pool.cpp
 * @brief ConnectionPool implementation
 */

#include "connection_pool.h"

#include <thread>

namespace rollingraft {

ConnectionPool::ConnectionPool(std::chrono::milliseconds connect_timeout)
    : work_guard_(asio::make_work_guard(io_context_)),
      connect_timeout_(connect_timeout) {
  io_thread_ = std::thread([this]() { io_context_.run(); });
}

ConnectionPool::~ConnectionPool() {
  CloseAll();
  work_guard_.reset();
  if (io_thread_.joinable()) {
    io_thread_.join();
  }
}

std::shared_ptr<asio::ip::tcp::socket> ConnectionPool::GetConnection(
    const std::string& addr) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Check existing connection
  auto it = connections_.find(addr);
  if (it != connections_.end()) {
    if (auto conn = it->second.lock()) {
      if (conn->is_open()) {
        return conn;
      }
    }
  }

  // Create new connection
  auto conn = CreateConnection(addr);
  if (conn) {
    connections_[addr] = conn;
  }
  return conn;
}

std::shared_ptr<asio::ip::tcp::socket> ConnectionPool::CreateConnection(
    const std::string& addr) {
  // Parse address
  auto colon_pos = addr.find(':');
  if (colon_pos == std::string::npos) {
    return nullptr;
  }

  std::string host = addr.substr(0, colon_pos);
  std::string port_str = addr.substr(colon_pos + 1);

  try {
    auto socket = std::make_shared<asio::ip::tcp::socket>(io_context_);

    // Use promise/future for synchronous wait on async operations
    auto promise = std::make_shared<std::promise<std::error_code>>();
    auto future = promise->get_future();
    auto completed = std::make_shared<std::atomic<bool>>(false);

    auto resolver = std::make_shared<asio::ip::tcp::resolver>(io_context_);
    resolver->async_resolve(
        host, port_str,
        [socket, resolver, promise, completed](
            std::error_code ec,
            asio::ip::tcp::resolver::results_type endpoints) {
          if (ec) {
            if (!completed->exchange(true)) {
              promise->set_value(ec);
            }
            return;
          }
          asio::async_connect(
              *socket, endpoints,
              [promise, completed](std::error_code ec,
                                   const asio::ip::tcp::endpoint&) {
                if (!completed->exchange(true)) {
                  promise->set_value(ec);
                }
              });
        });

    auto timer = std::make_shared<asio::steady_timer>(io_context_);
    timer->expires_after(connect_timeout_);
    timer->async_wait([promise, completed](std::error_code ec) {
      if (!ec && !completed->exchange(true)) {
        promise->set_value(asio::error::timed_out);
      }
    });

    if (future.wait_for(connect_timeout_ + std::chrono::milliseconds(100)) !=
        std::future_status::ready) {
      return nullptr;
    }

    auto connect_ec = future.get();
    if (connect_ec) {
      return nullptr;
    }

    return socket;
  } catch (...) {
    return nullptr;
  }
}

void ConnectionPool::CloseConnection(const std::string& addr) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = connections_.find(addr);
  if (it != connections_.end()) {
    if (auto conn = it->second.lock()) {
      std::error_code ec;
      conn->close(ec);
    }
    connections_.erase(it);
  }
}

void ConnectionPool::CloseAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& [addr, weak_conn] : connections_) {
    if (auto conn = weak_conn.lock()) {
      std::error_code ec;
      conn->close(ec);
    }
  }
  connections_.clear();
}

bool ConnectionPool::IsHealthy(const std::string& addr) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = connections_.find(addr);
  if (it != connections_.end()) {
    if (auto conn = it->second.lock()) {
      return conn->is_open();
    }
  }
  return false;
}

}  // namespace rollingraft
