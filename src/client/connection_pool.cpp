/**
 * @file connection_pool.cpp
 * @brief ConnectionPool implementation
 */

#include "connection_pool.h"

#include <thread>

namespace rollingraft {

ConnectionPool::ConnectionPool(std::chrono::milliseconds connect_timeout)
    : connect_timeout_(connect_timeout) {}

ConnectionPool::~ConnectionPool() { CloseAll(); }

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
    asio::io_context io_context;
    asio::ip::tcp::resolver resolver(io_context);
    auto endpoints = resolver.resolve(host, port_str);

    auto socket = std::make_shared<asio::ip::tcp::socket>(io_context);

    // Connect with timeout using async operations
    std::error_code connect_ec;
    asio::steady_timer timer(io_context);
    bool connect_done = false;

    timer.expires_after(connect_timeout_);
    timer.async_wait([&](std::error_code ec) {
      if (!ec && !connect_done) {
        socket->close();
      }
    });

    asio::async_connect(
        *socket, endpoints,
        [&](std::error_code ec, const asio::ip::tcp::endpoint&) {
          connect_done = true;
          connect_ec = ec;
          timer.cancel();
        });

    io_context.run();

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
