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
    // Use a temporary io_context to avoid blocking the pool's io_context
    // worker thread. If CreateConnection is called from within an
    // io_context_ handler (e.g. a callback), promise/future on the same
    // io_context would deadlock because the single worker thread is
    // blocked waiting for itself to run the async completion handlers.
    asio::io_context temp_io;
    asio::executor_work_guard<asio::io_context::executor_type> temp_work(
        temp_io.get_executor());

    asio::ip::tcp::socket temp_socket(temp_io);
    std::error_code connect_ec;
    std::atomic<bool> completed{false};

    auto resolver = std::make_shared<asio::ip::tcp::resolver>(temp_io);
    resolver->async_resolve(
        host, port_str,
        [&temp_socket, resolver, &connect_ec, &completed](
            std::error_code ec,
            asio::ip::tcp::resolver::results_type endpoints) {
          if (ec) {
            if (!completed.exchange(true)) {
              connect_ec = ec;
            }
            return;
          }
          asio::async_connect(
              temp_socket, endpoints,
              [&connect_ec, &completed](std::error_code ec,
                                        const asio::ip::tcp::endpoint&) {
                if (!completed.exchange(true)) {
                  connect_ec = ec;
                }
              });
        });

    asio::steady_timer timer(temp_io);
    timer.expires_after(connect_timeout_);
    timer.async_wait([&connect_ec, &completed](std::error_code ec) {
      if (!ec && !completed.exchange(true)) {
        connect_ec = asio::error::timed_out;
      }
    });

    // Run the temporary io_context with a hard ceiling.
    temp_io.run_for(connect_timeout_ + std::chrono::milliseconds(100));

    if (!completed.load()) {
      return nullptr;  // Timeout
    }
    if (connect_ec) {
      return nullptr;
    }

    // Transfer the connected native socket to the pool's io_context.
    auto socket = std::make_shared<asio::ip::tcp::socket>(io_context_);
    auto native_fd = temp_socket.release();
    socket->assign(asio::ip::tcp::v4(), native_fd);

    // Enable TCP keepalive to detect half-open connections.
    asio::socket_base::keep_alive keep_alive_option(true);
    socket->set_option(keep_alive_option);

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
