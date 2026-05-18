#pragma once

#include <asio.hpp>
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

namespace rollingraft {

/**
 * Manages a single Server-Sent Events (SSE) HTTP connection.
 *
 * Thread-safe for event enqueueing from any thread.
 * All ASIO operations run on the provided strand.
 */
class SseConnection : public std::enable_shared_from_this<SseConnection> {
 public:
  SseConnection(asio::ip::tcp::socket socket, asio::io_context::strand strand);

  /** Enqueue an event string for delivery. Thread-safe. */
  void EnqueueEvent(const std::string& event_data);

  /**
   * Write SSE HTTP headers and start the send loop.
   * Must be called from the strand.
   */
  void Start();

  /** Check if connection is still open. */
  bool IsOpen() const;

  /** Gracefully close the connection. */
  void Close();

 private:
  void DoWrite();
  void OnWrite(std::error_code ec, std::size_t bytes);

  asio::ip::tcp::socket socket_;
  asio::io_context::strand strand_;
  std::atomic<bool> open_{true};
  std::atomic<bool> writing_{false};

  std::mutex queue_mtx_;
  std::deque<std::string> queue_;
  static constexpr size_t kMaxQueueSize = 100;

  std::string write_buffer_;
};

}  // namespace rollingraft
