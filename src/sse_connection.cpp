#include "sse_connection.h"

namespace rollingraft {

SseConnection::SseConnection(asio::ip::tcp::socket socket,
                             asio::io_context::strand strand)
    : socket_(std::move(socket)), strand_(std::move(strand)) {}

void SseConnection::EnqueueEvent(const std::string& event_data) {
  if (!open_.load()) return;

  {
    std::lock_guard<std::mutex> lock(queue_mtx_);
    if (queue_.size() >= kMaxQueueSize) {
      // Backpressure: drop oldest events
      queue_.pop_front();
    }
    queue_.push_back(event_data);
  }

  bool expected = false;
  if (writing_.compare_exchange_strong(expected, true)) {
    asio::post(strand_, [self = shared_from_this()] { self->DoWrite(); });
  }
}

void SseConnection::Start() {
  std::string headers =
      "HTTP/1.1 200 OK\r\n"
      "Content-Type: text/event-stream\r\n"
      "Cache-Control: no-cache\r\n"
      "Connection: keep-alive\r\n"
      "\r\n";

  auto headers_ptr = std::make_shared<std::string>(std::move(headers));
  asio::async_write(
      socket_, asio::buffer(*headers_ptr),
      asio::bind_executor(strand_,
                          [self = shared_from_this(), headers_ptr](
                              std::error_code ec, std::size_t) {
                            if (!ec) {
                              self->DoWrite();
                            } else {
                              self->open_.store(false);
                              self->writing_.store(false);
                            }
                          }));
}

bool SseConnection::IsOpen() const { return open_.load(); }

void SseConnection::Close() {
  open_.store(false);
  asio::post(strand_, [self = shared_from_this()] {
    std::error_code ec;
    self->socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    self->socket_.close(ec);
  });
}

void SseConnection::DoWrite() {
  if (!open_.load()) {
    writing_.store(false);
    return;
  }

  {
    std::lock_guard<std::mutex> lock(queue_mtx_);
    if (queue_.empty()) {
      writing_.store(false);
      return;
    }
    write_buffer_ = std::move(queue_.front());
    queue_.pop_front();
  }

  asio::async_write(
      socket_, asio::buffer(write_buffer_),
      asio::bind_executor(
          strand_,
          [self = shared_from_this()](std::error_code ec, std::size_t bytes) {
            self->OnWrite(ec, bytes);
          }));
}

void SseConnection::OnWrite(std::error_code ec, std::size_t /*bytes*/) {
  if (ec) {
    open_.store(false);
    writing_.store(false);
    return;
  }
  DoWrite();
}

}  // namespace rollingraft
