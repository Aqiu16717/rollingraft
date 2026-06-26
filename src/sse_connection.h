#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <variant>

#include <asio.hpp>

#include <asio/ssl.hpp>

namespace rollingraft {

class SseConnection : public std::enable_shared_from_this<SseConnection> {
 public:
  using SocketVariant =
      std::variant<asio::ip::tcp::socket, asio::ssl::stream<asio::ip::tcp::socket>>;

  SseConnection(SocketVariant socket, asio::io_context::strand strand);

  void EnqueueEvent(const std::string& event_data);
  void Start();
  bool IsOpen() const;
  void Close();

 private:
  void DoWrite();
  void OnWrite(std::error_code ec, std::size_t bytes);

  SocketVariant socket_;
  asio::io_context::strand strand_;
  std::atomic<bool> open_{true};
  std::atomic<bool> writing_{false};

  std::mutex queue_mtx_;
  std::deque<std::string> queue_;
  static constexpr size_t kMaxQueueSize = 100;

  std::string write_buffer_;
};

}  // namespace rollingraft
