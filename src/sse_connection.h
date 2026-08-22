#pragma once

#include <array>
#include <atomic>
#include <deque>
#include <functional>
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
  using CloseCallback = std::function<void(SseConnection*)>;

  SseConnection(SocketVariant socket, asio::io_context::strand strand,
                CloseCallback close_callback = {});

  void EnqueueEvent(const std::string& event_data);
  void Start();
  bool IsOpen() const;
  void Close();

 private:
  void DoWrite();
  void OnWrite(std::error_code ec, std::size_t bytes);
  void WatchForDisconnect();
  void MarkClosed();

  SocketVariant socket_;
  asio::io_context::strand strand_;
  std::atomic<bool> open_{true};
  std::atomic<bool> writing_{false};
  CloseCallback close_callback_;
  std::array<char, 1> disconnect_buffer_{};

  std::mutex queue_mtx_;
  std::deque<std::string> queue_;
  static constexpr size_t kMaxQueueSize = 100;

  std::string write_buffer_;
};

}  // namespace rollingraft
