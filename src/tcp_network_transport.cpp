#include "rollingraft/network_transport.h"

#include <asio.hpp>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "rollingraft/logger.h"

namespace rollingraft {

// Message format: [length: 4 bytes (big-endian)][data: length bytes]
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
 public:
  TcpConnection(asio::io_context& io_ctx, NodeId peer_id, const NodeAddr& addr)
      : socket_(io_ctx),
        peer_id_(peer_id),
        addr_(addr),
        connected_(false) {}

  TcpConnection(asio::io_context& io_ctx)
      : socket_(io_ctx), peer_id_(-1), addr_(""), connected_(false) {}

  asio::ip::tcp::socket& Socket() { return socket_; }

  void Start() {
    connected_ = true;
    DoReadHeader();
  }

  void Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (socket_.is_open()) {
      std::error_code ec;
      socket_.close(ec);
    }
    connected_ = false;
  }

  bool IsConnected() const { return connected_ && socket_.is_open(); }

  NodeId GetPeerId() const { return peer_id_; }
  NodeAddr GetAddr() const { return addr_; }

  void Send(const std::string& data, RpcResponseCallback callback,
            std::chrono::milliseconds timeout) {
    auto self = shared_from_this();

    // Prepare message with length prefix
    uint32_t length = static_cast<uint32_t>(data.size());
    auto msg = std::make_shared<std::string>();
    msg->resize(4 + data.size());
    (*msg)[0] = static_cast<char>((length >> 24) & 0xFF);
    (*msg)[1] = static_cast<char>((length >> 16) & 0xFF);
    (*msg)[2] = static_cast<char>((length >> 8) & 0xFF);
    (*msg)[3] = static_cast<char>(length & 0xFF);
    std::memcpy(msg->data() + 4, data.data(), data.size());

    // Set timeout
    auto timer = std::make_shared<asio::steady_timer>(socket_.get_executor());
    timer->expires_after(timeout);
    timer->async_wait([self, callback](std::error_code ec) {
      if (!ec) {
        callback("", false, "Request timeout");
      }
    });

    // Send message
    asio::async_write(socket_, asio::buffer(*msg),
                      [self, msg, callback, timer](std::error_code ec, std::size_t) {
                        timer->cancel();
                        if (ec) {
                          callback("", false, "Send failed: " + ec.message());
                          self->connected_ = false;
