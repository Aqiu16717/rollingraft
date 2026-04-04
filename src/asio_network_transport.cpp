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
#include "rollingraft/network_transport.h"
#include "rollingraft/types.h"

namespace rollingraft {

// Message format: [length: 4 bytes (big-endian)][data: length bytes]
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
 public:
  TcpConnection(asio::io_context& io_ctx, NodeId peer_id, const NodeAddr& addr)
      : socket_(io_ctx), peer_id_(peer_id), addr_(addr), connected_(false) {}

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
    asio::async_write(
        socket_, asio::buffer(*msg),
        [self, msg, callback, timer](std::error_code ec, std::size_t) {
          timer->cancel();
          if (ec) {
            callback("", false, "Send failed: " + ec.message());
            self->connected_ = false;
          }
          // Response will be handled by DoRead
        });

    // Store callback for response matching
    std::lock_guard<std::mutex> lock(self->mutex_);
    pending_callbacks_.push_back(callback);
  }

  void SetRequestHandler(RpcRequestHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    request_handler_ = handler;
  }

 private:
  void DoReadHeader() {
    auto self = shared_from_this();
    asio::async_read(
        socket_, asio::buffer(header_buffer_),
        [this, self](std::error_code ec, std::size_t) {
          if (ec) {
            if (ec != asio::error::eof) {
              LOG_ERROR("Read header error: {}", ec.message());
            }
            connected_ = false;
            return;
          }

          uint32_t length = (static_cast<uint8_t>(header_buffer_[0]) << 24) |
                            (static_cast<uint8_t>(header_buffer_[1]) << 16) |
                            (static_cast<uint8_t>(header_buffer_[2]) << 8) |
                            static_cast<uint8_t>(header_buffer_[3]);

          if (length > 0 && length < 100 * 1024 * 1024) {  // Max 100MB
            body_buffer_.resize(length);
            DoReadBody(length);
          } else {
            LOG_ERROR("Invalid message length: {}", length);
            connected_ = false;
          }
        });
  }

  void DoReadBody(uint32_t length) {
    auto self = shared_from_this();
    asio::async_read(socket_, asio::buffer(body_buffer_),
                     [this, self](std::error_code ec, std::size_t /*bytes_transferred*/) {
                       if (ec) {
                         LOG_ERROR("Read body error: {}", ec.message());
                         connected_ = false;
                         return;
                       }

                       HandleMessage();
                       DoReadHeader();
                     });
  }

  void HandleMessage() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (request_handler_) {
      // This is a server connection, handle request
      std::string response;
      request_handler_(peer_id_, body_buffer_, response);

      // Send response
      uint32_t length = static_cast<uint32_t>(response.size());
      char header[4];
      header[0] = static_cast<char>((length >> 24) & 0xFF);
      header[1] = static_cast<char>((length >> 16) & 0xFF);
      header[2] = static_cast<char>((length >> 8) & 0xFF);
      header[3] = static_cast<char>(length & 0xFF);

      auto self = shared_from_this();
      auto msg = std::make_shared<std::string>(header, 4);
      msg->append(response);

      asio::async_write(socket_, asio::buffer(*msg),
                        [self, msg](std::error_code ec, std::size_t) {
                          if (ec) {
                            LOG_ERROR("Write response error: {}", ec.message());
                          }
                        });
    } else if (!pending_callbacks_.empty()) {
      // This is a client connection, handle response
      auto callback = pending_callbacks_.front();
      pending_callbacks_.pop_front();
      callback(body_buffer_, true, "");
    }
  }

 private:
  asio::ip::tcp::socket socket_;
  NodeId peer_id_;
  NodeAddr addr_;
  std::atomic<bool> connected_;

  mutable std::mutex mutex_;
  RpcRequestHandler request_handler_;
  std::deque<RpcResponseCallback> pending_callbacks_;

  char header_buffer_[4];
  std::string body_buffer_;
};

class AsioNetworkTransport : public NetworkTransport {
 public:
  AsioNetworkTransport() = default;
  ~AsioNetworkTransport() override { Stop(); }

  Status Initialize(const NodeAddr& listen_addr,
                    RpcRequestHandler handler) override {
    std::lock_guard<std::mutex> lock(mutex_);

    if (initialized_) {
      return Status::Error("Already initialized");
    }

    listen_addr_ = listen_addr;
    request_handler_ = handler;

    // Parse address
    auto pos = listen_addr.find(':');
    if (pos == std::string::npos) {
      return Status::Error("Invalid address format: " + listen_addr);
    }

    std::string host = listen_addr.substr(0, pos);
    uint16_t port =
        static_cast<uint16_t>(std::stoi(listen_addr.substr(pos + 1)));

    try {
      asio::ip::tcp::endpoint endpoint(asio::ip::make_address(host), port);
      acceptor_ =
          std::make_unique<asio::ip::tcp::acceptor>(io_context_, endpoint);
    } catch (const std::exception& e) {
      return Status::Error("Failed to create acceptor: " +
                           std::string(e.what()));
    }

    initialized_ = true;
    return Status::OK();
  }

  void SetConnectionCallback(ConnectionCallback callback) override {
    std::lock_guard<std::mutex> lock(mutex_);
    connection_callback_ = callback;
  }

  Status Start() override {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
      return Status::Error("Not initialized");
    }

    if (running_) {
      return Status::OK();
    }

    running_ = true;

    // Start io_context in a separate thread
    work_guard_ = std::make_unique<
        asio::executor_work_guard<asio::io_context::executor_type>>(
        io_context_.get_executor());

    io_thread_ = std::thread([this]() {
      try {
        io_context_.run();
      } catch (const std::exception& e) {
        LOG_ERROR("IO context error: {}", e.what());
      }
    });

    // Start accepting connections
    DoAccept();

    LOG_INFO("AsioNetworkTransport started on {}", listen_addr_);
    return Status::OK();
  }

  Status Stop() override {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!running_) {
      return Status::OK();
    }

    running_ = false;

    // Stop acceptor
    if (acceptor_ && acceptor_->is_open()) {
      std::error_code ec;
      acceptor_->close(ec);
    }

    // Close all connections
    for (auto& [id, conn] : connections_) {
      conn->Close();
    }
    connections_.clear();

    // Stop io_context
    work_guard_.reset();
    io_context_.stop();

    if (io_thread_.joinable()) {
      io_thread_.join();
    }

    LOG_INFO("AsioNetworkTransport stopped");
    return Status::OK();
  }

  void SendRpc(NodeId to, const NodeAddr& addr, const std::string& request_data,
               std::chrono::milliseconds timeout,
               RpcResponseCallback callback) override {
    auto conn = GetOrCreateConnection(to, addr);

    if (!conn || !conn->IsConnected()) {
      callback("", false, "Not connected");
      return;
    }

    conn->Send(request_data, callback, timeout);
  }

 private:
  void DoAccept() {
    auto new_conn = std::make_shared<TcpConnection>(io_context_);

    acceptor_->async_accept(
        new_conn->Socket(), [this, new_conn](std::error_code ec) {
          if (!ec) {
            new_conn->SetRequestHandler(request_handler_);
            new_conn->Start();
            {
              std::lock_guard<std::mutex> lock(mutex_);
              connections_[new_conn->GetPeerId()] = new_conn;
            }
            if (connection_callback_) {
              connection_callback_(new_conn->GetPeerId(), new_conn->GetAddr(),
                                   true);
            }
          } else if (running_) {
            LOG_ERROR("Accept error: {}", ec.message());
          }

          if (running_) {
            DoAccept();
          }
        });
  }

  std::shared_ptr<TcpConnection> GetOrCreateConnection(NodeId peer_id,
                                                       const NodeAddr& addr) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = connections_.find(peer_id);
    if (it != connections_.end() && it->second->IsConnected()) {
      return it->second;
    }

    // Parse address
    auto pos = addr.find(':');
    if (pos == std::string::npos) {
      return nullptr;
    }

    std::string host = addr.substr(0, pos);
    uint16_t port = static_cast<uint16_t>(std::stoi(addr.substr(pos + 1)));

    try {
      asio::ip::tcp::endpoint endpoint(asio::ip::make_address(host), port);

      auto conn = std::make_shared<TcpConnection>(io_context_, peer_id, addr);
      conn->Socket().async_connect(endpoint, [conn](std::error_code ec) {
        if (!ec) {
          conn->Start();
        } else {
          LOG_ERROR("Connect error: {}", ec.message());
        }
      });

      connections_[peer_id] = conn;
      return conn;
    } catch (const std::exception& e) {
      LOG_ERROR("Failed to create connection: {}", e.what());
      return nullptr;
    }
  }

 private:
  mutable std::mutex mutex_;
  bool initialized_ = false;
  bool running_ = false;

  asio::io_context io_context_;
  std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>>
      work_guard_;
  std::thread io_thread_;

  NodeAddr listen_addr_;
  std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
  RpcRequestHandler request_handler_;
  ConnectionCallback connection_callback_;

  std::unordered_map<NodeId, std::shared_ptr<TcpConnection>> connections_;
};

// Factory function
std::unique_ptr<NetworkTransport> CreateAsioNetworkTransport() {
  return std::make_unique<AsioNetworkTransport>();
}

// Default factory function (used by RaftNode)
std::unique_ptr<NetworkTransport> CreateDefaultNetworkTransport() {
  return std::make_unique<AsioNetworkTransport>();
}

}  // namespace rollingraft
