/**
 * @file asio_network_transport.cpp
 * @brief Asio-based TCP network transport implementation
 */

#include <asio.hpp>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
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
      : socket_(io_ctx),
        strand_(asio::make_strand(io_ctx)),
        peer_id_(peer_id),
        addr_(addr),
        connected_(false) {}

  TcpConnection(asio::io_context& io_ctx)
      : socket_(io_ctx),
        strand_(asio::make_strand(io_ctx)),
        peer_id_(-1),
        addr_(""),
        connected_(false) {}

  asio::ip::tcp::socket& Socket() { return socket_; }

  void Start() {
    LOG_INFO("TcpConnection::Start: peer_id={}, socket_open={}", peer_id_,
             socket_.is_open());
    connected_ = true;
    DoReadHeader();
    LOG_INFO("TcpConnection::Start completed: peer_id={}", peer_id_);
  }

  void Close() {
    std::unordered_map<uint64_t, PendingCallback> drained;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (socket_.is_open()) {
        std::error_code ec;
        socket_.close(ec);
      }
      connected_ = false;
      drained = std::move(pending_callbacks_);
      pending_callbacks_.clear();
    }
    for (auto& [id, pending] : drained) {
      if (pending.timer) pending.timer->cancel();
      if (pending.callback) pending.callback("", false, "Connection closed");
    }
  }

  bool IsConnected() const { return connected_ && socket_.is_open(); }

  NodeId GetPeerId() const { return peer_id_; }
  NodeAddr GetAddr() const { return addr_; }

  void Send(const std::string& data, uint64_t correlation_id,
            RpcResponseCallback callback, std::chrono::milliseconds timeout) {
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

    // Set timeout that covers FULL request-response lifecycle
    auto timer = std::make_shared<asio::steady_timer>(strand_);
    timer->expires_after(timeout);

    // CRITICAL: Store callback + timer BEFORE sending to avoid race condition
    // where response arrives before callback is stored
    {
      std::lock_guard<std::mutex> lock(self->mutex_);
      pending_callbacks_[correlation_id] = {std::move(callback), timer};
    }

    timer->async_wait([self, correlation_id](std::error_code ec) {
      if (ec) return;  // Cancelled
      RpcResponseCallback cb;
      {
        std::lock_guard<std::mutex> lock(self->mutex_);
        auto it = self->pending_callbacks_.find(correlation_id);
        if (it != self->pending_callbacks_.end()) {
          cb = std::move(it->second.callback);
          self->pending_callbacks_.erase(it);
        }
      }
      if (cb) {
        cb("", false, "Request timeout");
      }
      self->connected_ = false;
    });

    // Send message (serialized through strand)
    asio::async_write(
        socket_, asio::buffer(*msg),
        asio::bind_executor(strand_, [self, msg, correlation_id](
                                         std::error_code ec, std::size_t) {
          if (ec) {
            RpcResponseCallback cb;
            std::shared_ptr<asio::steady_timer> timer;
            {
              std::lock_guard<std::mutex> lock(self->mutex_);
              auto it = self->pending_callbacks_.find(correlation_id);
              if (it != self->pending_callbacks_.end()) {
                cb = std::move(it->second.callback);
                timer = std::move(it->second.timer);
                self->pending_callbacks_.erase(it);
              }
            }
            if (timer) timer->cancel();
            if (cb) {
              cb("", false, "Send failed: " + ec.message());
            }
            self->connected_ = false;
          }
          // On success: do NOT cancel timer — it covers response wait too
          // Response will be handled by DoRead -> HandleMessage
        }));
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
        asio::bind_executor(strand_, [this, self](std::error_code ec,
                                                  std::size_t bytes) {
          if (!ec) {
            LOG_INFO("Read header: peer_id={}, bytes={}", peer_id_, bytes);
          }
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
        }));
  }

  void DoReadBody(uint32_t /*length*/) {
    auto self = shared_from_this();
    asio::async_read(
        socket_, asio::buffer(body_buffer_),
        asio::bind_executor(strand_,
                            [this, self](std::error_code ec,
                                         std::size_t /*bytes_transferred*/) {
                              if (ec) {
                                LOG_ERROR("Read body error: {}", ec.message());
                                connected_ = false;
                                return;
                              }

                              HandleMessage();
                              DoReadHeader();
                            }));
  }

  void HandleMessage() {
    std::lock_guard<std::mutex> lock(mutex_);

    LOG_INFO("HandleMessage: peer_id={}, has_handler={}, pending_callbacks={}",
             peer_id_, (request_handler_ ? "yes" : "no"),
             pending_callbacks_.size());

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

      asio::async_write(
          socket_, asio::buffer(*msg),
          asio::bind_executor(
              strand_, [self, msg](std::error_code ec, std::size_t) {
                if (ec) {
                  LOG_ERROR("Write response error: {}", ec.message());
                }
              }));
    } else {
      // This is a client connection, handle response by correlation_id
      auto callback = ExtractCallbackLocked(body_buffer_);
      if (callback) {
        callback(body_buffer_, true, "");
      }
    }
  }

 private:
  asio::ip::tcp::socket socket_;
  asio::strand<asio::io_context::executor_type> strand_;
  NodeId peer_id_;
  NodeAddr addr_;
  std::atomic<bool> connected_;

  struct PendingCallback {
    RpcResponseCallback callback;
    std::shared_ptr<asio::steady_timer> timer;
  };

  mutable std::mutex mutex_;
  RpcRequestHandler request_handler_;
  std::unordered_map<uint64_t, PendingCallback> pending_callbacks_;

  char header_buffer_[4];
  std::string body_buffer_;

  RpcResponseCallback ExtractCallbackLocked(const std::string& response_data) {
    // Lightweight JSON parse to extract correlation_id
    uint64_t correlation_id = 0;
    try {
      auto pos = response_data.find("\"correlation_id\"");
      if (pos != std::string::npos) {
        auto colon_pos = response_data.find(':', pos + 16);
        if (colon_pos != std::string::npos) {
          correlation_id = std::stoull(response_data.substr(colon_pos + 1));
        }
      }
    } catch (...) {
      // Fall through
    }

    if (correlation_id != 0) {
      auto it = pending_callbacks_.find(correlation_id);
      if (it != pending_callbacks_.end()) {
        auto cb = std::move(it->second.callback);
        if (it->second.timer) it->second.timer->cancel();
        pending_callbacks_.erase(it);
        return cb;
      }
    }

    // Fallback: if no correlation_id match, return any pending callback
    if (!pending_callbacks_.empty()) {
      auto it = pending_callbacks_.begin();
      auto cb = std::move(it->second.callback);
      if (it->second.timer) it->second.timer->cancel();
      pending_callbacks_.erase(it);
      return cb;
    }

    return nullptr;
  }
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
      // Create acceptor with open+set_option+bind sequence to enable
      // SO_REUSEADDR before binding
      acceptor_ = std::make_unique<asio::ip::tcp::acceptor>(io_context_);
      acceptor_->open(endpoint.protocol());
      acceptor_->set_option(asio::socket_base::reuse_address(true));
      acceptor_->bind(endpoint);
      acceptor_->listen();
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

    if (running_.load(std::memory_order_relaxed)) {
      return Status::OK();
    }

    running_.store(true, std::memory_order_relaxed);

    // Start io_context thread pool
    work_guard_ = std::make_unique<
        asio::executor_work_guard<asio::io_context::executor_type>>(
        io_context_.get_executor());

    unsigned int num_threads =
        std::max(2u, std::thread::hardware_concurrency());
    io_threads_.reserve(num_threads);
    for (unsigned int i = 0; i < num_threads; ++i) {
      io_threads_.emplace_back([this]() {
        try {
          io_context_.run();
        } catch (const std::exception& e) {
          LOG_ERROR("IO context error: {}", e.what());
        }
      });
    }

    // Start accepting connections
    DoAccept();

    LOG_INFO("AsioNetworkTransport started on {} ({} threads)", listen_addr_,
             num_threads);
    return Status::OK();
  }

  Status Stop() override {
    {
      std::lock_guard<std::mutex> lock(mutex_);

      if (!running_.load(std::memory_order_relaxed)) {
        return Status::OK();
      }

      running_.store(false, std::memory_order_relaxed);

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
    }

    // Join all io_context threads
    for (auto& t : io_threads_) {
      if (t.joinable()) {
        t.join();
      }
    }
    io_threads_.clear();

    LOG_INFO("AsioNetworkTransport stopped");
    return Status::OK();
  }

  void SendRpc(NodeId to, const NodeAddr& addr, const std::string& request_data,
               uint64_t correlation_id, std::chrono::milliseconds timeout,
               RpcResponseCallback callback) override {
    if (!running_.load(std::memory_order_relaxed)) {
      callback("", false, "Transport stopped");
      return;
    }
    // Post to io_context for async execution on the thread pool
    asio::post(io_context_, [this, to, addr, request_data, correlation_id,
                             timeout, callback]() {
      auto conn = GetOrCreateConnection(to, addr);
      if (!conn) {
        callback("", false, "Failed to connect to " + addr);
        return;
      }
      conn->Send(request_data, correlation_id, callback, timeout);
    });
  }

 private:
  void DoAccept() {
    auto new_conn = std::make_shared<TcpConnection>(io_context_);

    acceptor_->async_accept(new_conn->Socket(), [this,
                                                 new_conn](std::error_code ec) {
      if (!ec) {
        new_conn->SetRequestHandler(request_handler_);
        new_conn->Start();
        // Inbound connections are NOT stored in connections_ (which is
        // for outbound peer connections only). Storing with peer_id=-1
        // would cause overwriting and use-after-free.
        LOG_INFO("Accepted inbound connection from {}",
                 new_conn->Socket().remote_endpoint().address().to_string());
      } else if (running_.load(std::memory_order_relaxed)) {
        LOG_ERROR("Accept error: {}", ec.message());
      }

      if (running_.load(std::memory_order_relaxed)) {
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

      // Use synchronous connect to ensure connection is ready before returning
      std::error_code ec;
      conn->Socket().connect(endpoint, ec);
      if (ec) {
        LOG_WARN("Failed to connect to {}: {}", addr, ec.message());
        return nullptr;
      }

      // Start reading immediately after successful connection
      conn->Start();
      connections_[peer_id] = conn;
      LOG_INFO("Connected to peer {} at {}", peer_id, addr);
      return conn;
    } catch (const std::exception& e) {
      LOG_ERROR("Failed to create connection: {}", e.what());
      return nullptr;
    }
  }

 private:
  mutable std::mutex mutex_;
  bool initialized_ = false;
  std::atomic<bool> running_{false};

  asio::io_context io_context_;
  std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>>
      work_guard_;
  std::vector<std::thread> io_threads_;

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
