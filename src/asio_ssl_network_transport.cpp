#include "rollingraft/asio_ssl_network_transport.h"
#include "rollingraft/asio_ssl_context_factory.h"
#include "rollingraft/logger.h"

#include <asio.hpp>
#include <asio/ssl.hpp>
#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace rollingraft {

class SslConnection : public std::enable_shared_from_this<SslConnection> {
 public:
  SslConnection(asio::io_context& io_ctx, asio::ssl::context& ssl_ctx,
                NodeId peer_id, const NodeAddr& addr)
      : ssl_(io_ctx, ssl_ctx), strand_(asio::make_strand(io_ctx)),
        peer_id_(peer_id), addr_(addr), connected_(false) {}

  SslConnection(asio::io_context& io_ctx, asio::ssl::context& ssl_ctx)
      : ssl_(io_ctx, ssl_ctx), strand_(asio::make_strand(io_ctx)),
        peer_id_(-1), addr_(""), connected_(false) {}

  asio::ssl::stream<asio::ip::tcp::socket>& SslStream() { return ssl_; }

  void StartServerHandshake(RpcRequestHandler handler) {
    auto self = shared_from_this();
    request_handler_ = std::move(handler);
    ssl_.async_handshake(
        asio::ssl::stream_base::server,
        asio::bind_executor(strand_, [this, self](std::error_code ec) {
          if (ec) { LOG_ERROR("SSL server handshake failed: {}", ec.message()); return; }
          connected_.store(true, std::memory_order_release);
          DoReadHeader();
        }));
  }

  void StartClientHandshake(std::function<void(std::error_code)> cb) {
    auto self = shared_from_this();
    ssl_.async_handshake(
        asio::ssl::stream_base::client,
        asio::bind_executor(strand_, [this, self, cb](std::error_code ec) {
          if (!ec) { connected_.store(true, std::memory_order_release); DoReadHeader(); }
          cb(ec);
        }));
  }

  void Close() {
    std::unordered_map<uint64_t, PendingCallback> drained;
    { std::lock_guard<std::mutex> lock(mutex_);
      std::error_code ec;
      ssl_.shutdown(ec);
      if (ssl_.next_layer().is_open()) ssl_.next_layer().close(ec);
      connected_.store(false, std::memory_order_release);
      drained = std::move(pending_callbacks_);
    }
    for (auto& [id, p] : drained) {
      if (p.timer) p.timer->cancel();
      if (p.callback) p.callback("", false, "Connection closed");
    }
  }

  bool IsConnected() const {
    return connected_.load(std::memory_order_acquire) && ssl_.next_layer().is_open();
  }

  void Send(const std::string& data, uint64_t correlation_id,
            RpcResponseCallback callback, std::chrono::milliseconds timeout) {
    auto self = shared_from_this();
    uint32_t length = static_cast<uint32_t>(data.size());
    auto msg = std::make_shared<std::string>();
    msg->resize(4 + data.size());
    (*msg)[0] = static_cast<char>((length >> 24) & 0xFF);
    (*msg)[1] = static_cast<char>((length >> 16) & 0xFF);
    (*msg)[2] = static_cast<char>((length >> 8) & 0xFF);
    (*msg)[3] = static_cast<char>(length & 0xFF);
    std::memcpy(msg->data() + 4, data.data(), data.size());

    auto timer = std::make_shared<asio::steady_timer>(strand_);
    timer->expires_after(timeout);
    { std::lock_guard<std::mutex> lock(self->mutex_);
      pending_callbacks_[correlation_id] = {std::move(callback), timer}; }

    timer->async_wait([self, correlation_id](std::error_code ec) {
      if (ec) return;
      RpcResponseCallback cb;
      { std::lock_guard<std::mutex> lock(self->mutex_);
        auto it = self->pending_callbacks_.find(correlation_id);
        if (it != self->pending_callbacks_.end()) {
          cb = std::move(it->second.callback);
          self->pending_callbacks_.erase(it);
        }
      }
      if (cb) cb("", false, "Request timeout");
    });

    asio::async_write(
        ssl_, asio::buffer(*msg),
        asio::bind_executor(strand_, [self, msg, correlation_id](std::error_code ec, std::size_t) {
          if (ec) {
            RpcResponseCallback cb;
            std::shared_ptr<asio::steady_timer> timer;
            { std::lock_guard<std::mutex> lock(self->mutex_);
              auto it = self->pending_callbacks_.find(correlation_id);
              if (it != self->pending_callbacks_.end()) {
                cb = std::move(it->second.callback);
                timer = std::move(it->second.timer);
                self->pending_callbacks_.erase(it);
              }
            }
            if (timer) timer->cancel();
            if (cb) cb("", false, "Send failed: " + ec.message());
            self->connected_.store(false, std::memory_order_release);
          }
        }));
  }

  void SetRequestHandler(RpcRequestHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    request_handler_ = std::move(handler);
  }

 private:
  void DoReadHeader() {
    auto self = shared_from_this();
    asio::async_read(ssl_, asio::buffer(header_buffer_),
      asio::bind_executor(strand_, [this, self](std::error_code ec, std::size_t) {
        if (ec) { connected_.store(false, std::memory_order_release); return; }
        uint32_t length = (static_cast<uint8_t>(header_buffer_[0]) << 24) |
                          (static_cast<uint8_t>(header_buffer_[1]) << 16) |
                          (static_cast<uint8_t>(header_buffer_[2]) << 8) |
                          static_cast<uint8_t>(header_buffer_[3]);
        if (length == 0) { DoReadHeader(); return; }
        if (length > 64 * 1024 * 1024) { connected_.store(false, std::memory_order_release); return; }
        body_buffer_.resize(length);
        DoReadBody(length);
      }));
  }

  void DoReadBody(uint32_t length) {
    auto self = shared_from_this();
    asio::async_read(ssl_, asio::buffer(body_buffer_),
      asio::bind_executor(strand_, [this, self, length](std::error_code ec, std::size_t) {
        if (ec) { connected_.store(false, std::memory_order_release); return; }
        std::string response;
        if (request_handler_) request_handler_(peer_id_, body_buffer_, response);
        if (!response.empty()) {
          uint32_t resp_len = static_cast<uint32_t>(response.size());
          auto resp_msg = std::make_shared<std::string>();
          resp_msg->resize(4 + response.size());
          (*resp_msg)[0] = static_cast<char>((resp_len >> 24) & 0xFF);
          (*resp_msg)[1] = static_cast<char>((resp_len >> 16) & 0xFF);
          (*resp_msg)[2] = static_cast<char>((resp_len >> 8) & 0xFF);
          (*resp_msg)[3] = static_cast<char>(resp_len & 0xFF);
          std::memcpy(resp_msg->data() + 4, response.data(), response.size());
          asio::async_write(ssl_, asio::buffer(*resp_msg),
            asio::bind_executor(strand_, [self, resp_msg](std::error_code, std::size_t) {}));
        }
        DoReadHeader();
      }));
  }

  struct PendingCallback { RpcResponseCallback callback; std::shared_ptr<asio::steady_timer> timer; };
  asio::ssl::stream<asio::ip::tcp::socket> ssl_;
  asio::strand<asio::io_context::executor_type> strand_;
  NodeId peer_id_; NodeAddr addr_;
  std::atomic<bool> connected_{false};
  std::array<char, 4> header_buffer_;
  std::string body_buffer_;
  std::unordered_map<uint64_t, PendingCallback> pending_callbacks_;
  RpcRequestHandler request_handler_;
  mutable std::mutex mutex_;
};

class AsioSslNetworkTransport::Impl {
 public:
  explicit Impl(const TlsConfig& tls_config) : tls_config_(tls_config) {}

  Status Initialize(const NodeAddr& listen_addr, RpcRequestHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (initialized_) return Status::Error("Already initialized");
    request_handler_ = handler;

    AsioSslContextFactory factory(tls_config_);
    auto status = factory.CreateServerContext(ssl_ctx_);
    if (!status.ok()) return status;

    auto pos = listen_addr.find(':');
    if (pos == std::string::npos) return Status::Error("Invalid address format: " + listen_addr);
    std::string host = listen_addr.substr(0, pos);
    uint16_t port = static_cast<uint16_t>(std::stoi(listen_addr.substr(pos + 1)));

    try {
      asio::ip::tcp::endpoint endpoint(asio::ip::make_address(host), port);
      acceptor_ = std::make_unique<asio::ip::tcp::acceptor>(io_context_);
      acceptor_->open(endpoint.protocol());
      acceptor_->set_option(asio::socket_base::reuse_address(true));
      acceptor_->bind(endpoint);
      acceptor_->listen();
    } catch (const std::exception& e) {
      return Status::Error("Failed to create acceptor: " + std::string(e.what()));
    }
    initialized_ = true;
    return Status::OK();
  }

  void SetConnectionCallback(ConnectionCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    connection_callback_ = std::move(callback);
  }

  Status Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_) return Status::Error("Not initialized");
    if (running_) return Status::Error("Already started");
    running_ = true;
    DoAccept();
    io_thread_ = std::thread([this]() {
      auto guard = asio::make_work_guard(io_context_);
      io_context_.run();
    });
    return Status::OK();
  }

  Status Stop() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) return Status::OK();
    running_ = false;
    if (acceptor_) { std::error_code ec; acceptor_->close(ec); }
    for (auto& [id, peer] : peers_) peer->Stop();
    peers_.clear();
    io_context_.stop();
    if (io_thread_.joinable()) io_thread_.join();
    return Status::OK();
  }

  void SendRpc(NodeId to, const NodeAddr& addr, const std::string& request_data,
               uint64_t correlation_id, std::chrono::milliseconds timeout,
               RpcResponseCallback callback) {
    std::shared_ptr<class SslPeerConnection> peer;
    { std::lock_guard<std::mutex> lock(mutex_);
      auto it = peers_.find(to);
      if (it == peers_.end()) {
        peer = std::make_shared<SslPeerConnection>(io_context_, ssl_ctx_, to, addr);
        peers_[to] = peer;
        peer->StartConnecting();
      } else { peer = it->second; }
    }
    peer->Send(request_data, correlation_id, std::move(callback), timeout);
  }

 private:
  class SslPeerConnection : public std::enable_shared_from_this<SslPeerConnection> {
   public:
    SslPeerConnection(asio::io_context& io_ctx, asio::ssl::context& ssl_ctx,
                      NodeId peer_id, const NodeAddr& addr)
        : io_ctx_(io_ctx), strand_(asio::make_strand(io_ctx)), ssl_ctx_(ssl_ctx),
          peer_id_(peer_id), addr_(addr), state_(State::kDisconnected),
          reconnect_timer_(io_ctx), connect_timer_(io_ctx) {
      auto pos = addr.find(':');
      if (pos != std::string::npos) {
        host_ = addr.substr(0, pos);
        port_ = static_cast<uint16_t>(std::stoi(addr.substr(pos + 1)));
      }
    }
    ~SslPeerConnection() { Stop(); }

    void StartConnecting() {
      asio::post(strand_, [self = shared_from_this()]() {
        if (self->state_.load(std::memory_order_acquire) != State::kDisconnected) return;
        self->state_.store(State::kConnecting, std::memory_order_release);
        self->DoConnect();
      });
    }

    void Stop() {
      asio::post(strand_, [self = shared_from_this()]() {
        self->state_.store(State::kFailed, std::memory_order_release);
        self->reconnect_timer_.cancel();
        self->connect_timer_.cancel();
        if (self->conn_) { self->conn_->Close(); self->conn_.reset(); }
      });
    }

    void Send(const std::string& request_data, uint64_t correlation_id,
              RpcResponseCallback callback, std::chrono::milliseconds timeout) {
      asio::post(strand_, [self = shared_from_this(), request_data, correlation_id,
                           callback, timeout]() mutable {
        if (self->conn_ && self->conn_->IsConnected()) {
          self->conn_->Send(request_data, correlation_id, std::move(callback), timeout);
          return;
        }
        self->pending_sends_.push_back({request_data, correlation_id, std::move(callback), timeout});
        if (self->state_.load(std::memory_order_acquire) == State::kDisconnected) self->StartConnecting();
      });
    }

    bool IsConnected() const {
      return conn_ && conn_->IsConnected() && state_.load(std::memory_order_acquire) == State::kConnected;
    }

   private:
    enum class State { kDisconnected, kConnecting, kConnected, kFailed };

    void DoConnect() {
      conn_ = std::make_shared<SslConnection>(io_ctx_, ssl_ctx_, peer_id_, addr_);
      auto self = shared_from_this();

      connect_timer_.expires_after(std::chrono::seconds(5));
      connect_timer_.async_wait(asio::bind_executor(strand_, [self](std::error_code ec) {
        if (ec) return;
        if (self->state_.load(std::memory_order_acquire) == State::kConnecting) {
          if (self->conn_) self->conn_->Close();
          self->state_.store(State::kDisconnected, std::memory_order_release);
          self->DrainPendingSendsWithError("Connect timeout");
        }
      }));

      asio::ip::tcp::endpoint endpoint(asio::ip::make_address(host_), port_);
      conn_->SslStream().next_layer().async_connect(endpoint,
        asio::bind_executor(strand_, [self](std::error_code ec) {
          self->connect_timer_.cancel();
          if (ec) {
            if (self->conn_) self->conn_->Close();
            self->conn_.reset();
            self->state_.store(State::kDisconnected, std::memory_order_release);
            self->DrainPendingSendsWithError("Connect failed: " + ec.message());
            return;
          }
          self->conn_->StartClientHandshake([self](std::error_code handshake_ec) {
            if (handshake_ec) {
              if (self->conn_) self->conn_->Close();
              self->conn_.reset();
              self->state_.store(State::kDisconnected, std::memory_order_release);
              self->DrainPendingSendsWithError("SSL handshake failed: " + handshake_ec.message());
              return;
            }
            self->state_.store(State::kConnected, std::memory_order_release);
            self->FlushPendingSends();
          });
        }));
    }

    void FlushPendingSends() {
      auto sends = std::move(pending_sends_);
      for (auto& s : sends) {
        if (conn_) conn_->Send(s.request_data, s.correlation_id, s.callback, s.timeout);
      }
    }

    void DrainPendingSendsWithError(const std::string& error_msg) {
      auto sends = std::move(pending_sends_);
      for (auto& s : sends) { if (s.callback) s.callback("", false, error_msg); }
    }

    struct PendingSend {
      std::string request_data;
      uint64_t correlation_id;
      RpcResponseCallback callback;
      std::chrono::milliseconds timeout;
    };

    asio::io_context& io_ctx_;
    asio::strand<asio::io_context::executor_type> strand_;
    asio::ssl::context& ssl_ctx_;
    NodeId peer_id_;
    NodeAddr addr_;
    std::string host_;
    uint16_t port_ = 0;
    std::atomic<State> state_{State::kDisconnected};
    std::shared_ptr<SslConnection> conn_;
    std::deque<PendingSend> pending_sends_;
    asio::steady_timer reconnect_timer_;
    asio::steady_timer connect_timer_;
  };

  void DoAccept() {
    auto conn = std::make_shared<SslConnection>(io_context_, ssl_ctx_);
    acceptor_->async_accept(conn->SslStream().next_layer(),
      [this, conn](std::error_code ec) {
        if (!running_) return;
        if (!ec) {
          conn->StartServerHandshake(request_handler_);
          if (connection_callback_) {
            NodeAddr remote_addr;
            try { remote_addr = conn->SslStream().next_layer().remote_endpoint().address().to_string(); } catch (...) {}
            connection_callback_(-1, remote_addr, true);
          }
        }
        DoAccept();
      });
  }

  TlsConfig tls_config_;
  asio::ssl::context ssl_ctx_{asio::ssl::context::tls_server};
  asio::io_context io_context_;
  std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
  std::thread io_thread_;
  RpcRequestHandler request_handler_;
  ConnectionCallback connection_callback_;
  std::unordered_map<NodeId, std::shared_ptr<SslPeerConnection>> peers_;
  bool initialized_ = false;
  bool running_ = false;
  std::mutex mutex_;
};

AsioSslNetworkTransport::AsioSslNetworkTransport(const TlsConfig& tls_config)
    : impl_(std::make_unique<Impl>(tls_config)) {}
AsioSslNetworkTransport::~AsioSslNetworkTransport() = default;

Status AsioSslNetworkTransport::Initialize(const NodeAddr& listen_addr, RpcRequestHandler handler) {
  return impl_->Initialize(listen_addr, std::move(handler));
}
void AsioSslNetworkTransport::SetConnectionCallback(ConnectionCallback callback) {
  impl_->SetConnectionCallback(std::move(callback));
}
Status AsioSslNetworkTransport::Start() { return impl_->Start(); }
Status AsioSslNetworkTransport::Stop() { return impl_->Stop(); }
void AsioSslNetworkTransport::SendRpc(NodeId to, const NodeAddr& addr, const std::string& request_data,
                                      uint64_t correlation_id, std::chrono::milliseconds timeout,
                                      RpcResponseCallback callback) {
  impl_->SendRpc(to, addr, request_data, correlation_id, timeout, std::move(callback));
}

}  // namespace rollingraft
