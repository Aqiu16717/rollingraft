/**
 * @file asio_network_transport.cpp
 * @brief Asio-based TCP network transport implementation
 */

#include <asio.hpp>
#include <asio/ssl.hpp>
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
#include <variant>

#include "rollingraft/asio_ssl_context_factory.h"
#include "rollingraft/logger.h"
#include "rollingraft/network_transport.h"
#include "rollingraft/tls_config.h"
#include "rollingraft/types.h"

namespace rollingraft {

// Message format: [length: 4 bytes (big-endian)][data: length bytes]
using SocketVariant =
    std::variant<asio::ip::tcp::socket,
                 asio::ssl::stream<asio::ip::tcp::socket>>;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
 public:
  TcpConnection(asio::io_context& io_ctx, NodeId peer_id, const NodeAddr& addr)
      : socket_(asio::ip::tcp::socket(io_ctx)),
        strand_(asio::make_strand(io_ctx)),
        peer_id_(peer_id),
        addr_(addr),
        connected_(false) {}

  TcpConnection(asio::io_context& io_ctx, asio::ssl::context& ssl_ctx,
                NodeId peer_id, const NodeAddr& addr)
      : socket_(asio::ssl::stream<asio::ip::tcp::socket>(io_ctx, ssl_ctx)),
        strand_(asio::make_strand(io_ctx)),
        peer_id_(peer_id),
        addr_(addr),
        connected_(false) {}

  TcpConnection(asio::io_context& io_ctx)
      : socket_(asio::ip::tcp::socket(io_ctx)),
        strand_(asio::make_strand(io_ctx)),
        peer_id_(-1),
        addr_(""),
        connected_(false) {}

  TcpConnection(asio::io_context& io_ctx, asio::ssl::context& ssl_ctx)
      : socket_(asio::ssl::stream<asio::ip::tcp::socket>(io_ctx, ssl_ctx)),
        strand_(asio::make_strand(io_ctx)),
        peer_id_(-1),
        addr_(""),
        connected_(false) {}

  asio::ip::tcp::socket& TcpSocket() {
    return std::get<0>(socket_);
  }

  asio::ssl::stream<asio::ip::tcp::socket>& SslSocket() {
    return std::get<1>(socket_);
  }

  bool IsSsl() const {
    return std::holds_alternative<
        asio::ssl::stream<asio::ip::tcp::socket>>(socket_);
  }

  void Start() {
    LOG_INFO("TcpConnection::Start: peer_id={}, socket_open={}", peer_id_,
             SocketIsOpen());
    connected_.store(true, std::memory_order_release);
    DoReadHeader();
    LOG_INFO("TcpConnection::Start completed: peer_id={}", peer_id_);
  }

  void Close() {
    std::unordered_map<uint64_t, PendingCallback> drained;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      SocketClose();
      connected_.store(false, std::memory_order_release);
      drained = std::move(pending_callbacks_);
      pending_callbacks_.clear();
    }
    for (auto& [id, pending] : drained) {
      if (pending.timer) pending.timer->cancel();
      if (pending.callback) pending.callback("", false, "Connection closed");
    }
  }

  bool IsConnected() const {
    return connected_.load(std::memory_order_acquire) && SocketIsOpen();
  }

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
      // NOTE: Do NOT set connected_ = false here. RPC timeout is an
      // application-layer event, not a transport-layer disconnect.
      // PeerConnection handles connection lifecycle; TcpConnection should
      // only mark itself disconnected on actual socket errors (read/write
      // failure) or explicit Close().
    });

    // Send message (serialized through strand)
    auto write_handler = [self, msg, correlation_id](std::error_code ec,
                                                     std::size_t) {
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
        self->connected_.store(false, std::memory_order_release);
      }
      // On success: do NOT cancel timer -- it covers response wait too
      // Response will be handled by DoRead -> HandleMessage
    };

    if (std::holds_alternative<asio::ip::tcp::socket>(socket_)) {
      asio::async_write(
          std::get<asio::ip::tcp::socket>(socket_), asio::buffer(*msg),
          asio::bind_executor(strand_, write_handler));
    } else {
      asio::async_write(
          std::get<asio::ssl::stream<asio::ip::tcp::socket>>(socket_),
          asio::buffer(*msg),
          asio::bind_executor(strand_, write_handler));
    }
  }

  void SetRequestHandler(RpcRequestHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    request_handler_ = handler;
  }

 private:
  void DoReadHeader() {
    auto self = shared_from_this();
    auto read_handler = [this, self](std::error_code ec, std::size_t bytes) {
      if (!ec) {
        LOG_INFO("Read header: peer_id={}, bytes={}", peer_id_, bytes);
      }
      if (ec) {
        if (ec != asio::error::eof) {
          LOG_ERROR("Read header error: {}", ec.message());
        }
        connected_.store(false, std::memory_order_release);
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
        connected_.store(false, std::memory_order_release);
      }
    };

    if (std::holds_alternative<asio::ip::tcp::socket>(socket_)) {
      asio::async_read(
          std::get<asio::ip::tcp::socket>(socket_), asio::buffer(header_buffer_),
          asio::bind_executor(strand_, read_handler));
    } else {
      asio::async_read(
          std::get<asio::ssl::stream<asio::ip::tcp::socket>>(socket_),
          asio::buffer(header_buffer_),
          asio::bind_executor(strand_, read_handler));
    }
  }

  void DoReadBody(uint32_t /*length*/) {
    auto self = shared_from_this();
    auto read_handler = [this, self](std::error_code ec,
                                     std::size_t /*bytes_transferred*/) {
      if (ec) {
        LOG_ERROR("Read body error: {}", ec.message());
        connected_.store(false, std::memory_order_release);
        return;
      }

      HandleMessage();
      DoReadHeader();
    };

    if (std::holds_alternative<asio::ip::tcp::socket>(socket_)) {
      asio::async_read(
          std::get<asio::ip::tcp::socket>(socket_), asio::buffer(body_buffer_),
          asio::bind_executor(strand_, read_handler));
    } else {
      asio::async_read(
          std::get<asio::ssl::stream<asio::ip::tcp::socket>>(socket_),
          asio::buffer(body_buffer_),
          asio::bind_executor(strand_, read_handler));
    }
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

      auto write_handler = [self, msg](std::error_code ec, std::size_t) {
        if (ec) {
          LOG_ERROR("Write response error: {}", ec.message());
        }
      };

      if (std::holds_alternative<asio::ip::tcp::socket>(socket_)) {
        asio::async_write(
            std::get<asio::ip::tcp::socket>(socket_), asio::buffer(*msg),
            asio::bind_executor(strand_, write_handler));
      } else {
        asio::async_write(
            std::get<asio::ssl::stream<asio::ip::tcp::socket>>(socket_),
            asio::buffer(*msg),
            asio::bind_executor(strand_, write_handler));
      }
    } else {
      // This is a client connection, handle response by correlation_id
      auto callback = ExtractCallbackLocked(body_buffer_);
      if (callback) {
        callback(body_buffer_, true, "");
      }
    }
  }

 private:
  bool SocketIsOpen() const {
    if (std::holds_alternative<asio::ip::tcp::socket>(socket_)) {
      return std::get<asio::ip::tcp::socket>(socket_).is_open();
    }
    return std::get<asio::ssl::stream<asio::ip::tcp::socket>>(socket_)
        .next_layer()
        .is_open();
  }

  void SocketClose() {
    if (std::holds_alternative<asio::ip::tcp::socket>(socket_)) {
      auto& socket = std::get<asio::ip::tcp::socket>(socket_);
      if (socket.is_open()) {
        std::error_code ec;
        socket.close(ec);
      }
    } else {
      auto& socket =
          std::get<asio::ssl::stream<asio::ip::tcp::socket>>(socket_);
      if (socket.next_layer().is_open()) {
        std::error_code ec;
        socket.next_layer().close(ec);
      }
    }
  }

  SocketVariant socket_;
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

// ========== PeerConnection (async connect + pending send queue) ==========
class PeerConnection : public std::enable_shared_from_this<PeerConnection> {
 public:
  enum class State : uint8_t {
    kDisconnected,
    kConnecting,
    kConnected,
    kFailed
  };

  static constexpr std::chrono::milliseconds kConnectTimeout{5000};
  static constexpr size_t kMaxPendingSends = 1000;

  PeerConnection(asio::io_context& io_ctx, NodeId peer_id, NodeAddr addr,
                 asio::ssl::context* ssl_ctx = nullptr)
      : io_ctx_(io_ctx),
        strand_(asio::make_strand(io_ctx)),
        peer_id_(peer_id),
        addr_(std::move(addr)),
        ssl_ctx_(ssl_ctx),
        reconnect_timer_(io_ctx),
        connect_timer_(io_ctx) {
    // Parse and cache host/port
    auto pos = addr_.find(':');
    if (pos != std::string::npos) {
      host_ = addr_.substr(0, pos);
      port_ = static_cast<uint16_t>(std::stoi(addr_.substr(pos + 1)));
    }
  }

  // NOTE: ~PeerConnection() does NOT call Close() because Close() uses
  // shared_from_this() which is unsafe during destruction. Owners must call
  // Close() explicitly before the last shared_ptr is dropped.

  // Initiate async connect (idempotent; CAS-guarded)
  void StartConnecting() {
    State expected = State::kDisconnected;
    if (!state_.compare_exchange_strong(expected, State::kConnecting,
                                        std::memory_order_acq_rel)) {
      expected = State::kFailed;
      if (!state_.compare_exchange_strong(expected, State::kConnecting,
                                          std::memory_order_acq_rel)) {
        return;
      }
    }

    std::shared_ptr<TcpConnection> conn;
    if (ssl_ctx_) {
      conn = std::make_shared<TcpConnection>(io_ctx_, *ssl_ctx_, peer_id_,
                                              addr_);
    } else {
      conn = std::make_shared<TcpConnection>(io_ctx_, peer_id_, addr_);
    }
    conn_ = conn;

    auto resolver = std::make_shared<asio::ip::tcp::resolver>(io_ctx_);

    // Connect timeout: if async_connect doesn't complete in 5s, treat as
    // failure. CRITICAL: Do NOT cancel this timer from resolver/connect
    // callbacks. Let it fire naturally; CAS in OnConnect ensures only the first
    // callback wins.
    connect_timer_.expires_after(kConnectTimeout);
    connect_timer_.async_wait(asio::bind_executor(
        strand_, [self = shared_from_this(), conn](std::error_code ec) {
          if (ec) return;  // Cancelled
          self->OnConnect(asio::error::timed_out, conn);
        }));

    resolver->async_resolve(
        host_, std::to_string(port_),
        asio::bind_executor(
            strand_, [self = shared_from_this(), conn, resolver](
                         std::error_code ec,
                         asio::ip::tcp::resolver::results_type results) {
              if (ec) {
                self->OnConnect(ec, conn);
                return;
              }
              asio::async_connect(
                  conn->TcpSocket(), results,
                  asio::bind_executor(
                      self->strand_,
                      [self, conn](std::error_code ec,
                                   asio::ip::tcp::endpoint /*ep*/) {
                        self->OnTcpConnected(ec, conn);
                      }));
            }));
  }

  // Enqueue a send. Dispatches to strand for state-machine consistency.
  void EnqueueSend(std::string request_data, uint64_t correlation_id,
                   RpcResponseCallback callback,
                   std::chrono::milliseconds timeout) {
    asio::post(strand_, [self = shared_from_this(),
                         request_data = std::move(request_data), correlation_id,
                         callback = std::move(callback), timeout]() {
      auto state = self->state_.load(std::memory_order_relaxed);

      if (state == State::kConnected) {
        // Lazy disconnect detection: if TcpConnection dropped, transition to
        // Failed
        if (self->conn_ && !self->conn_->IsConnected()) {
          self->state_.store(State::kFailed, std::memory_order_release);
          self->pending_sends_.push_back({std::move(request_data),
                                          correlation_id, std::move(callback),
                                          timeout});
          self->StartConnecting();
          return;
        }
        self->conn_->Send(request_data, correlation_id, callback, timeout);
        return;
      }

      // Buffer for later flush
      self->pending_sends_.push_back({std::move(request_data), correlation_id,
                                      std::move(callback), timeout});

      // Enforce queue size cap (drop oldest on overflow)
      if (self->pending_sends_.size() >= kMaxPendingSends) {
        auto dropped = std::move(self->pending_sends_.front());
        self->pending_sends_.pop_front();
        if (dropped.callback) {
          dropped.callback("", false, "Send queue full");
        }
      }

      // If we're in a terminal failed/disconnected state, attempt reconnect now
      if (state == State::kFailed || state == State::kDisconnected) {
        self->StartConnecting();
      }
    });
  }

  // Force close + drain callbacks with error (idempotent)
  void Close() {
    asio::post(strand_, [self = shared_from_this()]() {
      if (self->state_.load(std::memory_order_relaxed) ==
          State::kDisconnected) {
        return;  // Idempotent
      }
      self->connect_timer_.cancel();
      self->reconnect_timer_.cancel();
      if (self->conn_) {
        self->conn_->Close();
        self->conn_.reset();
      }
      self->DrainPendingSendsWithError("Transport stopped");
      self->state_.store(State::kDisconnected, std::memory_order_release);
    });
  }

  State GetState() const { return state_.load(std::memory_order_acquire); }

 private:
  void OnTcpConnected(std::error_code ec,
                      std::shared_ptr<TcpConnection> expected_conn) {
    if (conn_ != expected_conn) {
      if (expected_conn) expected_conn->Close();
      return;
    }

    if (ec) {
      OnConnectFailure(ec);
      return;
    }

    if (ssl_ctx_) {
      // Perform TLS handshake
      auto& ssl_socket = conn_->SslSocket();
      ssl_socket.async_handshake(
          asio::ssl::stream_base::client,
          asio::bind_executor(
              strand_, [self = shared_from_this(), expected_conn](
                           std::error_code handshake_ec) {
                self->OnConnect(handshake_ec, expected_conn);
              }));
    } else {
      OnConnect(std::error_code{}, expected_conn);
    }
  }

  void OnConnect(std::error_code ec,
                 std::shared_ptr<TcpConnection> expected_conn) {
    // Identity check: stale callback from replaced connection
    if (conn_ != expected_conn) {
      if (expected_conn) expected_conn->Close();
      return;
    }

    if (!ec) {
      // Success path
      LOG_INFO("PeerConnection connected to peer {} at {}", peer_id_, addr_);

      // Cancel connect timer so it doesn't fire later and cause spurious
      // timeout callbacks.
      connect_timer_.cancel();

      State expected = State::kConnecting;
      if (!state_.compare_exchange_strong(expected, State::kConnected,
                                          std::memory_order_acq_rel)) {
        // Timeout or another error raced ahead and already transitioned to
        // kFailed. The socket is connected but we lost the race. Close it.
        if (conn_) conn_->Close();
        return;
      }

      // Reset backoff on successful connection
      reconnect_delay_ = std::chrono::milliseconds(100);
      reconnect_timer_.cancel();

      conn_->Start();  // Begin async_read loop
      FlushPendingSends();
      return;
    }

    OnConnectFailure(ec);
  }

  void OnConnectFailure(std::error_code ec) {
    // Error path (includes timeout and operation_aborted from Close())
    State expected = State::kConnecting;
    if (!state_.compare_exchange_strong(expected, State::kFailed,
                                        std::memory_order_acq_rel)) {
      return;  // Already handled by another callback path
    }

    if (conn_) conn_->Close();  // Ensure socket closed, inflight ops cancelled
    LOG_WARN("Failed to connect to {}: {}", addr_, ec.message());
    DrainPendingSendsWithError("Connection failed: " + ec.message());
    StartBackoffReconnect();
  }

  void FlushPendingSends() {
    auto sends = std::move(pending_sends_);
    for (auto& s : sends) {
      if (conn_) {
        conn_->Send(s.request_data, s.correlation_id, s.callback, s.timeout);
      }
    }
  }

  void DrainPendingSendsWithError(const std::string& error_msg) {
    auto sends = std::move(pending_sends_);
    for (auto& s : sends) {
      if (s.callback) s.callback("", false, error_msg);
    }
  }

  void StartBackoffReconnect() {
    reconnect_timer_.expires_after(reconnect_delay_);
    reconnect_timer_.async_wait(asio::bind_executor(
        strand_, [self = shared_from_this()](std::error_code ec) {
          if (ec) return;  // Cancelled
          if (self->state_.load(std::memory_order_acquire) == State::kFailed) {
            self->StartConnecting();
          }
        }));

    reconnect_delay_ =
        std::min(reconnect_delay_ * 2, std::chrono::milliseconds(5000));
  }

  struct PendingSend {
    std::string request_data;
    uint64_t correlation_id;
    RpcResponseCallback callback;
    std::chrono::milliseconds timeout;
  };

  asio::io_context& io_ctx_;
  asio::strand<asio::io_context::executor_type> strand_;
  NodeId peer_id_;
  NodeAddr addr_;
  std::string host_;
  uint16_t port_ = 0;
  std::atomic<State> state_{State::kDisconnected};

  asio::ssl::context* ssl_ctx_ = nullptr;
  std::shared_ptr<TcpConnection> conn_;
  std::deque<PendingSend> pending_sends_;  // accessed only on strand_

  std::chrono::milliseconds reconnect_delay_{100};
  asio::steady_timer reconnect_timer_;
  asio::steady_timer connect_timer_;
};

class AsioNetworkTransport : public NetworkTransport {
 public:
  AsioNetworkTransport() = default;

  explicit AsioNetworkTransport(const TlsConfig& tls_config)
      : tls_config_(tls_config) {}

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

    // Initialize TLS contexts if enabled
    if (tls_config_.enabled) {
      AsioSslContextFactory factory(tls_config_);
      auto status = factory.CreateServerContext(server_ssl_context_);
      if (!status.ok()) {
        return Status::Error("Failed to create TLS server context: " +
                             status.GetMessage());
      }
      status = factory.CreateClientContext(client_ssl_context_);
      if (!status.ok()) {
        return Status::Error("Failed to create TLS client context: " +
                             status.GetMessage());
      }
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
    io_threads_exited_.store(0, std::memory_order_relaxed);

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
        io_threads_exited_.fetch_add(1, std::memory_order_release);
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

      // Close all peer connections
      for (auto& [id, peer] : peers_) {
        peer->Close();
      }
      peers_.clear();

      // Stop io_context
      work_guard_.reset();
      io_context_.stop();
    }

    // Wake up kqueue-blocked threads on macOS.
    try {
      asio::post(io_context_, []() {});
    } catch (const std::exception& e) {
      LOG_WARN("AsioNetworkTransport no-op post failed: {}", e.what());
    }

    // Wait for all worker threads to exit run() with timeout.
    bool all_exited = false;
    for (int i = 0; i < 200; ++i) {
      auto exited = io_threads_exited_.load(std::memory_order_acquire);
      if (exited == io_threads_.size()) {
        all_exited = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    for (auto& t : io_threads_) {
      if (t.joinable()) {
        if (all_exited) {
          t.join();
        } else {
          t.detach();
        }
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
      auto peer = GetOrCreatePeerConnection(to, addr);
      peer->EnqueueSend(request_data, correlation_id, callback, timeout);
    });
  }

 private:
  void DoAccept() {
    std::shared_ptr<TcpConnection> new_conn;
    if (tls_config_.enabled) {
      new_conn = std::make_shared<TcpConnection>(io_context_,
                                                  server_ssl_context_);
    } else {
      new_conn = std::make_shared<TcpConnection>(io_context_);
    }

    acceptor_->async_accept(new_conn->TcpSocket(), [this,
                                                    new_conn](
                                                       std::error_code ec) {
      if (!ec) {
        if (tls_config_.enabled) {
          auto& ssl_socket = new_conn->SslSocket();
          ssl_socket.async_handshake(
              asio::ssl::stream_base::server,
              [this, new_conn](std::error_code handshake_ec) {
                if (!handshake_ec) {
                  new_conn->SetRequestHandler(request_handler_);
                  new_conn->Start();
                  LOG_INFO("Accepted inbound TLS connection");
                } else {
                  LOG_ERROR("TLS handshake failed: {}",
                            handshake_ec.message());
                  new_conn->Close();
                }
                if (running_.load(std::memory_order_relaxed)) {
                  DoAccept();
                }
              });
          // Don't call DoAccept here; it's in the handshake callback
          return;
        } else {
          new_conn->SetRequestHandler(request_handler_);
          new_conn->Start();
          LOG_INFO("Accepted inbound connection from {}",
                   new_conn->TcpSocket()
                       .remote_endpoint()
                       .address()
                       .to_string());
        }
      } else if (running_.load(std::memory_order_relaxed)) {
        LOG_ERROR("Accept error: {}", ec.message());
      }

      if (running_.load(std::memory_order_relaxed)) {
        DoAccept();
      }
    });
  }

  std::shared_ptr<PeerConnection> GetOrCreatePeerConnection(
      NodeId peer_id, const NodeAddr& addr) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = peers_.find(peer_id);
    if (it != peers_.end()) {
      auto peer = it->second;
      auto state = peer->GetState();
      if (state == PeerConnection::State::kConnected ||
          state == PeerConnection::State::kConnecting) {
        return peer;
      }
      // Failed or unexpected state: close old peer and reconnect
      peer->Close();
      peers_.erase(it);
    }

    asio::ssl::context* ssl_ctx =
        tls_config_.enabled ? &client_ssl_context_ : nullptr;
    auto peer = std::make_shared<PeerConnection>(io_context_, peer_id, addr,
                                                  ssl_ctx);
    peers_[peer_id] = peer;
    peer->StartConnecting();
    return peer;
  }

 private:
  mutable std::mutex mutex_;
  bool initialized_ = false;
  std::atomic<bool> running_{false};
  std::atomic<size_t> io_threads_exited_{0};

  asio::io_context io_context_;
  std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>>
      work_guard_;
  std::vector<std::thread> io_threads_;

  NodeAddr listen_addr_;
  std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
  RpcRequestHandler request_handler_;
  ConnectionCallback connection_callback_;

  std::unordered_map<NodeId, std::shared_ptr<PeerConnection>> peers_;

  TlsConfig tls_config_;
  asio::ssl::context server_ssl_context_{asio::ssl::context::tls_server};
  asio::ssl::context client_ssl_context_{asio::ssl::context::tls_client};
};

// Factory function
std::unique_ptr<NetworkTransport> CreateAsioNetworkTransport() {
  return std::make_unique<AsioNetworkTransport>();
}

std::unique_ptr<NetworkTransport> CreateAsioNetworkTransport(
    const TlsConfig& tls_config) {
  return std::make_unique<AsioNetworkTransport>(tls_config);
}

// Default factory function (used by RaftNode)
std::unique_ptr<NetworkTransport> CreateDefaultNetworkTransport() {
  return std::make_unique<AsioNetworkTransport>();
}


}  // namespace rollingraft
