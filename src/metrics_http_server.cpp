#include "metrics_http_server.h"

#include <asio.hpp>
#include <sstream>
#include <type_traits>

#include "nlohmann/json.hpp"
#include "rollingraft/logger.h"
#include "rollingraft/metrics.h"
#include "sse_connection.h"

namespace rollingraft {

namespace {

std::shared_ptr<asio::ssl::context> CreateSslContext(
    const MetricsHttpServer::TlsConfig& config) {
  auto ctx = std::make_shared<asio::ssl::context>(asio::ssl::context::tls_server);

  try {
    ctx->use_certificate_chain_file(config.cert_file);
    ctx->use_private_key_file(config.key_file, asio::ssl::context::pem);

    if (!config.ca_file.empty()) {
      ctx->load_verify_file(config.ca_file);
      ctx->set_verify_mode(asio::ssl::verify_peer |
                           asio::ssl::verify_fail_if_no_peer_cert);
    } else {
      ctx->set_verify_mode(asio::ssl::verify_none);
    }
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to create SSL context: {}", e.what());
    return nullptr;
  }

  return ctx;
}

std::string ExtractPath(const std::string& request) {
  size_t start = request.find(' ');
  if (start == std::string::npos) return "";
  ++start;
  size_t end = request.find(' ', start);
  if (end == std::string::npos) return "";
  return request.substr(start, end - start);
}

std::string ExtractBody(const std::string& request) {
  size_t pos = request.find("\r\n\r\n");
  if (pos == std::string::npos) return "";
  return request.substr(pos + 4);
}

std::string ExtractAuthToken(const std::string& request) {
  static constexpr std::string_view kAuthPrefix = "Authorization: Bearer ";
  size_t pos = request.find(kAuthPrefix);
  if (pos == std::string::npos) return "";
  pos += kAuthPrefix.size();
  size_t end = request.find("\r\n", pos);
  if (end == std::string::npos) return "";
  return request.substr(pos, end - pos);
}

// Timing-safe string comparison to mitigate timing attacks on token validation.
// Never use operator== for comparing secrets.
bool TimingSafeEqual(const std::string& a, const std::string& b) {
  if (a.size() != b.size()) return false;
  volatile unsigned char diff = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    diff |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
  }
  return diff == 0;
}

bool IsAdminEndpoint(const std::string& path, const std::string& method) {
  if (path == "/v1/members" && method == "POST") return true;
  if (path.find("/v1/members/") == 0 && method == "DELETE") return true;
  if (path == "/v1/snapshot/trigger" && method == "POST") return true;
  if (path == "/v1/leadership/transfer" && method == "POST") return true;
  if (path == "/v1/config") return true;  // GET and PATCH
  return false;
}

}  // namespace

MetricsHttpServer::MetricsHttpServer(const std::string& bind_addr,
                                     MetricsRegistry* registry,
                                     const TlsConfig& tls_config,
                                     const std::string& admin_token)
    : bind_addr_(bind_addr), registry_(registry), tls_config_(tls_config),
      admin_token_(admin_token) {
  if (tls_config_.enabled) {
    ssl_ctx_ = CreateSslContext(tls_config_);
  }
}

MetricsHttpServer::~MetricsHttpServer() { Stop(); }

void MetricsHttpServer::Start() {
  if (running_.exchange(true)) return;

  io_ctx_ = std::make_unique<asio::io_context>(1);

  std::string host = "0.0.0.0";
  uint16_t port = 0;
  auto colon = bind_addr_.find(':');
  if (colon != std::string::npos) {
    host = bind_addr_.substr(0, colon);
    port = static_cast<uint16_t>(std::stoi(bind_addr_.substr(colon + 1)));
  } else {
    port = static_cast<uint16_t>(std::stoi(bind_addr_));
  }

  acceptor_ = std::make_unique<asio::ip::tcp::acceptor>(*io_ctx_);
  asio::ip::tcp::endpoint endpoint(asio::ip::make_address(host), port);
  acceptor_->open(endpoint.protocol());
  acceptor_->set_option(asio::ip::tcp::acceptor::reuse_address(true));
  acceptor_->bind(endpoint);
  acceptor_->listen();

  LOG_INFO("Agent HTTP server listening on {}", bind_addr_);
  DoAccept();
  thread_ = std::thread([this]() { Run(); });
}

void MetricsHttpServer::Stop() {
  if (!running_.exchange(false)) return;

  {
    std::lock_guard<std::mutex> lock(sse_mutex_);
    for (auto& weak_conn : sse_connections_) {
      if (auto conn = weak_conn.lock()) {
        conn->Close();
      }
    }
    sse_connections_.clear();
  }

  if (acceptor_ && acceptor_->is_open()) {
    std::error_code ec;
    acceptor_->close(ec);
  }
  if (io_ctx_) io_ctx_->stop();
  if (thread_.joinable()) thread_.join();
}

void MetricsHttpServer::Run() { io_ctx_->run(); }

void MetricsHttpServer::DoAccept() {
  if (!running_) return;

  if (tls_config_.enabled && ssl_ctx_) {
    acceptor_->async_accept(
        [this](std::error_code ec, asio::ip::tcp::socket socket) {
          if (!ec && running_) {
            auto ssl_socket =
                std::make_shared<asio::ssl::stream<asio::ip::tcp::socket>>(
                    std::move(socket), *ssl_ctx_);
            ssl_socket->async_handshake(
                asio::ssl::stream_base::server,
                [this, ssl_socket](std::error_code handshake_ec) {
                  if (!handshake_ec) {
                    HandleConnection(std::move(*ssl_socket));
                  } else {
                    LOG_WARN("TLS handshake failed: {}",
                             handshake_ec.message());
                    std::error_code close_ec;
                    ssl_socket->next_layer().close(close_ec);
                  }
                });
          }
          if (running_) DoAccept();
        });
  } else {
    acceptor_->async_accept(
        [this](std::error_code ec, asio::ip::tcp::socket socket) {
          if (!ec && running_) {
            HandleConnection(std::move(socket));
          }
          if (running_) DoAccept();
        });
  }
}

void MetricsHttpServer::SetStatusProvider(StatusProvider provider) {
  status_provider_ = std::move(provider);
}
void MetricsHttpServer::SetAddMemberHandler(AddMemberHandler handler) {
  add_member_handler_ = std::move(handler);
}
void MetricsHttpServer::SetRemoveMemberHandler(RemoveMemberHandler handler) {
  remove_member_handler_ = std::move(handler);
}
void MetricsHttpServer::SetTriggerSnapshotHandler(
    TriggerSnapshotHandler handler) {
  trigger_snapshot_handler_ = std::move(handler);
}
void MetricsHttpServer::SetTransferLeadershipHandler(
    TransferLeadershipHandler handler) {
  transfer_leadership_handler_ = std::move(handler);
}
void MetricsHttpServer::SetConfigProvider(ConfigProvider provider) {
  config_provider_ = std::move(provider);
}
void MetricsHttpServer::SetConfigUpdater(ConfigUpdater handler) {
  config_updater_ = std::move(handler);
}

void MetricsHttpServer::BroadcastEvent(const std::string& json_event) {
  if (!io_ctx_ || !running_) return;

  std::string sse_data = "data: " + json_event + "\n\n";

  std::vector<std::shared_ptr<SseConnection>> connections;
  {
    std::lock_guard<std::mutex> lock(sse_mutex_);
    RemoveDeadSseConnections();
    for (auto& weak_conn : sse_connections_) {
      if (auto conn = weak_conn.lock()) {
        connections.push_back(conn);
      }
    }
  }

  for (auto& conn : connections) {
    conn->EnqueueEvent(sse_data);
  }
}

void MetricsHttpServer::RemoveDeadSseConnections() {
  sse_connections_.erase(
      std::remove_if(sse_connections_.begin(), sse_connections_.end(),
                     [](const auto& w) { return w.expired(); }),
      sse_connections_.end());
}

std::string ExtractMethod(const std::string& request) {
  size_t end = request.find(' ');
  if (end == std::string::npos) return "";
  return request.substr(0, end);
}

std::tuple<std::string, std::string, std::string, bool>
MetricsHttpServer::BuildResponse(const std::string& request) {
  std::string path = ExtractPath(request);
  std::string body = ExtractBody(request);
  std::string method = ExtractMethod(request);

  std::string response_body;
  std::string status_line = "HTTP/1.1 404 Not Found\r\n";
  std::string content_type = "application/json\r\n";
  bool is_sse = false;

  // Admin endpoint authentication check
  if (!admin_token_.empty() && IsAdminEndpoint(path, method)) {
    std::string token = ExtractAuthToken(request);
    if (!TimingSafeEqual(token, admin_token_)) {
      response_body = nlohmann::json{{"error", "UNAUTHORIZED"},
                                      {"message", "Invalid or missing admin token"}}
                          .dump() +
                      "\n";
      status_line = "HTTP/1.1 401 Unauthorized\r\n";
      return {response_body, status_line, content_type, is_sse};
    }
  }

  if (path == "/metrics" && registry_) {
    response_body = registry_->FormatPrometheus();
    status_line = "HTTP/1.1 200 OK\r\n";
    content_type = "text/plain; version=0.0.4\r\n";
  } else if (path == "/healthz" || path == "/livez") {
    response_body = "{\"status\":\"alive\"}\n";
    status_line = "HTTP/1.1 200 OK\r\n";
  } else if (path == "/readyz") {
    if (status_provider_) {
      try {
        auto j = nlohmann::json::parse(status_provider_());
        bool ready = false;
        if (j.contains("role") && j["role"] == "Leader") {
          ready = true;
        } else if (j.contains("leader_id") && !j["leader_id"].is_null() &&
                   j["leader_id"] != -1) {
          ready = true;
        }
        if (ready) {
          response_body = "{\"status\":\"ready\"}\n";
        } else {
          response_body = "{\"status\":\"not_ready\"}\n";
          status_line = "HTTP/1.1 503 Service Unavailable\r\n";
        }
      } catch (const std::exception& e) {
        response_body = nlohmann::json{{"status", "error"},
                                        {"message", e.what()}}
                            .dump() +
                        "\n";
        status_line = "HTTP/1.1 500 Internal Server Error\r\n";
      }
    } else {
      response_body = "{\"status\":\"unknown\"}\n";
      status_line = "HTTP/1.1 200 OK\r\n";
    }
  } else if (path == "/v1/status") {
    if (status_provider_) {
      response_body = status_provider_();
    } else {
      response_body = "{\"error\":\"status_provider_not_set\"}\n";
    }
    status_line = "HTTP/1.1 200 OK\r\n";
  } else if (path == "/v1/members" && request.find("POST") == 0 &&
             add_member_handler_) {
    int32_t node_id = -1;
    std::string addr;
    try {
      auto j = nlohmann::json::parse(body);
      if (j.contains("node_id")) node_id = j["node_id"];
      if (j.contains("addr")) addr = j["addr"];
    } catch (const std::exception& e) {
      response_body = nlohmann::json{{"error", "BAD_REQUEST"},
                                      {"message", e.what()}}
                          .dump() +
                      "\n";
      status_line = "HTTP/1.1 400 Bad Request\r\n";
    }
    if (status_line == "HTTP/1.1 404 Not Found\r\n") {
      response_body = add_member_handler_(node_id, addr);
      status_line = "HTTP/1.1 202 Accepted\r\n";
    }
  } else if (path.find("/v1/members/") == 0 && request.find("DELETE") == 0 &&
             remove_member_handler_) {
    int32_t node_id = -1;
    size_t last_slash = path.find_last_of('/');
    if (last_slash != std::string::npos && last_slash + 1 < path.size()) {
      node_id = std::stoi(path.substr(last_slash + 1));
    }
    response_body = remove_member_handler_(node_id);
    status_line = "HTTP/1.1 202 Accepted\r\n";
  } else if (path == "/v1/snapshot/trigger" && request.find("POST") == 0 &&
             trigger_snapshot_handler_) {
    response_body = trigger_snapshot_handler_();
    status_line = "HTTP/1.1 202 Accepted\r\n";
  } else if (path == "/v1/leadership/transfer" &&
             request.find("POST") == 0 && transfer_leadership_handler_) {
    int32_t target_id = -1;
    try {
      auto j = nlohmann::json::parse(body);
      if (j.contains("target_node_id")) target_id = j["target_node_id"];
    } catch (const std::exception& e) {
      response_body = nlohmann::json{{"error", "BAD_REQUEST"},
                                      {"message", e.what()}}
                          .dump() +
                      "\n";
      status_line = "HTTP/1.1 400 Bad Request\r\n";
    }
    if (status_line == "HTTP/1.1 404 Not Found\r\n") {
      response_body = transfer_leadership_handler_(target_id);
      status_line = "HTTP/1.1 202 Accepted\r\n";
    }
  } else if (path == "/v1/config" && request.find("GET") == 0 &&
             config_provider_) {
    response_body = config_provider_();
    status_line = "HTTP/1.1 200 OK\r\n";
  } else if (path == "/v1/config" && request.find("PATCH") == 0 &&
             config_updater_) {
    response_body = config_updater_(body);
    status_line = "HTTP/1.1 200 OK\r\n";
  } else if (path == "/v1/events") {
    is_sse = true;
  }

  return {response_body, status_line, content_type, is_sse};
}

void MetricsHttpServer::HandleConnection(SocketVariant socket) {
  auto buffer = std::make_shared<std::array<char, 4096>>();
  std::visit(
      [this, buffer](auto&& socket_ref) mutable {
        using SocketType = std::decay_t<decltype(socket_ref)>;
        auto socket_ptr = std::make_shared<SocketType>(std::move(socket_ref));

        socket_ptr->async_read_some(
            asio::buffer(*buffer),
            [this, buffer, socket_ptr](std::error_code ec,
                                       std::size_t bytes) mutable {
              if (ec) return;

              std::string request(buffer->data(), bytes);
              auto [response_body, status_line, content_type, is_sse] =
                  BuildResponse(request);

              if (is_sse) {
                auto conn = std::make_shared<SseConnection>(
                    std::move(*socket_ptr),
                    asio::io_context::strand(*io_ctx_));
                {
                  std::lock_guard<std::mutex> lock(sse_mutex_);
                  RemoveDeadSseConnections();
                  sse_connections_.push_back(conn);
                }
                conn->Start();
                return;
              }

              std::ostringstream response;
              response << status_line;
              response << "Content-Type: " << content_type;
              response << "Content-Length: " << response_body.size()
                       << "\r\n";
              response << "Connection: close\r\n";
              response << "\r\n";
              response << response_body;

              auto resp_str = std::make_shared<std::string>(response.str());
              asio::async_write(
                  *socket_ptr, asio::buffer(*resp_str),
                  [resp_str, socket_ptr](std::error_code,
                                         std::size_t) {
                    std::error_code close_ec;
                    if constexpr (std::is_same_v<
                                      SocketType,
                                      asio::ssl::stream<
                                          asio::ip::tcp::socket>>) {
                      socket_ptr->next_layer().shutdown(
                          asio::ip::tcp::socket::shutdown_both, close_ec);
                      socket_ptr->next_layer().close(close_ec);
                    } else {
                      socket_ptr->shutdown(
                          asio::ip::tcp::socket::shutdown_both, close_ec);
                      socket_ptr->close(close_ec);
                    }
                  });
            });
      },
      std::move(socket));
}

}  // namespace rollingraft
