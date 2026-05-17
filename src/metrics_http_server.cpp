#include "metrics_http_server.h"

#include <asio.hpp>
#include <sstream>

#include "rollingraft/logger.h"
#include "rollingraft/metrics.h"

namespace rollingraft {

MetricsHttpServer::MetricsHttpServer(const std::string& bind_addr,
                                     MetricsRegistry* registry)
    : bind_addr_(bind_addr), registry_(registry) {}

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
  if (acceptor_ && acceptor_->is_open()) {
    std::error_code ec;
    acceptor_->close(ec);
  }
  if (io_ctx_) io_ctx_->stop();
  if (thread_.joinable()) thread_.join();
}

void MetricsHttpServer::Run() { io_ctx_->run(); }

void MetricsHttpServer::SetStatusProvider(StatusProvider provider) {
  status_provider_ = std::move(provider);
}
void MetricsHttpServer::SetAddMemberHandler(AddMemberHandler handler) {
  add_member_handler_ = std::move(handler);
}
void MetricsHttpServer::SetRemoveMemberHandler(RemoveMemberHandler handler) {
  remove_member_handler_ = std::move(handler);
}
void MetricsHttpServer::SetTriggerSnapshotHandler(TriggerSnapshotHandler handler) {
  trigger_snapshot_handler_ = std::move(handler);
}
void MetricsHttpServer::SetTransferLeadershipHandler(TransferLeadershipHandler handler) {
  transfer_leadership_handler_ = std::move(handler);
}

// Simple URL path extractor
static std::string ExtractPath(const std::string& request) {
  size_t start = request.find(' ');
  if (start == std::string::npos) return "";
  ++start;
  size_t end = request.find(' ', start);
  if (end == std::string::npos) return "";
  return request.substr(start, end - start);
}

// Simple body extractor (after \r\n\r\n)
static std::string ExtractBody(const std::string& request) {
  size_t pos = request.find("\r\n\r\n");
  if (pos == std::string::npos) return "";
  return request.substr(pos + 4);
}

void MetricsHttpServer::HandleRequest(asio::ip::tcp::socket socket) {
  auto buffer = std::make_shared<std::array<char, 4096>>();
  auto socket_ptr = std::make_shared<asio::ip::tcp::socket>(std::move(socket));
  socket_ptr->async_read_some(
      asio::buffer(*buffer),
      [this, buffer, socket_ptr](std::error_code ec, std::size_t bytes) mutable {
        if (ec) return;

        std::string request(buffer->data(), bytes);
        std::string path = ExtractPath(request);
        std::string body = ExtractBody(request);

        std::string response_body;
        std::string status_line = "HTTP/1.1 404 Not Found\r\n";
        std::string content_type = "application/json\r\n";

        if (path == "/metrics" && registry_) {
          response_body = registry_->FormatPrometheus();
          status_line = "HTTP/1.1 200 OK\r\n";
          content_type = "text/plain; version=0.0.4\r\n";
        } else if (path == "/healthz" || path == "/livez") {
          response_body = "{\"status\":\"alive\"}\n";
          status_line = "HTTP/1.1 200 OK\r\n";
        } else if (path == "/readyz") {
          if (status_provider_) {
            std::string s = status_provider_();
            bool has_leader = s.find("\"leader_id\"") != std::string::npos &&
                              s.find("\"leader_id\":-1") == std::string::npos;
            bool is_leader = s.find("\"role\":\"Leader\"") != std::string::npos;
            if (is_leader || has_leader) {
              response_body = "{\"status\":\"ready\"}\n";
            } else {
              response_body = "{\"status\":\"not_ready\"}\n";
              status_line = "HTTP/1.1 503 Service Unavailable\r\n";
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
        } else if (path == "/v1/members" && request.find("POST") == 0 && add_member_handler_) {
          // Very simple JSON parse: {"node_id":N,"addr":"A"}
          int32_t node_id = -1;
          std::string addr;
          size_t id_pos = body.find("\"node_id\"");
          if (id_pos != std::string::npos) {
            size_t num_start = body.find_first_of("0123456789-", id_pos + 9);
            if (num_start != std::string::npos) {
              node_id = std::stoi(body.substr(num_start));
            }
          }
          size_t addr_pos = body.find("\"addr\"");
          if (addr_pos != std::string::npos) {
            size_t q1 = body.find('"', addr_pos + 6);
            if (q1 != std::string::npos) {
              size_t q2 = body.find('"', q1 + 1);
              if (q2 != std::string::npos) addr = body.substr(q1 + 1, q2 - q1 - 1);
            }
          }
          response_body = add_member_handler_(node_id, addr);
          status_line = "HTTP/1.1 202 Accepted\r\n";
        } else if (path.find("/v1/members/") == 0 && request.find("DELETE") == 0 && remove_member_handler_) {
          int32_t node_id = -1;
          size_t last_slash = path.find_last_of('/');
          if (last_slash != std::string::npos && last_slash + 1 < path.size()) {
            node_id = std::stoi(path.substr(last_slash + 1));
          }
          response_body = remove_member_handler_(node_id);
          status_line = "HTTP/1.1 202 Accepted\r\n";
        } else if (path == "/v1/snapshot/trigger" && request.find("POST") == 0 && trigger_snapshot_handler_) {
          response_body = trigger_snapshot_handler_();
          status_line = "HTTP/1.1 202 Accepted\r\n";
        } else if (path == "/v1/leadership/transfer" && request.find("POST") == 0 && transfer_leadership_handler_) {
          int32_t target_id = -1;
          size_t id_pos = body.find("\"target_node_id\"");
          if (id_pos != std::string::npos) {
            size_t num_start = body.find_first_of("0123456789-", id_pos + 16);
            if (num_start != std::string::npos) {
              target_id = std::stoi(body.substr(num_start));
            }
          }
          response_body = transfer_leadership_handler_(target_id);
          status_line = "HTTP/1.1 202 Accepted\r\n";
        } else if (path == "/v1/events") {
          // SSE endpoint placeholder
          response_body = "event: connected\ndata: {}\n\n";
          status_line = "HTTP/1.1 200 OK\r\n";
          content_type = "text/event-stream\r\n";
        }

        std::ostringstream response;
        response << status_line;
        response << "Content-Type: " << content_type;
        response << "Content-Length: " << response_body.size() << "\r\n";
        response << "Connection: close\r\n";
        response << "\r\n";
        response << response_body;

        auto resp_str = std::make_shared<std::string>(response.str());
        asio::async_write(*socket_ptr, asio::buffer(*resp_str),
                          [resp_str, socket_ptr](std::error_code, std::size_t) {
                            std::error_code close_ec;
                            socket_ptr->shutdown(asio::ip::tcp::socket::shutdown_both, close_ec);
                            socket_ptr->close(close_ec);
                          });
      });
}

}  // namespace rollingraft
