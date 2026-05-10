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
  if (running_.exchange(true)) {
    return;
  }

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

  LOG_INFO("Metrics HTTP server listening on {}", bind_addr_);

  DoAccept();
  thread_ = std::thread([this]() { Run(); });
}

void MetricsHttpServer::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  if (acceptor_ && acceptor_->is_open()) {
    std::error_code ec;
    acceptor_->close(ec);
  }

  if (io_ctx_) {
    io_ctx_->stop();
  }

  if (thread_.joinable()) {
    thread_.join();
  }
}

void MetricsHttpServer::Run() { io_ctx_->run(); }

void MetricsHttpServer::DoAccept() {
  if (!running_) return;

  acceptor_->async_accept(
      [this](std::error_code ec, asio::ip::tcp::socket socket) {
        if (!ec && running_) {
          HandleRequest(std::move(socket));
          DoAccept();
        }
      });
}

void MetricsHttpServer::HandleRequest(asio::ip::tcp::socket socket) {
  auto buffer = std::make_shared<std::array<char, 4096>>();
  auto socket_ptr = std::make_shared<asio::ip::tcp::socket>(std::move(socket));
  socket_ptr->async_read_some(
      asio::buffer(*buffer),
      [this, buffer, socket_ptr](std::error_code ec,
                                 std::size_t bytes) mutable {
        if (ec) return;

        std::string request(buffer->data(), bytes);

        std::string response_body;
        std::string status_line;

        if (request.find("GET /metrics") != std::string::npos && registry_) {
          response_body = registry_->FormatPrometheus();
          status_line = "HTTP/1.1 200 OK\r\n";
        } else {
          status_line = "HTTP/1.1 404 Not Found\r\n";
          response_body = "Not Found\n";
        }

        std::ostringstream response;
        response << status_line;
        response << "Content-Type: text/plain; version=0.0.4\r\n";
        response << "Content-Length: " << response_body.size() << "\r\n";
        response << "Connection: close\r\n";
        response << "\r\n";
        response << response_body;

        auto resp_str = std::make_shared<std::string>(response.str());
        asio::async_write(*socket_ptr, asio::buffer(*resp_str),
                          [resp_str, socket_ptr](std::error_code, std::size_t) {
                            std::error_code close_ec;
                            socket_ptr->shutdown(
                                asio::ip::tcp::socket::shutdown_both, close_ec);
                            socket_ptr->close(close_ec);
                          });
      });
}

}  // namespace rollingraft
