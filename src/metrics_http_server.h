#pragma once

#include <asio.hpp>
#include <atomic>
#include <memory>
#include <string>
#include <thread>

namespace rollingraft {

class MetricsRegistry;

/**
 * Minimal Asio-based HTTP server that serves GET /metrics
 * in Prometheus text format.
 */
class MetricsHttpServer {
 public:
  MetricsHttpServer(const std::string& bind_addr,
                    MetricsRegistry* registry);
  ~MetricsHttpServer();

  void Start();
  void Stop();

 private:
  void Run();
  void DoAccept();
  void HandleRequest(asio::ip::tcp::socket socket);

  std::string bind_addr_;
  MetricsRegistry* registry_;

  std::unique_ptr<asio::io_context> io_ctx_;
  std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
  std::thread thread_;
  std::atomic<bool> running_{false};
};

}  // namespace rollingraft
