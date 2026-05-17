#pragma once

#include <asio.hpp>
#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace rollingraft {

class MetricsRegistry;

/**
 * Minimal Asio-based HTTP server that serves agent-friendly endpoints:
 * - GET /metrics    — Prometheus text format
 * - GET /healthz    — Liveness probe (process alive)
 * - GET /readyz     — Readiness probe (node can serve requests)
 * - GET /livez      — Same as /healthz, Kubernetes convention
 * - GET /v1/status  — JSON node status (role, term, leader, config)
 */
class MetricsHttpServer {
 public:
  /**
   * Status callback for /v1/status and /readyz endpoints.
   * Returns JSON string representing current node status.
   */
  using StatusProvider = std::function<std::string()>;

  MetricsHttpServer(const std::string& bind_addr, MetricsRegistry* registry);
  ~MetricsHttpServer();

  void Start();
  void Stop();

  /** Set a callback that provides JSON status for /v1/status and /readyz. */
  void SetStatusProvider(StatusProvider provider);

 private:
  void Run();
  void DoAccept();
  void HandleRequest(asio::ip::tcp::socket socket);

  std::string bind_addr_;
  MetricsRegistry* registry_;
  StatusProvider status_provider_;

  std::unique_ptr<asio::io_context> io_ctx_;
  std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
  std::thread thread_;
  std::atomic<bool> running_{false};
};

}  // namespace rollingraft
