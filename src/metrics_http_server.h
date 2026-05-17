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
 * Asio-based HTTP server that serves agent-friendly endpoints:
 * - GET /metrics         — Prometheus text format
 * - GET /healthz         — Liveness probe
 * - GET /livez           — Liveness probe (K8s convention)
 * - GET /readyz          — Readiness probe
 * - GET /v1/status       — JSON node status
 * - POST /v1/members     — Add member
 * - DELETE /v1/members/{id} — Remove member
 * - POST /v1/snapshot/trigger — Trigger snapshot
 * - POST /v1/leadership/transfer — Transfer leadership
 * - PATCH /v1/config     — Hot reload config
 * - GET /v1/config       — Get current config
 * - GET /v1/events       — SSE event stream (placeholder)
 */
class MetricsHttpServer {
 public:
  using StatusProvider = std::function<std::string()>;
  using AddMemberHandler = std::function<std::string(int32_t node_id, const std::string& addr)>;
  using RemoveMemberHandler = std::function<std::string(int32_t node_id)>;
  using TriggerSnapshotHandler = std::function<std::string()>;
  using TransferLeadershipHandler = std::function<std::string(int32_t target_node_id)>;
  using ConfigProvider = std::function<std::string()>;
  using ConfigUpdater = std::function<std::string(const std::string& json)>;

  MetricsHttpServer(const std::string& bind_addr, MetricsRegistry* registry);
  ~MetricsHttpServer();

  void Start();
  void Stop();

  void SetStatusProvider(StatusProvider provider);
  void SetAddMemberHandler(AddMemberHandler handler);
  void SetRemoveMemberHandler(RemoveMemberHandler handler);
  void SetTriggerSnapshotHandler(TriggerSnapshotHandler handler);
  void SetTransferLeadershipHandler(TransferLeadershipHandler handler);
  void SetConfigProvider(ConfigProvider provider);
  void SetConfigUpdater(ConfigUpdater handler);

 private:
  void Run();
  void DoAccept();
  void HandleRequest(asio::ip::tcp::socket socket);

  std::string bind_addr_;
  MetricsRegistry* registry_;

  StatusProvider status_provider_;
  AddMemberHandler add_member_handler_;
  RemoveMemberHandler remove_member_handler_;
  TriggerSnapshotHandler trigger_snapshot_handler_;
  TransferLeadershipHandler transfer_leadership_handler_;
  ConfigProvider config_provider_;
  ConfigUpdater config_updater_;

  std::unique_ptr<asio::io_context> io_ctx_;
  std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
  std::thread thread_;
  std::atomic<bool> running_{false};
};

}  // namespace rollingraft
