#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>

#include <asio.hpp>

#include <asio/ssl.hpp>

namespace rollingraft {

class MetricsRegistry;
class SseConnection;

struct MetricsHttpServerTlsConfig {
  bool enabled = false;
  std::string cert_file;
  std::string key_file;
  std::string ca_file;
};

class MetricsHttpServer {
 public:
  using StatusProvider = std::function<std::string()>;
  // Admin handlers take the target raft group_id (0 for the legacy
  // single-group path). group_id comes from the JSON body ("group_id") for
  // POSTs and from the ?group_id= query parameter for DELETE.
  using AddMemberHandler =
      std::function<std::string(int32_t node_id, const std::string& addr, uint64_t group_id)>;
  using RemoveMemberHandler = std::function<std::string(int32_t node_id, uint64_t group_id)>;
  using TriggerSnapshotHandler = std::function<std::string(uint64_t group_id)>;
  using TransferLeadershipHandler =
      std::function<std::string(int32_t target_node_id, uint64_t group_id)>;
  using ConfigProvider = std::function<std::string(uint64_t group_id)>;
  using ConfigUpdater = std::function<std::string(const std::string& json, uint64_t group_id)>;
  using TlsConfig = MetricsHttpServerTlsConfig;

  MetricsHttpServer(const std::string& bind_addr, MetricsRegistry* registry,
                    const TlsConfig& tls_config = {}, const std::string& admin_token = "");
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

  void BroadcastEvent(const std::string& json_event);

 public:
  std::tuple<std::string, std::string, std::string, bool> BuildResponse(const std::string& request);
  void RemoveDeadSseConnections();

  // Simple per-IP rate limiter: sliding window of 10 requests per second
  bool CheckRateLimit(const std::string& client_ip);
  void CleanupRateLimit();

 private:
  void ScheduleHeartbeat();
  void OnHeartbeat(std::error_code ec);

 private:
  void Run();
  void DoAccept();

  using SocketVariant =
      std::variant<asio::ip::tcp::socket, asio::ssl::stream<asio::ip::tcp::socket>>;
  void HandleConnection(SocketVariant socket);

  std::string bind_addr_;
  MetricsRegistry* registry_;

  TlsConfig tls_config_;
  std::string admin_token_;
  std::shared_ptr<asio::ssl::context> ssl_ctx_;

  std::mutex sse_mutex_;
  // Strong refs: SSE connections must outlive the headers write,
  // otherwise they are destroyed as soon as the write callback
  // releases its shared_from_this and the socket closes.
  std::vector<std::shared_ptr<SseConnection>> sse_connections_;

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

  std::unique_ptr<asio::steady_timer> heartbeat_timer_;
  static constexpr std::chrono::seconds kHeartbeatInterval{15};

  // Rate limiting: per-IP request timestamps (sliding window)
  mutable std::mutex rate_limit_mtx_;
  std::unordered_map<std::string, std::deque<std::chrono::steady_clock::time_point>> rate_limit_;
  static constexpr size_t kMaxRequestsPerSecond = 10;
  static constexpr std::chrono::seconds kRateLimitWindow{1};

  // Authentication logic tested via public HTTP interface in unit tests
};

}  // namespace rollingraft
