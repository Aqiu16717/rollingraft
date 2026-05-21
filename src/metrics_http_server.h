#pragma once

#include <asio.hpp>
#include <asio/ssl.hpp>
#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <variant>

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
  using AddMemberHandler = std::function<std::string(int32_t node_id, const std::string& addr)>;
  using RemoveMemberHandler = std::function<std::string(int32_t node_id)>;
  using TriggerSnapshotHandler = std::function<std::string()>;
  using TransferLeadershipHandler = std::function<std::string(int32_t target_node_id)>;
  using ConfigProvider = std::function<std::string()>;
  using ConfigUpdater = std::function<std::string(const std::string& json)>;
  using TlsConfig = MetricsHttpServerTlsConfig;

  MetricsHttpServer(const std::string& bind_addr, MetricsRegistry* registry,
                    const TlsConfig& tls_config = {});
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

 private:
  void Run();
  void DoAccept();

  using SocketVariant = std::variant<asio::ip::tcp::socket,
                                     asio::ssl::stream<asio::ip::tcp::socket>>;
  void HandleConnection(SocketVariant socket);

  std::tuple<std::string, std::string, std::string, bool> BuildResponse(
      const std::string& request);
  void RemoveDeadSseConnections();

  std::string bind_addr_;
  MetricsRegistry* registry_;

  TlsConfig tls_config_;
  std::shared_ptr<asio::ssl::context> ssl_ctx_;

  std::mutex sse_mutex_;
  std::vector<std::weak_ptr<SseConnection>> sse_connections_;

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
