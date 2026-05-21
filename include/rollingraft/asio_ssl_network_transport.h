#pragma once

#include "rollingraft/network_transport.h"
#include "rollingraft/tls_config.h"

namespace rollingraft {

class AsioSslNetworkTransport : public NetworkTransport {
 public:
  explicit AsioSslNetworkTransport(const TlsConfig& tls_config);
  ~AsioSslNetworkTransport() override;

  Status Initialize(const NodeAddr& listen_addr,
                    RpcRequestHandler handler) override;
  void SetConnectionCallback(ConnectionCallback callback) override;
  Status Start() override;
  Status Stop() override;
  void SendRpc(NodeId to, const NodeAddr& addr,
               const std::string& request_data, uint64_t correlation_id,
               std::chrono::milliseconds timeout,
               RpcResponseCallback callback) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rollingraft
