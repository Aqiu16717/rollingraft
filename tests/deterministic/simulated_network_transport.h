#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "rollingraft/network_transport.h"
#include "rollingraft/types.h"

namespace rollingraft {

class SimulatedClock;
class SimulatedNetwork;

/**
 * Bridge SimulatedNetwork to NetworkTransport interface.
 *
 * Wraps a SimulatedNetwork endpoint so RaftNode can use it
 * as its NetworkTransport dependency.
 */
class SimulatedNetworkTransport : public NetworkTransport {
 public:
  SimulatedNetworkTransport(NodeId node_id, SimulatedNetwork* network,
                            SimulatedClock* clock);
  ~SimulatedNetworkTransport() override;

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
  struct State {
    NodeId node_id;
    SimulatedNetwork* network;
    SimulatedClock* clock;
    RpcRequestHandler request_handler;
    ConnectionCallback connection_callback;
    std::unordered_map<uint64_t, RpcResponseCallback> pending_callbacks;
    std::mutex callbacks_mtx;
  };
  std::shared_ptr<State> state_;
};

}  // namespace rollingraft
