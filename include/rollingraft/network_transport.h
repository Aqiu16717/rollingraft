#pragma once

#include <functional>
#include <rollingraft/status.h>
#include <rollingraft/types.h>

namespace rollingraft {

// RPC response callback
// - response_data: response data (serialized)
// - success: whether successful
// - error_msg: error message (if failed)
using RpcResponseCallback =
    std::function<void(const std::string& response_data, bool success,
                       const std::string& error_msg)>;

// RPC request handler
// - from: source node ID of the request
// - request_data: request data (serialized)
// - response_data: output response data (needs serialization)
using RpcRequestHandler = std::function<void(
    NodeId from, const std::string& request_data, std::string& response_data)>;

// Connection state callback
// - peer_id: peer node ID
// - addr: peer address
// - connected: true=连接建立, false=连接断开
using ConnectionCallback =
    std::function<void(NodeId peer_id, const NodeAddr& addr, bool connected)>;

class NetworkTransport {
 public:
  virtual ~NetworkTransport() = default;

  // Initialize transport layer
  // @param listen_addr: listen address (e.g., "0.0.0.0:8001")
  // @param handler: callback for handling received requests
  // @return Status::OK() on success
  virtual Status Initialize(const NodeAddr& listen_addr,
                            RpcRequestHandler handler) = 0;

  // Set connection state callback (optional)
  virtual void SetConnectionCallback(ConnectionCallback callback) = 0;

  // Start transport layer, begin listening
  virtual Status Start() = 0;

  // Stop transport layer, close all connections
  virtual Status Stop() = 0;

  // Send RPC request (async)
  // @param to: target node ID
  // @param addr: target address
  // @param request_data: request data (serialized)
  // @param timeout: timeout duration
  // @param callback: response callback (executed in IO thread)
  virtual void SendRpc(NodeId to, const NodeAddr& addr,
                       const std::string& request_data,
                       std::chrono::milliseconds timeout,
                       RpcResponseCallback callback) = 0;
};

}  // namespace rollingraft
