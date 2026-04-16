/**
 * @file network_transport.h
 * @brief Abstract network transport interface for Raft RPC
 *
 * Defines the NetworkTransport interface for sending and receiving
 * Raft RPC messages. Implementations can use different networking
 * libraries (Asio, libuv, etc.).
 */

#pragma once

#include <chrono>
#include <functional>
#include <rollingraft/status.h>
#include <rollingraft/types.h>
#include <string>

namespace rollingraft {

/**
 * Callback for RPC responses.
 *
 * @param response_data Serialized response data
 * @param success Whether the RPC succeeded
 * @param error_msg Error message if failed
 */
using RpcResponseCallback =
    std::function<void(const std::string& response_data, bool success,
                       const std::string& error_msg)>;

/**
 * Handler for incoming RPC requests.
 *
 * @param from Source node ID
 * @param request_data Serialized request data
 * @param response_data Output buffer for response (to be serialized)
 */
using RpcRequestHandler = std::function<void(
    NodeId from, const std::string& request_data, std::string& response_data)>;

/**
 * Callback for connection state changes.
 *
 * @param peer_id Peer node ID
 * @param addr Peer network address
 * @param connected True if connected, false if disconnected
 */
using ConnectionCallback =
    std::function<void(NodeId peer_id, const NodeAddr& addr, bool connected)>;

/**
 * Abstract network transport interface.
 *
 * Implement this interface to provide custom network transport.
 * The transport is responsible for:
 * - Listening for incoming connections
 * - Sending RPC requests to peers
 * - Managing connection lifecycle
 *
 * Thread-safety: Implementations must be thread-safe as SendRpc()
 * may be called from multiple threads concurrently.
 */
class NetworkTransport {
 public:
  virtual ~NetworkTransport() = default;

  /**
   * Initialize the transport layer.
   *
   * @param listen_addr Address to listen on (e.g., "0.0.0.0:8001")
   * @param handler Callback for handling received RPC requests
   * @return Status::OK() on success
   */
  virtual Status Initialize(const NodeAddr& listen_addr,
                            RpcRequestHandler handler) = 0;

  /**
   * Set connection state callback (optional).
   *
   * Called when peer connections are established or closed.
   *
   * @param callback Function to call on connection state changes
   */
  virtual void SetConnectionCallback(ConnectionCallback callback) = 0;

  /**
   * Start the transport layer.
   *
   * Begins listening for incoming connections and enables sending.
   *
   * @return Status::OK() on success
   */
  virtual Status Start() = 0;

  /**
   * Stop the transport layer.
   *
   * Closes all connections and stops listening.
   *
   * @return Status::OK() on success
   */
  virtual Status Stop() = 0;

  /**
   * Send an RPC request asynchronously.
   *
   * The callback is invoked when the response arrives or timeout occurs.
   * Implementations should not block the caller.
   *
   * @param to Target node ID
   * @param addr Target node address
   * @param request_data Serialized request data
   * @param timeout Maximum time to wait for response
   * @param callback Callback for response or error
   */
  virtual void SendRpc(NodeId to, const NodeAddr& addr,
                       const std::string& request_data,
                       std::chrono::milliseconds timeout,
                       RpcResponseCallback callback) = 0;
};

}  // namespace rollingraft
