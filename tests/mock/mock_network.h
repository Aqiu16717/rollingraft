#pragma once

#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "rollingraft/network_transport.h"

namespace rollingraft {

/**
 * Mock network transport for testing.
 * Records sent messages and allows manual response injection.
 */
class MockNetworkTransport : public NetworkTransport {
 public:
  MockNetworkTransport() = default;
  ~MockNetworkTransport() override = default;

  // Non-copyable, non-movable
  MockNetworkTransport(const MockNetworkTransport&) = delete;
  MockNetworkTransport& operator=(const MockNetworkTransport&) = delete;

  Status Initialize(const NodeAddr& listen_addr,
                    RpcRequestHandler handler) override;

  void SetConnectionCallback(ConnectionCallback callback) override;

  Status Start() override;
  Status Stop() override;

  void SendRpc(NodeId to, const NodeAddr& addr,
               const std::string& request_data,
               std::chrono::milliseconds timeout,
               RpcResponseCallback callback) override;

  // Test control interface

  struct RecordedRequest {
    NodeId to;
    NodeAddr addr;
    std::string request_data;
    std::chrono::milliseconds timeout;
    RpcResponseCallback callback;
  };

  /**
   * Get all recorded requests.
   */
  std::vector<RecordedRequest> GetRecordedRequests() const;

  /**
   * Clear recorded requests.
   */
  void ClearRecordedRequests();

  /**
   * Trigger a response for a specific request.
   */
  void TriggerResponse(size_t request_index, const std::string& response_data,
                       bool success = true,
                       const std::string& error = "");

  /**
   * Set auto-response for all future requests.
   */
  void SetAutoResponse(const std::string& response_data, bool success = true);

  /**
   * Disable auto-response.
   */
  void ClearAutoResponse();

  /**
   * Simulate network partition.
   */
  void SetPartitioned(NodeId peer_id, bool partitioned);

  /**
   * Inject a response as if it came from a peer.
   * For testing: simulates receiving RPC response.
   */
  void InjectResponse(NodeId from, const std::string& response_data);

 private:
  RpcRequestHandler request_handler_;
  ConnectionCallback connection_callback_;
  std::vector<RecordedRequest> recorded_requests_;
  mutable std::mutex mutex_;

  bool auto_response_ = false;
  std::string auto_response_data_;
  bool auto_response_success_ = true;

  std::map<NodeId, bool> partitioned_peers_;
};

}  // namespace rollingraft
