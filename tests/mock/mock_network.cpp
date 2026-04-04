#include "mock_network.h"

namespace rollingraft {

Status MockNetworkTransport::Initialize(const NodeAddr& listen_addr,
                                        RpcRequestHandler handler) {
  request_handler_ = std::move(handler);
  return Status::OK();
}

void MockNetworkTransport::SetConnectionCallback(ConnectionCallback callback) {
  connection_callback_ = std::move(callback);
}

Status MockNetworkTransport::Start() {
  return Status::OK();
}

Status MockNetworkTransport::Stop() {
  return Status::OK();
}

void MockNetworkTransport::SendRpc(NodeId to, const NodeAddr& addr,
                                   const std::string& request_data,
                                   std::chrono::milliseconds timeout,
                                   RpcResponseCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Check if peer is partitioned
  auto it = partitioned_peers_.find(to);
  if (it != partitioned_peers_.end() && it->second) {
    // Simulate network failure
    if (callback) {
      callback("", false, "Network partitioned");
    }
    return;
  }

  recorded_requests_.push_back({to, addr, request_data, timeout, callback});

  if (auto_response_ && callback) {
    callback(auto_response_data_, auto_response_success_, "");
  }
}

std::vector<MockNetworkTransport::RecordedRequest>
MockNetworkTransport::GetRecordedRequests() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return recorded_requests_;
}

void MockNetworkTransport::ClearRecordedRequests() {
  std::lock_guard<std::mutex> lock(mutex_);
  recorded_requests_.clear();
}

void MockNetworkTransport::TriggerResponse(size_t request_index,
                                           const std::string& response_data,
                                           bool success,
                                           const std::string& error) {
  RpcResponseCallback callback;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (request_index >= recorded_requests_.size()) return;
    callback = recorded_requests_[request_index].callback;
  }
  if (callback) {
    callback(response_data, success, error);
  }
}

void MockNetworkTransport::SetAutoResponse(const std::string& response_data,
                                           bool success) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto_response_ = true;
  auto_response_data_ = response_data;
  auto_response_success_ = success;
}

void MockNetworkTransport::ClearAutoResponse() {
  std::lock_guard<std::mutex> lock(mutex_);
  auto_response_ = false;
  auto_response_data_.clear();
}

void MockNetworkTransport::SetPartitioned(NodeId peer_id, bool partitioned) {
  std::lock_guard<std::mutex> lock(mutex_);
  partitioned_peers_[peer_id] = partitioned;
}

void MockNetworkTransport::InjectResponse(NodeId from,
                                          const std::string& response_data) {
  // This simulates receiving an RPC response from a peer
  // In a real implementation, this would call the request handler
  // For unit testing purposes, we just store it or trigger callbacks
  std::lock_guard<std::mutex> lock(mutex_);
  // Implementation placeholder - actual injection depends on test needs
  (void)from;
  (void)response_data;
}

}  // namespace rollingraft
