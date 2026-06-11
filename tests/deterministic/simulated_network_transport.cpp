#include "simulated_network_transport.h"

#include "simulated_clock.h"
#include "simulated_network.h"

namespace rollingraft {

SimulatedNetworkTransport::SimulatedNetworkTransport(
    NodeId node_id, SimulatedNetwork* network, SimulatedClock* clock)
    : state_(std::make_shared<State>()) {
  state_->node_id = node_id;
  state_->network = network;
  state_->clock = clock;
}

SimulatedNetworkTransport::~SimulatedNetworkTransport() { Stop(); }

Status SimulatedNetworkTransport::Initialize(const NodeAddr& listen_addr,
                                             RpcRequestHandler handler) {
  (void)listen_addr;
  state_->request_handler = std::move(handler);

  auto weak = std::weak_ptr<State>(state_);
  state_->network->RegisterEndpoint(
      state_->node_id,
      [weak](NodeId from, const std::string& payload, uint64_t correlation_id,
             std::string& response) {
        auto state = weak.lock();
        if (!state) return;

        // Try as incoming request first to avoid correlation_id collision
        // (different nodes may use the same correlation_id independently)
        if (state->request_handler) {
          state->request_handler(from, payload, response);
          if (!response.empty()) {
            return;  // Handled as request
          }
        }
        // Empty response — treat as response to a pending RPC
        RpcResponseCallback cb;
        {
          std::lock_guard<std::mutex> lock(state->callbacks_mtx);
          auto it = state->pending_callbacks.find(correlation_id);
          if (it != state->pending_callbacks.end()) {
            cb = std::move(it->second);
            state->pending_callbacks.erase(it);
          }
        }
        if (cb) {
          cb(payload, true, "");
        }
      });
  return Status::OK();
}

void SimulatedNetworkTransport::SetConnectionCallback(
    ConnectionCallback callback) {
  state_->connection_callback = std::move(callback);
}

Status SimulatedNetworkTransport::Start() { return Status::OK(); }

Status SimulatedNetworkTransport::Stop() {
  if (!state_) return Status::OK();
  if (state_->network) {
    state_->network->UnregisterEndpoint(state_->node_id);
  }
  {
    std::lock_guard<std::mutex> lock(state_->callbacks_mtx);
    state_->pending_callbacks.clear();
  }
  // Release shared state so any pending clock callbacks with weak_ptr
  // will no-op instead of accessing dangling memory.
  state_.reset();
  return Status::OK();
}

void SimulatedNetworkTransport::SendRpc(
    NodeId to, [[maybe_unused]] const NodeAddr& addr, const std::string& request_data,
    uint64_t correlation_id, std::chrono::milliseconds timeout,
    RpcResponseCallback callback) {
  if (!state_) return;
  {
    std::lock_guard<std::mutex> lock(state_->callbacks_mtx);
    state_->pending_callbacks[correlation_id] = std::move(callback);
  }
  state_->network->Send(state_->node_id, to, request_data, correlation_id);

  // Schedule timeout
  uint64_t timeout_ms = static_cast<uint64_t>(timeout.count());
  if (timeout_ms > 0 && state_->clock) {
    auto weak = std::weak_ptr<State>(state_);
    state_->clock->After(timeout_ms, [weak, correlation_id]() {
      auto state = weak.lock();
      if (!state) return;
      RpcResponseCallback cb;
      {
        std::lock_guard<std::mutex> lock(state->callbacks_mtx);
        auto it = state->pending_callbacks.find(correlation_id);
        if (it != state->pending_callbacks.end()) {
          cb = std::move(it->second);
          state->pending_callbacks.erase(it);
        }
      }
      if (cb) {
        cb("", false, "timeout");
      }
    });
  }
}

}  // namespace rollingraft
