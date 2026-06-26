#include "simulated_network.h"

#include <algorithm>
#include <optional>

#include "rollingraft/network_transport.h"

#include "simulated_clock.h"
namespace rollingraft {
SimulatedNetwork::SimulatedNetwork(SimulatedClock* clock, uint64_t seed)
    : clock_(clock), rng_(static_cast<uint32_t>(seed)) {}

void SimulatedNetwork::RegisterEndpoint(NodeId id, MessageHandler handler) {
  std::lock_guard<std::mutex> lock(mtx_);
  endpoints_[id] = std::move(handler);
}

void SimulatedNetwork::UnregisterEndpoint(NodeId id) {
  std::lock_guard<std::mutex> lock(mtx_);
  endpoints_.erase(id);
}

void SimulatedNetwork::Send(NodeId from, NodeId to, const std::string& payload,
                            uint64_t correlation_id) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (partitions_.find({std::min(from, to), std::max(from, to)}) != partitions_.end()) {
    return;
  }
  if (drop_probability_ > 0.0f) {
    std::uniform_real_distribution<float> d(0.0f, 1.0f);
    if (d(rng_) < drop_probability_) return;
  }
  uint64_t now = clock_->Now(), delay = fixed_delay_ms_;
  if (next_delay_count_ > 0) {
    delay = next_delay_ms_;
    --next_delay_count_;
  }
  SimulatedMessage msg{from, to, payload, correlation_id, now, now + delay};
  pending_messages_.push_back(msg);
  if (duplicate_probability_ > 0.0f) {
    std::uniform_real_distribution<float> d(0.0f, 1.0f);
    if (d(rng_) < duplicate_probability_) pending_messages_.push_back(msg);
  }
  if (reorder_) std::shuffle(pending_messages_.begin(), pending_messages_.end(), rng_);
}

void SimulatedNetwork::DeliverAll() { DeliverUntil(clock_->Now()); }

bool SimulatedNetwork::DeliverOne() {
  std::optional<SimulatedMessage> msg;
  {
    std::lock_guard<std::mutex> lock(mtx_);
    uint64_t now = clock_->Now();
    auto it = std::find_if(pending_messages_.begin(), pending_messages_.end(),
                           [now](const SimulatedMessage& m) { return m.deliver_time_ms <= now; });
    if (it == pending_messages_.end()) return false;
    msg = std::move(*it);
    pending_messages_.erase(it);
  }
  if (msg) MaybeDeliver(*msg);
  return true;
}

void SimulatedNetwork::DeliverUntil(uint64_t time_ms) {
  std::vector<SimulatedMessage> ready;
  {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = pending_messages_.begin();
    while (it != pending_messages_.end()) {
      if (it->deliver_time_ms <= time_ms) {
        ready.push_back(std::move(*it));
        it = pending_messages_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto& msg : ready) MaybeDeliver(msg);
}

void SimulatedNetwork::MaybeDeliver(const SimulatedMessage& msg) {
  MessageHandler handler;
  bool connected = false;
  {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = endpoints_.find(msg.to);
    if (it != endpoints_.end()) {
      handler = it->second;
    }
    connected = partitions_.find({std::min(msg.to, msg.from), std::max(msg.to, msg.from)}) ==
                partitions_.end();
  }
  if (handler) {
    std::string response;
    handler(msg.from, msg.payload, msg.correlation_id, response);
    if (!response.empty() && connected) {
      Send(msg.to, msg.from, response, msg.correlation_id);
    }
  }
}

void SimulatedNetwork::Partition(NodeId a, NodeId b) {
  std::lock_guard<std::mutex> lock(mtx_);
  partitions_.insert({std::min(a, b), std::max(a, b)});
}

void SimulatedNetwork::HealPartition(NodeId a, NodeId b) {
  std::lock_guard<std::mutex> lock(mtx_);
  partitions_.erase({std::min(a, b), std::max(a, b)});
}

void SimulatedNetwork::HealAllPartitions() {
  std::lock_guard<std::mutex> lock(mtx_);
  partitions_.clear();
}

void SimulatedNetwork::DropMessages(float probability) {
  std::lock_guard<std::mutex> lock(mtx_);
  drop_probability_ = std::clamp(probability, 0.0f, 1.0f);
}

void SimulatedNetwork::DelayAll(uint64_t delay_ms) {
  std::lock_guard<std::mutex> lock(mtx_);
  fixed_delay_ms_ = delay_ms;
}

void SimulatedNetwork::DelayNext(uint64_t delay_ms, size_t count) {
  std::lock_guard<std::mutex> lock(mtx_);
  next_delay_ms_ = delay_ms;
  next_delay_count_ = count;
}

void SimulatedNetwork::DuplicateMessages(float probability) {
  std::lock_guard<std::mutex> lock(mtx_);
  duplicate_probability_ = std::clamp(probability, 0.0f, 1.0f);
}

void SimulatedNetwork::ReorderMessages(bool enable) {
  std::lock_guard<std::mutex> lock(mtx_);
  reorder_ = enable;
}

bool SimulatedNetwork::IsConnected(NodeId a, NodeId b) const {
  std::lock_guard<std::mutex> lock(mtx_);
  return partitions_.find({std::min(a, b), std::max(a, b)}) == partitions_.end();
}

size_t SimulatedNetwork::PendingMessageCount() const {
  std::lock_guard<std::mutex> lock(mtx_);
  return pending_messages_.size();
}

void SimulatedNetwork::ClearPendingMessages() {
  std::lock_guard<std::mutex> lock(mtx_);
  pending_messages_.clear();
}

}  // namespace rollingraft
