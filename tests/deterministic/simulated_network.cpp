#include "simulated_network.h"
#include <algorithm>
#include "rollingraft/network_transport.h"
#include "simulated_clock.h"
namespace rollingraft {
SimulatedNetwork::SimulatedNetwork(SimulatedClock* clock, uint64_t seed)
    : clock_(clock), rng_(static_cast<uint32_t>(seed)) {}
void SimulatedNetwork::RegisterEndpoint(NodeId id, NetworkTransport* transport) { endpoints_[id] = transport; }
void SimulatedNetwork::UnregisterEndpoint(NodeId id) { endpoints_.erase(id); }
void SimulatedNetwork::Send(NodeId from, NodeId to, const std::string& payload) {
  if (!IsConnected(from, to)) return;
  if (drop_probability_ > 0.0f) { std::uniform_real_distribution<float> d(0.0f,1.0f); if (d(rng_) < drop_probability_) return; }
  uint64_t now = clock_->Now(), delay = fixed_delay_ms_;
  if (next_delay_count_ > 0) { delay = next_delay_ms_; --next_delay_count_; }
  SimulatedMessage msg{from, to, payload, now, now + delay};
  pending_messages_.push_back(msg);
  if (duplicate_probability_ > 0.0f) { std::uniform_real_distribution<float> d(0.0f,1.0f); if (d(rng_) < duplicate_probability_) pending_messages_.push_back(msg); }
  if (reorder_) std::shuffle(pending_messages_.begin(), pending_messages_.end(), rng_);
}
void SimulatedNetwork::DeliverAll() { DeliverUntil(clock_->Now()); }
bool SimulatedNetwork::DeliverOne() {
  uint64_t now = clock_->Now();
  auto it = std::find_if(pending_messages_.begin(), pending_messages_.end(), [now](const SimulatedMessage& m){ return m.deliver_time_ms <= now; });
  if (it == pending_messages_.end()) return false;
  MaybeDeliver(*it); pending_messages_.erase(it); return true;
}
void SimulatedNetwork::DeliverUntil(uint64_t time_ms) {
  std::vector<SimulatedMessage> ready;
  auto it = pending_messages_.begin();
  while (it != pending_messages_.end()) {
    if (it->deliver_time_ms <= time_ms) { ready.push_back(std::move(*it)); it = pending_messages_.erase(it); }
    else ++it;
  }
  for (auto& msg : ready) MaybeDeliver(msg);
}
void SimulatedNetwork::MaybeDeliver(const SimulatedMessage& msg) {
  auto it = endpoints_.find(msg.to);
  if (it != endpoints_.end() && it->second) { std::string r; it->second->Send(msg.from, msg.payload, r); }
}
void SimulatedNetwork::Partition(NodeId a, NodeId b) { partitions_.insert({std::min(a,b), std::max(a,b)}); }
void SimulatedNetwork::HealPartition(NodeId a, NodeId b) { partitions_.erase({std::min(a,b), std::max(a,b)}); }
void SimulatedNetwork::HealAllPartitions() { partitions_.clear(); }
void SimulatedNetwork::DropMessages(float probability) { drop_probability_ = std::clamp(probability, 0.0f, 1.0f); }
void SimulatedNetwork::DelayAll(uint64_t delay_ms) { fixed_delay_ms_ = delay_ms; }
void SimulatedNetwork::DelayNext(uint64_t delay_ms, size_t count) { next_delay_ms_ = delay_ms; next_delay_count_ = count; }
void SimulatedNetwork::DuplicateMessages(float probability) { duplicate_probability_ = std::clamp(probability, 0.0f, 1.0f); }
void SimulatedNetwork::ReorderMessages(bool enable) { reorder_ = enable; }
bool SimulatedNetwork::IsConnected(NodeId a, NodeId b) const { return partitions_.find({std::min(a,b), std::max(a,b)}) == partitions_.end(); }
size_t SimulatedNetwork::PendingMessageCount() const { return pending_messages_.size(); }
void SimulatedNetwork::ClearPendingMessages() { pending_messages_.clear(); }
}  // namespace rollingraft
