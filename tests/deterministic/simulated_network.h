#pragma once
#include <cstdint>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "rollingraft/types.h"
namespace rollingraft {
class NetworkTransport;
class SimulatedClock;
struct SimulatedMessage {
  NodeId from; NodeId to; std::string payload;
  uint64_t send_time_ms; uint64_t deliver_time_ms;
};
class SimulatedNetwork {
 public:
  explicit SimulatedNetwork(SimulatedClock* clock, uint64_t seed = 42);
  void RegisterEndpoint(NodeId id, NetworkTransport* transport);
  void UnregisterEndpoint(NodeId id);
  void Send(NodeId from, NodeId to, const std::string& payload);
  void DeliverAll(); bool DeliverOne(); void DeliverUntil(uint64_t time_ms);
  void Partition(NodeId a, NodeId b); void HealPartition(NodeId a, NodeId b);
  void HealAllPartitions(); void DropMessages(float probability);
  void DelayAll(uint64_t delay_ms); void DelayNext(uint64_t delay_ms, size_t count = 1);
  void DuplicateMessages(float probability); void ReorderMessages(bool enable);
  bool IsConnected(NodeId a, NodeId b) const; size_t PendingMessageCount() const;
  void ClearPendingMessages();
 private:
  void MaybeDeliver(const SimulatedMessage& msg);
  SimulatedClock* clock_;
  std::unordered_map<NodeId, NetworkTransport*> endpoints_;
  std::vector<SimulatedMessage> pending_messages_;
  std::set<std::pair<NodeId, NodeId>> partitions_;
  float drop_probability_ = 0.0f; uint64_t fixed_delay_ms_ = 0;
  uint64_t next_delay_ms_ = 0; size_t next_delay_count_ = 0;
  float duplicate_probability_ = 0.0f; bool reorder_ = false;
  std::mt19937 rng_;
};
}  // namespace rollingraft
