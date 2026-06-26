#pragma once
#include <cstdint>
#include <functional>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "rollingraft/network_transport.h"
#include "rollingraft/types.h"
namespace rollingraft {
class SimulatedClock;
struct SimulatedMessage {
  NodeId from;
  NodeId to;
  std::string payload;
  uint64_t correlation_id;
  uint64_t send_time_ms;
  uint64_t deliver_time_ms;
};
class SimulatedNetwork {
 public:
  using MessageHandler = std::function<void(NodeId from, const std::string& payload,
                                            uint64_t correlation_id, std::string& response)>;
  explicit SimulatedNetwork(SimulatedClock* clock, uint64_t seed = 42);
  void RegisterEndpoint(NodeId id, MessageHandler handler);
  void UnregisterEndpoint(NodeId id);
  void Send(NodeId from, NodeId to, const std::string& payload, uint64_t correlation_id = 0);
  void DeliverAll();
  bool DeliverOne();
  void DeliverUntil(uint64_t time_ms);
  void Partition(NodeId a, NodeId b);
  void HealPartition(NodeId a, NodeId b);
  void HealAllPartitions();
  void DropMessages(float probability);
  void DelayAll(uint64_t delay_ms);
  void DelayNext(uint64_t delay_ms, size_t count = 1);
  void DuplicateMessages(float probability);
  void ReorderMessages(bool enable);
  bool IsConnected(NodeId a, NodeId b) const;
  size_t PendingMessageCount() const;
  void ClearPendingMessages();

 private:
  void MaybeDeliver(const SimulatedMessage& msg);
  SimulatedClock* clock_;
  mutable std::mutex mtx_;
  std::unordered_map<NodeId, MessageHandler> endpoints_;
  std::vector<SimulatedMessage> pending_messages_;
  std::set<std::pair<NodeId, NodeId>> partitions_;
  float drop_probability_ = 0.0f;
  uint64_t fixed_delay_ms_ = 0;
  uint64_t next_delay_ms_ = 0;
  size_t next_delay_count_ = 0;
  float duplicate_probability_ = 0.0f;
  bool reorder_ = false;
  std::mt19937 rng_;
};
}  // namespace rollingraft
