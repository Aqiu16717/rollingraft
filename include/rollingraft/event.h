/**
 * @file event.h
 * @brief Agent-friendly event notification system for RollingRaft
 *
 * Provides a unified event bus for Raft node lifecycle, leadership changes,
 * membership changes, log compaction, and snapshot events. AI agents can
 * subscribe to these events to react to cluster state changes without polling.
 *
 * Example:
 * @code
 *   node.Subscribe([](const NodeRoleChangedEvent& e) {
 *     LOG_INFO("Node {} became {} in term {}", e.node_id,
 *              RaftNodeRoleToString(e.new_role), e.term);
 *   });
 * @endcode
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <vector>

#include "rollingraft/raft_node.h"
#include "rollingraft/types.h"

namespace rollingraft {

// Forward declarations
class RaftNode;

// ============================================================================
// Event Types
// ============================================================================

/** Node role transition event. */
struct NodeRoleChangedEvent {
  static constexpr const char* kName = "NodeRoleChanged";
  NodeId node_id = -1;
  RaftNodeRole old_role = FOLLOWER;
  RaftNodeRole new_role = FOLLOWER;
  Term term = 0;
  std::chrono::steady_clock::time_point timestamp;
};

/** Leader change event (detected on any node). */
struct LeaderChangedEvent {
  static constexpr const char* kName = "LeaderChanged";
  NodeId node_id = -1;
  NodeId old_leader_id = -1;
  NodeId new_leader_id = -1;
  NodeAddr new_leader_addr;
  Term term = 0;
  std::chrono::steady_clock::time_point timestamp;
};

/** Membership change event. */
struct MembershipChangedEvent {
  static constexpr const char* kName = "MembershipChanged";
  enum class ChangeType { kAdd, kRemove };

  NodeId node_id = -1;
  ChangeType change_type = ChangeType::kAdd;
  NodeId target_node_id = -1;
  NodeAddr target_node_addr;
  uint64_t config_version = 0;
  Term term = 0;
  std::chrono::steady_clock::time_point timestamp;
};

/** Log compaction / snapshot trigger event. */
struct LogCompactedEvent {
  static constexpr const char* kName = "LogCompacted";
  NodeId node_id = -1;
  Index snapshot_index = 0;
  Term snapshot_term = 0;
  uint64_t bytes_reclaimed = 0;
  uint64_t log_size_before = 0;
  uint64_t log_size_after = 0;
  std::chrono::steady_clock::time_point timestamp;
};

/** Snapshot installation event (follower receiving from leader). */
struct SnapshotInstalledEvent {
  static constexpr const char* kName = "SnapshotInstalled";
  NodeId node_id = -1;
  NodeId from_leader_id = -1;
  Index snapshot_index = 0;
  Term snapshot_term = 0;
  uint64_t bytes_received = 0;
  bool success = false;
  std::string error_message;
  std::chrono::steady_clock::time_point timestamp;
};

/** Proposal committed to log (reached majority). */
struct ProposalCommittedEvent {
  static constexpr const char* kName = "ProposalCommitted";
  NodeId node_id = -1;
  Index index = 0;
  Term term = 0;
  std::chrono::steady_clock::time_point timestamp;
};

/** Proposal applied to state machine. */
struct ProposalAppliedEvent {
  static constexpr const char* kName = "ProposalApplied";
  NodeId node_id = -1;
  Index index = 0;
  Term term = 0;
  bool success = false;
  std::chrono::steady_clock::time_point timestamp;
};

/** Election timeout fired. */
struct ElectionTimeoutEvent {
  static constexpr const char* kName = "ElectionTimeout";
  NodeId node_id = -1;
  Term current_term = 0;
  RaftNodeRole current_role = FOLLOWER;
  std::chrono::steady_clock::time_point timestamp;
};

/** Node lifecycle event. */
struct NodeLifecycleEvent {
  static constexpr const char* kName = "NodeLifecycle";
  enum class State { kStarted, kStopped };

  NodeId node_id = -1;
  State state = State::kStarted;
  std::chrono::steady_clock::time_point timestamp;
};

// ============================================================================
// Event Variant (type-safe union)
// ============================================================================

/**
 * Type-safe variant containing any Raft event.
 *
 * Uses std::shared_ptr<void> for type erasure to avoid heavy
 * std::variant overhead. Visitors are dispatched via type_index.
 */
class RaftEvent {
 public:
  template <typename T>
  explicit RaftEvent(T event)  // NOLINT(google-explicit-constructor)
      : type_(typeid(T)),
        data_(std::make_shared<T>(std::move(event))),
        name_(T::kName) {}

  /** Get event type name for logging/debugging. */
  const char* Name() const { return name_; }

  /** Check if event holds type T. */
  template <typename T>
  bool Is() const {
    return type_ == typeid(T);
  }

  /** Get event as type T (undefined if Is<T>() is false). */
  template <typename T>
  const T& As() const {
    return *static_cast<const T*>(data_.get());
  }

 private:
  std::type_index type_;
  std::shared_ptr<void> data_;
  const char* name_;
};

// ============================================================================
// Event Bus
// ============================================================================

/**
 * Thread-safe event bus for publishing and subscribing to Raft events.
 *
 * Supports typed subscriptions and wildcard (all events) subscriptions.
 * Events are dispatched synchronously on the publisher's thread.
 */
class EventBus {
 public:
  using EventHandler = std::function<void(const RaftEvent&)>;

  /**
   * Subscribe to a specific event type.
   * @tparam T Event type (e.g., NodeRoleChangedEvent)
   * @param handler Callback invoked when event is published
   * @return Subscription ID for unsubscribing
   */
  template <typename T>
  uint64_t Subscribe(std::function<void(const T&)> handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t id = next_id_++;
    auto wrapped = [handler](const RaftEvent& event) {
      if (event.Is<T>()) {
        handler(event.As<T>());
      }
    };
    subscriptions_[typeid(T)][id] = std::move(wrapped);
    return id;
  }

  /**
   * Subscribe to all events (wildcard).
   * @param handler Callback invoked for every published event
   * @return Subscription ID for unsubscribing
   */
  uint64_t SubscribeAll(EventHandler handler);

  /**
   * Unsubscribe by ID.
   * @param id Subscription ID returned from Subscribe
   */
  void Unsubscribe(uint64_t id);

  /**
   * Publish an event to all matching subscribers.
   * @tparam T Event type
   * @param event Event data
   * @note Synchronous dispatch on caller's thread
   */
  template <typename T>
  void Publish(const T& event) {
    std::vector<EventHandler> handlers;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // Typed subscribers
      auto it = subscriptions_.find(typeid(T));
      if (it != subscriptions_.end()) {
        for (const auto& [id, handler] : it->second) {
          handlers.push_back(handler);
        }
      }
      // Wildcard subscribers
      for (const auto& [id, handler] : wildcard_subs_) {
        handlers.push_back(handler);
      }
    }
    RaftEvent wrapped(event);
    for (const auto& handler : handlers) {
      handler(wrapped);
    }
  }

  /** Get number of active subscriptions. */
  size_t SubscriptionCount() const;

 private:
  mutable std::mutex mutex_;
  uint64_t next_id_ = 1;
  std::unordered_map<std::type_index, std::unordered_map<uint64_t, EventHandler>>
      subscriptions_;
  std::unordered_map<uint64_t, EventHandler> wildcard_subs_;
};

}  // namespace rollingraft
