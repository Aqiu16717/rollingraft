/**
 * @file event.cpp
 * @brief EventBus implementation
 */

#include "rollingraft/event.h"

namespace rollingraft {

uint64_t EventBus::SubscribeAll(EventHandler handler) {
  std::lock_guard<std::mutex> lock(mutex_);
  uint64_t id = next_id_++;
  wildcard_subs_[id] = std::move(handler);
  return id;
}

void EventBus::Unsubscribe(uint64_t id) {
  std::lock_guard<std::mutex> lock(mutex_);
  // Search in typed subscriptions
  for (auto& [type, subs] : subscriptions_) {
    subs.erase(id);
  }
  // Search in wildcard subscriptions
  wildcard_subs_.erase(id);
}

size_t EventBus::SubscriptionCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t count = wildcard_subs_.size();
  for (const auto& [type, subs] : subscriptions_) {
    count += subs.size();
  }
  return count;
}

}  // namespace rollingraft
