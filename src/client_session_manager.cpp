#include "rollingraft/client_session_manager.h"

#include "rollingraft/logger.h"

namespace rollingraft {

ClientSessionManager::ClientSessionManager(size_t max_sessions, uint64_t ttl_ms)
    : max_sessions_(max_sessions), ttl_ms_(ttl_ms) {}

bool ClientSessionManager::IsDuplicate(uint64_t session_id, uint64_t seq_num,
                                       SessionResult& result) {
  std::lock_guard<std::mutex> lock(mtx_);

  auto it = sessions_.find(session_id);
  if (it == sessions_.end()) {
    return false;  // New session
  }

  // Touch session (mark as recently used)
  TouchSession(session_id, it->second);

  auto& entry = it->second.entry;
  if (seq_num < entry.last_seq) {
    // Old request: return cached result
    result = entry.last_result;
    return true;
  }

  if (seq_num == entry.last_seq) {
    // Exact duplicate: return cached result
    result = entry.last_result;
    return true;
  }

  // New sequence number
  return false;
}

void ClientSessionManager::RecordResult(uint64_t session_id, uint64_t seq_num,
                                        const SessionResult& result) {
  std::lock_guard<std::mutex> lock(mtx_);

  auto it = sessions_.find(session_id);
  if (it != sessions_.end()) {
    auto& entry = it->second.entry;
    entry.last_seq = seq_num;
    entry.last_result = result;
    entry.last_active = std::chrono::steady_clock::now();
    TouchSession(session_id, it->second);
    return;
  }

  // New session: check capacity
  if (sessions_.size() >= max_sessions_ && max_sessions_ > 0) {
    EvictLRU();
  }

  // Insert new session at front of LRU
  lru_list_.push_front(session_id);
  ClientSessionEntry entry;
  entry.last_seq = seq_num;
  entry.last_result = result;
  entry.last_active = std::chrono::steady_clock::now();

  SessionNode node;
  node.entry = std::move(entry);
  node.lru_it = lru_list_.begin();
  sessions_[session_id] = std::move(node);
}

size_t ClientSessionManager::EvictExpired() {
  if (ttl_ms_ == 0) {
    return 0;
  }

  std::lock_guard<std::mutex> lock(mtx_);
  auto now = std::chrono::steady_clock::now();
  size_t removed = 0;

  // Iterate from back (LRU) to front for efficient removal
  auto it = lru_list_.rbegin();
  while (it != lru_list_.rend()) {
    auto session_id = *it;
    auto sit = sessions_.find(session_id);
    if (sit == sessions_.end()) {
      ++it;
      continue;
    }

    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - sit->second.entry.last_active)
            .count();
    if (elapsed >= 0 && static_cast<uint64_t>(elapsed) >= ttl_ms_) {
      // Remove expired session
      auto base_it = std::next(it).base();  // Convert reverse to forward iterator
      sessions_.erase(sit);
      it = std::list<uint64_t>::reverse_iterator(lru_list_.erase(base_it));
      ++removed;
    } else {
      // Sessions are in LRU order, so if this one isn't expired,
      // more recent ones won't be either (they were touched more recently)
      // BUT: a session could be very old but still recently touched,
      // so we can't early-exit. We need to check all.
      ++it;
    }
  }

  return removed;
}

size_t ClientSessionManager::Size() const {
  std::lock_guard<std::mutex> lock(mtx_);
  return sessions_.size();
}

void ClientSessionManager::Clear() {
  std::lock_guard<std::mutex> lock(mtx_);
  sessions_.clear();
  lru_list_.clear();
}

void ClientSessionManager::TouchSession(uint64_t session_id, SessionNode& node) {
  // Move to front of LRU list
  lru_list_.erase(node.lru_it);
  lru_list_.push_front(session_id);
  node.lru_it = lru_list_.begin();
  node.entry.last_active = std::chrono::steady_clock::now();
}

void ClientSessionManager::EvictLRU() {
  if (lru_list_.empty()) {
    return;
  }

  auto session_id = lru_list_.back();
  sessions_.erase(session_id);
  lru_list_.pop_back();
}

}  // namespace rollingraft
