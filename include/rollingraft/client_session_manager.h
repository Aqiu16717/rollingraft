/**
 * @file client_session_manager.h
 * @brief In-memory client session manager with LRU eviction and TTL expiration
 *
 * Provides idempotent command execution by tracking (session_id, sequence_num)
 * pairs and caching their results. Used by RaftNode::Propose() for deduplication.
 */

#pragma once

#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

#include "rollingraft/state_machine.h"
#include "rollingraft/types.h"

namespace rollingraft {

/**
 * Cached result for a (session_id, seq_num) pair.
 */
struct SessionResult {
  bool success = false;
  std::string response;
  Index applied_index = 0;
  Term applied_term = 0;
  std::string error_message;
};

/**
 * In-memory client session with LRU tracking.
 */
struct ClientSessionEntry {
  uint64_t last_seq = 0;
  SessionResult last_result;
  std::chrono::steady_clock::time_point last_active;
};

/**
 * Thread-safe client session manager.
 *
 * Features:
 * - Deduplication: rejects duplicate (session_id, seq_num) requests
 * - LRU eviction: removes oldest sessions when capacity exceeded
 * - TTL expiration: removes inactive sessions after a timeout
 * - Thread-safe: all operations protected by internal mutex
 */
class ClientSessionManager {
 public:
  /**
   * @param max_sessions Maximum number of sessions to keep in memory
   * @param ttl_ms Session TTL in milliseconds (0 = no expiration)
   */
  explicit ClientSessionManager(size_t max_sessions = 10000, uint64_t ttl_ms = 300000);

  /**
   * Check if a request is a duplicate and return cached result if so.
   *
   * @param session_id Client session identifier
   * @param seq_num Monotonically increasing sequence number
   * @param result Out parameter for cached result (only valid if returns true)
   * @return true if duplicate (result cached), false if new request
   */
  bool IsDuplicate(uint64_t session_id, uint64_t seq_num, SessionResult& result);

  /**
   * Record a successful execution result for a session.
   *
   * @param session_id Client session identifier
   * @param seq_num Sequence number that was executed
   * @param result Execution result to cache
   */
  void RecordResult(uint64_t session_id, uint64_t seq_num, const SessionResult& result);

  /**
   * Remove expired sessions (inactive longer than TTL).
   *
   * @return Number of sessions removed
   */
  size_t EvictExpired();

  /**
   * Get current session count.
   */
  size_t Size() const;

  /**
   * Clear all sessions.
   */
  void Clear();

 private:
  mutable std::mutex mtx_;
  size_t max_sessions_;
  uint64_t ttl_ms_;

  // LRU list: front = most recently used, back = least recently used
  std::list<uint64_t> lru_list_;

  struct SessionNode {
    ClientSessionEntry entry;
    std::list<uint64_t>::iterator lru_it;
  };

  std::unordered_map<uint64_t, SessionNode> sessions_;

  void TouchSession(uint64_t session_id, SessionNode& node);
  void EvictLRU();
};

}  // namespace rollingraft
