/**
 * @file raft_log.h
 * @brief In-memory Raft log management
 *
 * Provides in-memory storage for Raft log entries with support
 * for log truncation and snapshot integration.
 */

#pragma once

#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "rollingraft/status.h"
#include "rollingraft/types.h"

namespace rollingraft {

/** Single entry in the Raft log. */
struct RaftLogEntry {
  RaftLogEntry() = default;
  RaftLogEntry(Index index, Term term, const std::string& command)
      : index_(index), term_(term), command_(command) {}

  Index index_;            // Position in the log (1-based)
  Term term_;              // Term when entry was created
  std::string data_;       // Binary data payload
  std::string command_;    // Command string (legacy, prefer data_)
  uint32_t checksum_ = 0;  // CRC32 checksum for data integrity
};

/**
 * In-memory Raft log manager.
 *
 * Manages log entries in memory with support for:
 * - Appending new entries
 * - Random access by index
 * - Range queries for replication
 * - Truncation on conflicts
 * - Snapshot integration (shifting start index)
 *
 * Note: This is the in-memory cache. Persistence is handled separately
 * by LogPersister.
 *
 * Thread-safety: Not thread-safe. External synchronization required.
 */
class RaftLog {
 public:
  RaftLog() = default;

  /**
   * Append a new log entry.
   *
   * @param term Current term
   * @param data Entry data
   * @return Pair of (index, status)
   */
  std::pair<Index, Status> Append(Term term, std::string data);

  /**
   * Append an existing log entry (used during recovery).
   *
   * @param entry Entry to append
   * @return Status::OK() on success
   */
  Status AppendLogEntry(const RaftLogEntry& entry);

  /**
   * Get a single log entry by index.
   *
   * @param index Log index
   * @return Entry if found, nullopt otherwise
   */
  std::optional<RaftLogEntry> GetEntry(Index index) const;

  /**
   * Get log entries in range [start, end).
   *
   * @param start First index (inclusive)
   * @param end Last index (exclusive)
   * @return Vector of entries in range
   */
  std::vector<RaftLogEntry> GetEntries(Index start, Index end) const;

  /**
   * Get information about the last log entry.
   *
   * @return Pair of (last_index, last_term)
   */
  std::pair<Index, Term> GetLastLogInfo() const;

  /**
   * Get the term of a specific log entry.
   *
   * @param index Log index
   * @return Term of entry (0 if not found)
   */
  Term GetLogTerm(Index index) const;

  /**
   * Truncate log entries from index onwards.
   *
   * Called when log conflict is detected.
   *
   * @param from_index First index to delete
   * @return Status::OK() on success
   */
  Status TruncateSuffix(Index from_index);

  /** Get last log index. Convenience method. */
  Index LastLogIndex() const { return GetLastLogInfo().first; }

  /** Get last log term. Convenience method. */
  Term LastLogTerm() const { return GetLastLogInfo().second; }

  /**
   * Get the first available log index.
   *
   * Returns 1 for fresh logs, or (last_snapshot_index + 1) after snapshot.
   *
   * @return First valid log index
   */
  Index GetFirstIndex() const { return start_index_; }

  /**
   * Set new start index after installing snapshot.
   *
   * Discards all entries before the new start index.
   *
   * @param index New first valid index
   */
  void SetStartIndex(Index index);

  /**
   * Get log statistics for snapshot trigger decision.
   *
   * @return Pair of (entry count since last snapshot, estimated bytes)
   */
  std::pair<size_t, size_t> GetLogStats() const;

 private:
  /** Convert logical log index to physical deque index. */
  size_t ToPhysicalIndex(Index logical_index) const;

  /** Check if index is within valid range. */
  bool IsInRange(Index index) const;

 private:
  std::deque<RaftLogEntry> entries_;  // Log entries storage
  Index start_index_ = 1;  // First valid index (may be > 1 after snapshot)
};

}  // namespace rollingraft
