/**
 * @file state_machine.h
 * @brief State machine interface for Raft log application
 *
 * Users implement this interface to define how commands are applied
 * to their application state. The state machine must be deterministic
 * since the same log entries will be applied on all nodes.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace rollingraft {

/**
 * Metadata for a snapshot.
 *
 * Identifies the log position (index, term) that the snapshot covers.
 * Used during snapshot transfer to ensure consistency.
 */
struct SnapshotMeta {
  uint64_t last_included_index_;  // Last log index in snapshot
  uint64_t last_included_term_;   // Term of last included index
};

/**
 * Interface for reading snapshot data.
 *
 * Provides a read-only view of snapshot data. Implementations should
 * be lightweight and support streaming reads for large snapshots.
 */
class Snapshot {
 public:
  virtual ~Snapshot() = default;

  /**
   * Get snapshot metadata.
   * @return Reference to snapshot metadata
   */
  virtual const SnapshotMeta& GetMeta() const = 0;

  /**
   * Read data from snapshot at given offset.
   *
   * @param offset Byte offset to start reading from
   * @param dest Buffer to store read data
   * @param length Maximum bytes to read
   * @return Actual bytes read (may be less than length at EOF)
   */
  virtual size_t Read(uint64_t offset, uint8_t* dest, size_t length) = 0;

  /**
   * Get the file path for this snapshot (if file-based).
   * @return Path to snapshot file, or empty string if not file-based
   */
  virtual std::string GetPath() const = 0;
};

/**
 * Result of applying a command to the state machine.
 *
 * Returned by StateMachine::Apply() to indicate success/failure
 * and provide response data to the client.
 */
struct ApplyResult {
  bool success = false;        // Whether application succeeded
  std::string response;        // Response data to return to client
  uint64_t applied_index = 0;  // Log index that was applied
  std::string error_message;   // Error message if success is false
};

/**
 * State machine interface for Raft log application.
 *
 * Implement this interface to integrate your application with Raft.
 * The state machine must be deterministic - applying the same log entry
 * (same index and term) must produce the same result on all nodes.
 *
 * Thread-safety: Implementations must be thread-safe as Apply()
 * may be called concurrently with reads.
 */
class StateMachine {
 public:
  StateMachine() = default;
  virtual ~StateMachine() = default;

  // Non-copyable
  StateMachine(const StateMachine&) = delete;
  StateMachine& operator=(const StateMachine&) = delete;

  /**
   * Apply a committed log entry to the state machine.
   *
   * This is the core method that updates your application state.
   * Called when a log entry is committed by Raft consensus.
   *
   * @param data Raw command data from the log entry
   * @param index Log index being applied (monotonically increasing)
   * @return Result including success status and response to client
   *
   * @note Must be deterministic - same (data, index) must produce
   *       the same result on all nodes
   */
  virtual ApplyResult Apply(std::span<const uint8_t> data, uint64_t index) = 0;

  /**
   * Get the last applied log index.
   *
   * @return Index of the most recently applied log entry
   */
  virtual uint64_t GetLastAppliedIndex() const = 0;

  /**
   * Create a snapshot of the current state.
   *
   * Called periodically by Raft to compact the log. The snapshot
   * should capture the current state at GetLastAppliedIndex().
   *
   * @return Shared pointer to snapshot implementation
   * @note Should be fast and non-blocking if possible
   */
  virtual std::shared_ptr<Snapshot> CreateSnapshot() = 0;

  /**
   * Restore state from a snapshot.
   *
   * Called when a follower receives a snapshot from the leader.
   * Replaces current state with the snapshot contents.
   *
   * @param snapshot Raw snapshot data
   * @return true if restore succeeded, false otherwise
   */
  virtual bool Restore(const std::vector<uint8_t>& snapshot) = 0;

  /**
   * Wait for a specific log index to be applied.
   *
   * Used for linearizable reads. The callback is invoked when
   * GetLastAppliedIndex() >= index.
   *
   * @param index Target log index to wait for
   * @param cb Callback to invoke when index is reached
   */
  virtual void WaitIndex(uint64_t index, std::function<void()> cb) = 0;

  /**
   * Execute a read-only query against the state machine.
   *
   * Called after ReadIndex confirms the node is still leader and
   * the target log index has been applied. The query must not
   * modify state machine state.
   *
   * @param data Raw query data
   * @return Result including response data
   */
  virtual ApplyResult Query(std::span<const uint8_t> data) = 0;
};

}  // namespace rollingraft
