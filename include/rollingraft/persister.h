#pragma once

#include <functional>
#include <rollingraft/raft_log.h>
#include <rollingraft/status.h>
#include <rollingraft/types.h>

namespace rollingraft {

// Persistent metadata (must be persisted before each election)
struct PersistentState {
  Term current_term = 0;  // 当前任期
  NodeId voted_for = -1;  // ID of candidate voted for (-1 means not voted)
};

// Persistence interface
class Persister {
 public:
  virtual ~Persister() = default;

  // ==================== 生命周期 ====================

  // Open/create persistent storage
  // @param data_dir: 数据目录路径
  // @return: OK on success, IOError on failure
  virtual Status Open(const std::string& data_dir) = 0;

  // Close persistent storage
  virtual void Close() = 0;

  // ==================== Metadata Operations ====================

  // Save persistent state (must be synchronous write)
  // Called in the following scenarios:
  // - When becoming Candidate (term++, voted_for=self)
  // - When receiving RPC with higher term (revert to Follower)
  // - When voting for other candidates
  virtual Status SaveState(const PersistentState& state) = 0;

  // Load persistent state
  // If state doesn't exist, return OK with default state values
  virtual Status LoadState(PersistentState& state) = 0;

  // ==================== Log Operations ====================

  // Append log entries (batch)
  // Called when Leader receives client commands
  // Called when Follower receives AppendEntries
  virtual Status AppendEntries(const std::vector<RaftLogEntry>& entries) = 0;

  // Get log entries in range [start, end)
  // Used for Leader to send AppendEntries to Followers
  virtual Status GetEntries(uint64_t start, uint64_t end,
                            std::vector<RaftLogEntry>* out) = 0;

  // Get single log entry
  virtual Status GetEntry(uint64_t index, RaftLogEntry& entry) = 0;

  // Truncate log (delete all entries from index onwards)
  // Called on log conflict (Follower's log is newer than Leader's)
  virtual Status TruncateSuffix(uint64_t from_index) = 0;

  // Delete prefix logs (used to clean up old logs after snapshot)
  // Delete all logs in [1, before_index)
  virtual Status TruncatePrefix(uint64_t before_index) = 0;

  // Get info of the last log entry
  virtual std::pair<uint64_t, uint64_t> GetLastLogInfo() = 0;

  // ==================== Snapshot Operations (Optional) ====================

  // Save snapshot metadata
  // @param snapshot_data: serialized state machine data
  // @param last_index: last log index included in snapshot
  // @param last_term: last log term included in snapshot
  virtual Status SaveSnapshot(const std::string& snapshot_data,
                              uint64_t last_index, uint64_t last_term) {
    (void)snapshot_data;
    (void)last_index;
    (void)last_term;
    return Status::OK();  // Default no-op implementation
  }

  // Load snapshot
  virtual Status LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                              uint64_t& last_term) {
    (void)snapshot_data;
    (void)last_index;
    (void)last_term;
    return Status::OK();  // Default no-op implementation
  }

  // Get snapshot info
  virtual bool HasSnapshot() const { return false; }
};

// Factory function type
using PersisterFactory = std::function<std::unique_ptr<Persister>()>;

}  // namespace rollingraft
