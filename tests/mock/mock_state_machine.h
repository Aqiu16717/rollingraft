#pragma once

#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "rollingraft/state_machine.h"

namespace rollingraft {

/**
 * Mock state machine for testing.
 * Records applied commands and supports snapshots.
 */
class MockStateMachine : public StateMachine {
 public:
  MockStateMachine() = default;
  ~MockStateMachine() override = default;

  ApplyResult Apply(std::span<const uint8_t> data, uint64_t index) override;

  uint64_t GetLastAppliedIndex() const override;

  std::shared_ptr<Snapshot> CreateSnapshot() override;

  bool Restore(const std::vector<uint8_t>& snapshot) override;

  void WaitIndex(uint64_t index, std::function<void()> cb) override;

  // Test control interface

  /**
   * Get all applied commands.
   */
  std::vector<std::string> GetAppliedCommands() const;

  /**
   * Get command at specific index.
   */
  std::string GetCommandAt(uint64_t index) const;

  /**
   * Check if specific index was applied.
   */
  bool WasIndexApplied(uint64_t index) const;

  /**
   * Trigger waiters up to the specified index.
   */
  void NotifyWaiters(uint64_t index);

  /**
   * Reset all state.
   */
  void Reset();

 private:
  struct SnapshotImpl : public Snapshot {
    std::vector<uint8_t> data;
    uint64_t last_index = 0;
    uint64_t last_term = 0;

    const SnapshotMeta& GetMeta() const override {
      static SnapshotMeta meta;
      meta.last_included_index_ = last_index;
      meta.last_included_term_ = last_term;
      return meta;
    }

    size_t Read(uint64_t offset, uint8_t* dest, size_t length) override {
      if (offset >= data.size()) return 0;
      size_t to_copy =
          std::min(length, data.size() - static_cast<size_t>(offset));
      std::memcpy(dest, data.data() + offset, to_copy);
      return to_copy;
    }

    std::string GetPath() const override { return ""; }
  };

  mutable std::mutex mutex_;
  std::map<uint64_t, std::string> applied_commands_;
  uint64_t last_applied_index_ = 0;
  std::map<uint64_t, std::vector<std::function<void()>>> waiters_;
};

}  // namespace rollingraft
