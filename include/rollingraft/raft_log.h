#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rollingraft/status.h"

namespace rollingraft {

struct RaftLogEntry {
  RaftLogEntry() = default;
  RaftLogEntry(uint32_t index, uint32_t term, const std::string& command)
      : index_(index), term_(term), command_(command) {}

  uint32_t index_;
  uint32_t term_;
  std::string data_;
  std::string command_;
};

class RaftLog {
 public:
  RaftLog() = default;
  Status AppendLogEntry(const RaftLogEntry& log_entry) {
    entries_.push_back(log_entry);
    return Status();
  }
  uint32_t LastLogIndex() const;
  uint32_t LastLogTerm() const;

 private:
  std::vector<RaftLogEntry> entries_;
};

}  // namespace rollingraft
