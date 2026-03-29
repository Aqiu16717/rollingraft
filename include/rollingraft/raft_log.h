#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rollingraft/status.h"
#include "rollingraft/types.h"

namespace rollingraft {

struct RaftLogEntry {
  RaftLogEntry() = default;
  RaftLogEntry(Index index, Term term, const std::string& command)
      : index_(index), term_(term), command_(command) {}

  Index index_;
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
  Index LastLogIndex() const;
  Term LastLogTerm() const;

 private:
  std::vector<RaftLogEntry> entries_;
};

}  // namespace rollingraft
