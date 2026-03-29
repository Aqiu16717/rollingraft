#pragma once

#include <cstdint>
#include <deque>
#include <optional>
#include <span>
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

// Log manager class (in-memory cache)
class RaftLog {
 public:
  RaftLog() = default;

  // Append single log entry
  std::pair<Index, Status> Append(Term term, std::string data);

  Status AppendLogEntry(const RaftLogEntry& entry);

  std::optional<RaftLogEntry> GetEntry(Index index) const;

  std::vector<RaftLogEntry> GetEntries(Index start, Index end) const;

  std::pair<Index, Term> GetLastLogInfo() const;

  Term GetLogTerm(Index index) const;

  Status TruncateSuffix(Index from_index);

  Index LastLogIndex() const { return GetLastLogInfo().first; }

  Term LastLogTerm() const { return GetLastLogInfo().second; }

 private:
  size_t ToPhysicalIndex(Index logical_index) const;
  bool IsInRange(Index index) const;

 private:
  std::deque<RaftLogEntry> entries_;
  Index start_index_ = 1;
};

}  // namespace rollingraft
