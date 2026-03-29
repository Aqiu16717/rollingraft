#include "rollingraft/raft_log.h"

#include <algorithm>
#include <cstring>

namespace rollingraft {

std::pair<Index, Status> RaftLog::Append(Term term, std::string data) {
  auto [last_index, _] = GetLastLogInfo();
  Index new_index = (last_index == 0) ? start_index_ : last_index + 1;

  RaftLogEntry entry(new_index, term, std::move(data));
  entries_.push_back(std::move(entry));

  return {new_index, Status::OK()};
}

Status RaftLog::AppendLogEntry(const RaftLogEntry& entry) {
  entries_.push_back(entry);
  return Status::OK();
}

std::optional<RaftLogEntry> RaftLog::GetEntry(Index index) const {
  if (!IsInRange(index)) {
    return std::nullopt;
  }
  return entries_[ToPhysicalIndex(index)];
}

std::vector<RaftLogEntry> RaftLog::GetEntries(Index start, Index end) const {
  std::vector<RaftLogEntry> result;

  if (start >= end || entries_.empty()) {
    return result;
  }

  // 调整到有效范围
  Index actual_start = std::max(start, start_index_);
  Index actual_end =
      std::min(end, start_index_ + static_cast<Index>(entries_.size()));

  if (actual_start >= actual_end) {
    return result;
  }

  size_t phys_start = ToPhysicalIndex(actual_start);
  size_t phys_end = ToPhysicalIndex(actual_end);

  result.reserve(phys_end - phys_start);
  for (size_t i = phys_start; i < phys_end; ++i) {
    result.push_back(entries_[i]);
  }

  return result;
}

std::pair<Index, Term> RaftLog::GetLastLogInfo() const {
  if (entries_.empty()) {
    return {0, 0};
  }
  const auto& last = entries_.back();
  return {last.index_, last.term_};
}

Term RaftLog::GetLogTerm(Index index) const {
  if (index == 0) return 0;

  auto entry = GetEntry(index);
  if (!entry) return 0;
  return entry->term_;
}

Status RaftLog::TruncateSuffix(Index from_index) {
  if (from_index > start_index_ + entries_.size()) {
    return Status::OK();  // 无需截断
  }

  if (from_index <= start_index_) {
    entries_.clear();
  } else {
    size_t new_size = ToPhysicalIndex(from_index);
    if (new_size < entries_.size()) {
      entries_.resize(new_size);
    }
  }

  return Status::OK();
}

size_t RaftLog::ToPhysicalIndex(Index logical_index) const {
  return static_cast<size_t>(logical_index - start_index_);
}

bool RaftLog::IsInRange(Index index) const {
  return index >= start_index_ &&
         index < start_index_ + static_cast<Index>(entries_.size());
}

}  // namespace rollingraft
