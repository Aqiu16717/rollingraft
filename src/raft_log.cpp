/**
 * @file raft_log.cpp
 * @brief In-memory Raft log implementation
 */

#include "rollingraft/raft_log.h"

#include <algorithm>
#include <cstring>

namespace rollingraft {
namespace {

size_t EstimateEntryBytes(const RaftLogEntry& entry) {
  return sizeof(entry.index_) + sizeof(entry.term_) + entry.data_.size();
}

}  // namespace

std::pair<Index, Status> RaftLog::Append(Term term, std::string data) {
  auto [last_index, _] = GetLastLogInfo();
  Index new_index = (last_index == 0) ? start_index_ : last_index + 1;

  RaftLogEntry entry;
  entry.index_ = new_index;
  entry.term_ = term;
  entry.data_ = std::move(data);
  entries_.push_back(std::move(entry));
  estimated_bytes_ += EstimateEntryBytes(entries_.back());

  return {new_index, Status::OK()};
}

Status RaftLog::AppendLogEntry(const RaftLogEntry& entry) {
  entries_.push_back(entry);
  estimated_bytes_ += EstimateEntryBytes(entries_.back());
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

  // Clamp to valid range
  Index actual_start = std::max(start, start_index_);
  Index actual_end = std::min(end, start_index_ + static_cast<Index>(entries_.size()));

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
  if (index == 0) {
    return 0;
  }

  auto entry = GetEntry(index);
  if (!entry) {
    return 0;
  }
  return entry->term_;
}

Status RaftLog::TruncateSuffix(Index from_index) {
  if (from_index > start_index_ + entries_.size()) {
    return Status::OK();  // No truncation needed
  }

  if (from_index <= start_index_) {
    entries_.clear();
    estimated_bytes_ = 0;
  } else {
    size_t new_size = ToPhysicalIndex(from_index);
    while (entries_.size() > new_size) {
      estimated_bytes_ -= EstimateEntryBytes(entries_.back());
      entries_.pop_back();
    }
  }

  return Status::OK();
}

size_t RaftLog::ToPhysicalIndex(Index logical_index) const {
  return static_cast<size_t>(logical_index - start_index_);
}

bool RaftLog::IsInRange(Index index) const {
  return index >= start_index_ && index < start_index_ + static_cast<Index>(entries_.size());
}

void RaftLog::SetStartIndex(Index index) {
  // Clear all existing entries - they are now covered by snapshot
  entries_.clear();
  estimated_bytes_ = 0;
  start_index_ = index;
}

std::pair<size_t, size_t> RaftLog::GetLogStats() const {
  return {entries_.size(), estimated_bytes_};
}

}  // namespace rollingraft
