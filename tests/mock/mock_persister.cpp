#include "mock_persister.h"

namespace rollingraft {

Status MockPersister::Open(const std::string& dir) {
  (void)dir;
  if (CheckFailure()) return Status::Error(failure_msg_);
  return Status::OK();
}

void MockPersister::Close() {}

Status MockPersister::SaveState(const PersistentState& state) {
  if (CheckFailure()) return Status::Error(failure_msg_);
  std::lock_guard<std::mutex> lock(mutex_);
  state_ = state;
  return Status::OK();
}

Status MockPersister::LoadState(PersistentState& state) {
  std::lock_guard<std::mutex> lock(mutex_);
  state = state_;
  return Status::OK();
}

Status MockPersister::AppendEntries(const std::vector<RaftLogEntry>& entries) {
  if (CheckFailure()) return Status::Error(failure_msg_);
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& entry : entries) {
    logs_[entry.index_] = entry;
  }
  ++write_count_;
  return Status::OK();
}

Status MockPersister::Sync() {
  if (CheckFailure()) return Status::Error(failure_msg_);
  std::lock_guard<std::mutex> lock(mutex_);
  ++sync_count_;
  return Status::OK();
}

Status MockPersister::GetEntries(uint64_t start, uint64_t end,
                                 std::vector<RaftLogEntry>* out) {
  std::lock_guard<std::mutex> lock(mutex_);
  out->clear();
  for (uint64_t i = start; i < end; ++i) {
    auto it = logs_.find(i);
    if (it != logs_.end()) {
      out->push_back(it->second);
    }
  }
  return Status::OK();
}

Status MockPersister::GetEntry(uint64_t index, RaftLogEntry& entry) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = logs_.find(index);
  if (it == logs_.end()) {
    return Status::Error("Entry not found");
  }
  entry = it->second;
  return Status::OK();
}

Status MockPersister::TruncateSuffix(uint64_t from_index) {
  if (CheckFailure()) return Status::Error(failure_msg_);
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto it = logs_.begin(); it != logs_.end();) {
    if (it->first >= from_index) {
      it = logs_.erase(it);
    } else {
      ++it;
    }
  }
  return Status::OK();
}

Status MockPersister::TruncatePrefix(uint64_t before_index) {
  if (CheckFailure()) return Status::Error(failure_msg_);
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto it = logs_.begin(); it != logs_.end();) {
    if (it->first < before_index) {
      it = logs_.erase(it);
    } else {
      ++it;
    }
  }
  return Status::OK();
}

std::pair<uint64_t, uint64_t> MockPersister::GetLastLogInfo() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (logs_.empty()) {
    return {0, 0};
  }
  const auto& last = logs_.rbegin()->second;
  return {last.index_, last.term_};
}

Status MockPersister::SaveSnapshot(const std::string& snapshot_data,
                                   uint64_t last_index, uint64_t last_term) {
  if (CheckFailure()) return Status::Error(failure_msg_);
  std::lock_guard<std::mutex> lock(mutex_);
  snapshot_data_ = snapshot_data;
  snapshot_last_index_ = last_index;
  snapshot_last_term_ = last_term;
  return Status::OK();
}

Status MockPersister::LoadSnapshot(std::string& snapshot_data,
                                   uint64_t& last_index, uint64_t& last_term) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (snapshot_data_.empty()) {
    return Status::Error("No snapshot");
  }
  snapshot_data = snapshot_data_;
  last_index = snapshot_last_index_;
  last_term = snapshot_last_term_;
  return Status::OK();
}

bool MockPersister::HasSnapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return !snapshot_data_.empty();
}

void MockPersister::InjectFailure(const std::string& error_msg) {
  failure_msg_ = error_msg;
}

void MockPersister::ClearFailure() { failure_msg_.clear(); }

size_t MockPersister::EntryCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return logs_.size();
}

void MockPersister::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  logs_.clear();
  state_ = {0, -1};
  snapshot_data_.clear();
  write_count_ = 0;
  sync_count_ = 0;
}

bool MockPersister::CheckFailure() { return !failure_msg_.empty(); }

}  // namespace rollingraft
