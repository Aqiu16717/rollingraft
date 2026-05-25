#include "mock_state_machine.h"

#include <cstring>
#include <sstream>

namespace rollingraft {

ApplyResult MockStateMachine::Apply(std::span<const uint8_t> data,
                                    uint64_t index) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::string cmd(data.begin(), data.end());
  applied_commands_[index] = cmd;
  last_applied_index_ = index;

  // Notify waiters
  auto it = waiters_.begin();
  while (it != waiters_.end() && it->first <= index) {
    for (auto& cb : it->second) {
      if (cb) cb();
    }
    it = waiters_.erase(it);
  }

  ApplyResult result;
  result.success = true;
  result.response = cmd;  // Echo back
  result.applied_index = index;
  return result;
}

uint64_t MockStateMachine::GetLastAppliedIndex() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return last_applied_index_;
}

std::shared_ptr<Snapshot> MockStateMachine::CreateSnapshot() {
  std::lock_guard<std::mutex> lock(mutex_);

  auto snapshot = std::make_shared<SnapshotImpl>();
  snapshot->last_index = last_applied_index_;
  snapshot->last_term = 0;  // Simplified

  // Serialize all applied commands
  std::ostringstream oss;
  for (const auto& [idx, cmd] : applied_commands_) {
    oss << idx << ":" << cmd << "\n";
  }
  std::string str = oss.str();
  snapshot->data.assign(str.begin(), str.end());

  return snapshot;
}

bool MockStateMachine::Restore(const std::vector<uint8_t>& snapshot) {
  std::lock_guard<std::mutex> lock(mutex_);

  applied_commands_.clear();
  std::string str(snapshot.begin(), snapshot.end());
  std::istringstream iss(str);
  std::string line;

  while (std::getline(iss, line)) {
    auto pos = line.find(':');
    if (pos != std::string::npos) {
      uint64_t idx = std::stoull(line.substr(0, pos));
      std::string cmd = line.substr(pos + 1);
      applied_commands_[idx] = cmd;
      if (idx > last_applied_index_) {
        last_applied_index_ = idx;
      }
    }
  }

  return true;
}

void MockStateMachine::WaitIndex(uint64_t index, std::function<void()> cb) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (index <= last_applied_index_) {
    if (cb) cb();
  } else {
    waiters_[index].push_back(std::move(cb));
  }
}

ApplyResult MockStateMachine::Query(std::span<const uint8_t> data) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::string cmd(data.begin(), data.end());

  ApplyResult result;
  result.success = true;
  result.response = cmd;  // Echo back the query
  result.applied_index = last_applied_index_;
  return result;
}

std::vector<std::string> MockStateMachine::GetAppliedCommands() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::string> result;
  for (const auto& [idx, cmd] : applied_commands_) {
    result.push_back(cmd);
  }
  return result;
}

std::string MockStateMachine::GetCommandAt(uint64_t index) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = applied_commands_.find(index);
  return it != applied_commands_.end() ? it->second : "";
}

bool MockStateMachine::WasIndexApplied(uint64_t index) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return applied_commands_.count(index) > 0;
}

void MockStateMachine::NotifyWaiters(uint64_t index) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = waiters_.begin();
  while (it != waiters_.end() && it->first <= index) {
    for (auto& cb : it->second) {
      if (cb) cb();
    }
    it = waiters_.erase(it);
  }
}

void MockStateMachine::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  applied_commands_.clear();
  last_applied_index_ = 0;
  waiters_.clear();
}

}  // namespace rollingraft
