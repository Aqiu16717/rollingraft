#include <algorithm>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <rollingraft/raft_node.h>
#include <rollingraft/state_machine.h>
#include <span>
#include <string>
#include <thread>

class CounterSnapshot : public rollingraft::Snapshot {
 public:
  CounterSnapshot(int64_t value, uint64_t index, uint64_t term)
      : value_(value) {
    meta_.last_included_index_ = index;
    meta_.last_included_term_ = term;

    data_.resize(sizeof(int64_t));
    std::memcpy(data_.data(), &value_, sizeof(int64_t));
  }

  ~CounterSnapshot() = default;

  const rollingraft::SnapshotMeta& GetMeta() const override { return meta_; }

  size_t Read(uint64_t offset, uint8_t* dest, size_t length) override {
    if (offset > data_.size()) {
      return 0;
    }
    size_t can_read = std::min(data_.size() - offset, length);
    std::memcpy(dest, data_.data() + offset, can_read);
    return can_read;
  }

  std::string GetPath() const override { return ""; }

 private:
  rollingraft::SnapshotMeta meta_;
  int64_t value_;
  std::vector<uint8_t> data_;
};

class CounterMachine : public rollingraft::StateMachine {
 public:
  CounterMachine() : value_(0), last_applied_index_(0) {}

  ~CounterMachine() = default;

  rollingraft::ApplyResult Apply(std::span<const uint8_t> data,
                                 uint64_t index) override {
    std::lock_guard<std::mutex> lock(mtx_);

    std::string cmd(data.begin(), data.end());

    if (cmd == "inc") {
      ++value_;
    } else if (cmd == "dec") {
      --value_;
    } else if (cmd.rfind("add ", 0) == 0) {
      int64_t delta = std::stoll(cmd.substr(4));
      value_ += delta;
    } else if (cmd.rfind("sub ", 0) == 0) {
      int64_t delta = std::stoll(cmd.substr(4));
      value_ -= delta;
    }

    last_applied_index_ = index;

    rollingraft::ApplyResult result;
    result.success_ = true;
    result.response_ = std::to_string(value_);
    result.applied_index_ = index;

    NotifyWaiters(index);

    return result;
  }

  uint64_t GetLastAppliedIndex() const override {
    return last_applied_index_.load();
  }

  std::shared_ptr<rollingraft::Snapshot> CreateSnapshot() override {
    std::lock_guard<std::mutex> lock(mtx_);
    return std::make_shared<CounterSnapshot>(value_, last_applied_index_, 0);
  }

  bool Restore(const std::vector<uint8_t>& snapshot) override {
    std::lock_guard<std::mutex> lock(mtx_);
    if (snapshot.size() < sizeof(int64_t)) {
      return false;
    }
    std::memcpy(&value_, snapshot.data(), sizeof(int64_t));
    return true;
  }

  void WaitIndex(uint64_t index, std::function<void()> cb) override {
    std::lock_guard<std::mutex> lock(mtx_);
    if (last_applied_index_.load() >= index) {
      cb();
    } else {
      waiters_.emplace(index, std::move(cb));
    }
  }

  int64_t GetValue() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return value_;
  }

 private:
  void NotifyWaiters(uint64_t index) {
    auto it = waiters_.begin();
    while (it != waiters_.end() && it->first <= index) {
      it->second();
      it = waiters_.erase(it);
    }
  }

  mutable std::mutex mtx_;
  int64_t value_;
  std::atomic<uint64_t> last_applied_index_;
  std::multimap<uint64_t, std::function<void()>> waiters_;
};

void PrintUsage(const char* prog) {
  std::cout << "Usage: " << prog
            << " <node_id> <listen_port> <peer1_port> [peer2_port ...]\n";
  std::cout << "Example:\n";
  std::cout << "  " << prog << " 1 8001 8002 8003\n";
}

int main(int argc, char** argv) {
  if (argc < 4) {
    PrintUsage(argv[0]);
    return 1;
  }

  uint64_t node_id = std::stoll(argv[1]);
  std::string listen_port = argv[2];

  rollingraft::RaftNodeConfig config;
  config.node_id_ = node_id;
  config.listen_addr_ = "127.0.0.1:" + listen_port;
  config.data_dir_ = "./data/node" + std::to_string(node_id);

  std::vector<uint32_t> peers;
  for (int i = 3; i < argc; ++i) {
    peers.push_back(std::stoi(argv[i]));
  }

  rollingraft::RaftNode node(node_id, port, peers);
  node.BecomeFollower();

  std::cout << "Counter node " << node_id << " started on port " << port
            << "\n";
  std::cout << "Peers: ";
  for (auto p : peers) {
    std::cout << p << " ";
  }
  std::cout << "\n";

  auto sm = std::make_unique<CounterMachine>();

  std::this_thread::sleep_for(std::chrono::seconds(60));

  return 0;
}
