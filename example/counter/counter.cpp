#include <iostream>
#include <memory>
#include <rollingraft/raft_node.h>

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
    size_t can_read = std::min(data.size() - offset, length);
    std::memcpy(dest, data_.data() + offset, can_read);
    return can_read;
  }

  std::string GetPath() const override { return ""; }

 private:
  rollingraft::SnapshotMeta meta_;
  int64_t value_;
  std::vector<uint8_t> data_;
};

class CounterMachine : Public StateMachine {
 public:
};

int main() {
  rollingraft::RaftNodeConfig config;
  config.node_id = 1;
  config.listen_addr_ = "127.0.0.1:9527";
  config.listen_addr_ = {"127.0.0.1:9528", "127.0.0.1:9529"};
  config.data_dir_ = "./data/node1";

  auto sm = std::make_unique<CounterMachine>();

  return 0;
}
