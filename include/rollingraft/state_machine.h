#pragma once
namespace rollingraft {

struct SnapshotMeta {
  uint64_t last_included_index_;
  uint64_t last_included_term_;
};

// read-only readview
class Snapshot {
 public:
  virtual ~Snapshot() = default;

  virtual const SnapshotMeta& GetMeta() const = 0;

  virtual size_t Read(uint64_t offset, uint8_t* dest, size_t length) = 0;

  virtual std::string GetPath() const = 0;
};

struct ApplyResult {
  bool success_;
  // data that return to client
  std::string response_;
  uint64_t applied_index_;
};

class StateMachine {
 public:
  StateMachine() = default;
  virtual ~StateMachine() = default;

  // write
  virtual ApplyResult Apply(std::span<const uint8_t> data, uint64_t index) = 0;

  // read
  virtual uint64_t GetLastAppliedIndex() const = 0;

  // create snapshot handle, lightweight, fast, and nonblock
  virtual std::shared_ptr<Snapshot> CreateSnapshot() = 0;

  // load from outside
  virtual bool Restore(const std::vector<uint8_t>& snapshot) = 0;

  // for linearizable reads: block or callback until the index reaches the
  // target value
  virtual void WaitIndex(uint64_t index, std::function<void()> cb) = 0;
};

}  // namespace rollingraft
