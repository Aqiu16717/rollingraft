#pragma once
namespace rollingraft {

struct SnapshotMeta {
  uint64_t last_included_index;
  uint64_t last_included_term;
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
  std::string response_;
  uint64_t applied_index_;
};

class StateMachine {
 public:
  StateMachine() = default;
  virtual ~StateMachine() = default;

  virtual ApplyResult Apply(const std::vector<uint8_t>& data,
                            uint64_t index) = 0;
  virtual uint64_t GetLastAppliedIndex() const = 0;

  virtual std::vector<uint8_t> Snapshot() = 0;
  virtual bool Restore(const std::vector<uint8_t>& snapshot) = 0;
};

}  // namespace rollingraft
