#pragma once
namespace rollingraft {

class StateMachine {
 public:
  StateMachine() = default;
  virtual ~StateMachine() = default;

  virtual void Apply(const std::vector<uintt_t>& command) = 0;
  virtual std::vector<uint8_t> Query(const std::vector<uint8_t>& query) = 0;

  virtual std::vector<uint8_t> Snapshot() = 0;
  virtual void Restore(const std::vector<uint8_t>& snapshot) = 0;
};

}  // namespace rollingraft
