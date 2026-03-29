#pragma once

#include <functional>
#include <rollingraft/raft_log.h>
#include <rollingraft/status.h>
#include <rollingraft/types.h>

namespace rollingraft {
class Persister {
 public:
  virtual ~Persister() = default;
  virtual Status Write(const std::string& data) = 0;
  virtual Status Read(std::string& data) = 0;
};
} // namespace rollingraft