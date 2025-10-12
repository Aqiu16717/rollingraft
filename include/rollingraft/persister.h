#pragma once

#include <rollingraft/status.h>

namespace rollingraft {
class Persister {
 public:
  virtual ~Persister() = default;
  virtual Status Write(const std::string& data) = 0;
  virtual Status Read(std::string& data) = 0;
};
} // namespace rollingraft