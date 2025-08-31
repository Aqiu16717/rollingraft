#pragma once
#include "rollingraft/logger.h"

namespace rollingraft {
class SpdlogAdapter : public Logger {
 public:
  SpdlogAdapter();
  ~SpdlogAdapter() override;

  void Log(LogLevel level, const std::string& message) override;
  LogLevel GetLogLevel() const override;
  void SetLogLevel(LogLevel level) override;

 private:
  class Impl;
  Impl* impl_;  // Pointer to implementation
};

}  // namespace rollingraft