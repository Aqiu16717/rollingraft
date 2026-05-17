#pragma once
#include "rollingraft/logger.h"
#include "rollingraft/types.h"

namespace rollingraft {

class SpdlogAdapter : public Logger {
 public:
  SpdlogAdapter();
  ~SpdlogAdapter() override;

  void Log(LogLevel level, const std::string& message) override;
  LogLevel GetLogLevel() const override;
  void SetLogLevel(LogLevel level) override;
  void ConfigureJsonMode(bool enabled, NodeId node_id = -1) override;

 private:
  class Impl;
  Impl* impl_;
};

}  // namespace rollingraft
