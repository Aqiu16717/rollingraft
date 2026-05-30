#pragma once
#include <memory>

#include "rollingraft/logger.h"
#include "rollingraft/types.h"

namespace rollingraft {

class SpdlogAdapter : public Logger {
 public:
  SpdlogAdapter();
  ~SpdlogAdapter() override;

  // Non-copyable, non-movable (Pimpl with unique_ptr)
  SpdlogAdapter(const SpdlogAdapter&) = delete;
  SpdlogAdapter& operator=(const SpdlogAdapter&) = delete;

  void Log(LogLevel level, const std::string& message) override;
  LogLevel GetLogLevel() const override;
  void SetLogLevel(LogLevel level) override;
  void ConfigureJsonMode(bool enabled, NodeId node_id = -1) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rollingraft
