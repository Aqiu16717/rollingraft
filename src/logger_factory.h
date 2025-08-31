#pragma once

#include <memory>
#include "rollingraft/logger.h"

namespace rollingraft {

class LoggerFactory {
 public:
  LoggerFactory(const LoggerFactory&) = delete;
  LoggerFactory& operator=(const LoggerFactory&) = delete;
  LoggerFactory(LoggerFactory&&) = delete;
  LoggerFactory& operator=(LoggerFactory&&) = delete;

  static LoggerFactory& Instance() {
    static LoggerFactory instance;
    return instance;
  }

  Logger* GetLogger();
  void SetLogger(std::unique_ptr<Logger> logger);

 private:
  LoggerFactory() = default;
  ~LoggerFactory() = default;

  // Use unique_ptr to manage the logger's lifetime
  std::unique_ptr<Logger> logger_;
};
}  // namespace rollingraft
