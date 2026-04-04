/**
 * @file logger.cpp
 * @brief Logger factory implementation
 */

#include "rollingraft/logger.h"
#include "logger_spdlog_adapter.h"
#include <memory>

using namespace rollingraft;

Logger* LoggerFactory::GetLogger() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!logger_) {
    logger_ = std::make_unique<SpdlogAdapter>();
  }
  return logger_.get();
}

void LoggerFactory::SetLogger(std::unique_ptr<Logger> logger) {
  std::lock_guard<std::mutex> lock(mutex_);
  logger_ = std::move(logger);
}
