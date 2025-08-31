#include "logger_factory.h"
#include "logger_spdlog_adapter.h"
#include <memory>

using namespace rollingraft;

Logger* LoggerFactory::GetLogger() {
  if (!logger_) {
    logger_ = std::make_unique<SpdlogAdapter>();
  }
  return logger_.get();
}

void LoggerFactory::SetLogger(std::unique_ptr<Logger> logger) {
  logger_ = std::move(logger);
}
