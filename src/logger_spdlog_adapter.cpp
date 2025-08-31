#include "logger_spdlog_adapter.h"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

using namespace rollingraft;

class SpdlogAdapter::Impl {
 public:
  Impl() {
    logger_ = spdlog::basic_logger_mt("rollingraft_logger", "rollingraft.log");
    logger_->set_level(spdlog::level::info);
  }
  ~Impl() { spdlog::drop("rollingraft_logger"); }

  void Log(LogLevel level, const std::string& message) {
    switch (level) {
      case LogLevel::TRACE:
        logger_->trace(message);
        break;
      case LogLevel::DEBUG:
        logger_->debug(message);
        break;
      case LogLevel::INFO:
        logger_->info(message);
        break;
      case LogLevel::WARN:
        logger_->warn(message);
        break;
      case LogLevel::ERROR:
        logger_->error(message);
        break;
      case LogLevel::FATAL:
        logger_->critical(message);
        break;
    }
  }

  LogLevel GetLogLevel() const {
    auto level = logger_->level();
    switch (level) {
      case spdlog::level::trace:
        return LogLevel::TRACE;
      case spdlog::level::debug:
        return LogLevel::DEBUG;
      case spdlog::level::info:
        return LogLevel::INFO;
      case spdlog::level::warn:
        return LogLevel::WARN;
      case spdlog::level::err:
        return LogLevel::ERROR;
      case spdlog::level::critical:
        return LogLevel::FATAL;
      default:
        return LogLevel::INFO;  // Default fallback
    }
  }

  void SetLogLevel(LogLevel level) {
    switch (level) {
      case LogLevel::TRACE:
        logger_->set_level(spdlog::level::trace);
        break;
      case LogLevel::DEBUG:
        logger_->set_level(spdlog::level::debug);
        break;
      case LogLevel::INFO:
        logger_->set_level(spdlog::level::info);
        break;
      case LogLevel::WARN:
        logger_->set_level(spdlog::level::warn);
        break;
      case LogLevel::ERROR:
        logger_->set_level(spdlog::level::err);
        break;
      case LogLevel::FATAL:
        logger_->set_level(spdlog::level::critical);
        break;
    }
  }

 private:
  std::shared_ptr<spdlog::logger> logger_;
};

SpdlogAdapter::SpdlogAdapter() : impl_(new Impl()) {}

SpdlogAdapter::~SpdlogAdapter() { delete impl_; }

void SpdlogAdapter::Log(LogLevel level, const std::string& message) {
  impl_->Log(level, message);
}

LogLevel SpdlogAdapter::GetLogLevel() const { return impl_->GetLogLevel(); }

void SpdlogAdapter::SetLogLevel(LogLevel level) { impl_->SetLogLevel(level); }
