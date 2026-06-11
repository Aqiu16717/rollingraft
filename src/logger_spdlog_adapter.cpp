/**
 * @file logger_spdlog_adapter.cpp
 * @brief Spdlog-based logger adapter implementation
 */

#include "logger_spdlog_adapter.h"

#include <chrono>
#include <iomanip>
#include <sstream>

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

using namespace rollingraft;

class SpdlogAdapter::Impl {
 public:
  Impl()
      : logger_(spdlog::basic_logger_mt("rollingraft_logger", "rollingraft.log")) {
    logger_->set_level(spdlog::level::info);
  }
  ~Impl() { spdlog::drop("rollingraft_logger"); }

  void Log(LogLevel level, const std::string& message) {
    if (json_mode_) {
      std::string json = FormatJson(level, message);
      logger_->info(json);
      return;
    }
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
        return LogLevel::INFO;
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

  void SetJsonMode(bool enabled, NodeId node_id) {
    json_mode_ = enabled;
    node_id_ = node_id;
    if (enabled) {
      logger_->set_pattern("%v");
    } else {
      logger_->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] %v");
    }
  }

 private:
  static std::string LevelToString(LogLevel level) {
    switch (level) {
      case LogLevel::TRACE: return "TRACE";
      case LogLevel::DEBUG: return "DEBUG";
      case LogLevel::INFO: return "INFO";
      case LogLevel::WARN: return "WARN";
      case LogLevel::ERROR: return "ERROR";
      case LogLevel::FATAL: return "FATAL";
    }
    return "UNKNOWN";
  }

  static std::string Iso8601Now() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) % 1000;
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm tm_utc = *std::gmtime(&time_t_now);
    std::ostringstream oss;
    oss << std::put_time(&tm_utc, "%Y-%m-%dT%H:%M:%S");
    oss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    oss << 'Z';
    return oss.str();
  }

  std::string FormatJson(LogLevel level, const std::string& message) {
    std::ostringstream oss;
    oss << "{\"timestamp\":\"" << Iso8601Now() << "\"";
    oss << ",\"level\":\"" << LevelToString(level) << "\"";
    if (node_id_ >= 0) {
      oss << ",\"node_id\":" << node_id_;
    }
    oss << ",\"message\":\"";
    for (char c : message) {
      switch (c) {
        case '"': oss << "\\\""; break;
        case '\\': oss << "\\\\"; break;
        case '\b': oss << "\\b"; break;
        case '\f': oss << "\\f"; break;
        case '\n': oss << "\\n"; break;
        case '\r': oss << "\\r"; break;
        case '\t': oss << "\\t"; break;
        default:
          if (static_cast<unsigned char>(c) < 0x20) {
            oss << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                << static_cast<int>(c);
          } else {
            oss << c;
          }
      }
    }
    oss << "\"}";
    return oss.str();
  }

  std::shared_ptr<spdlog::logger> logger_;
  bool json_mode_ = false;
  NodeId node_id_ = -1;
};

SpdlogAdapter::SpdlogAdapter() : impl_(std::make_unique<Impl>()) {}

SpdlogAdapter::~SpdlogAdapter() = default;

void SpdlogAdapter::Log(LogLevel level, const std::string& message) {
  impl_->Log(level, message);
}

LogLevel SpdlogAdapter::GetLogLevel() const { return impl_->GetLogLevel(); }

void SpdlogAdapter::SetLogLevel(LogLevel level) { impl_->SetLogLevel(level); }

void SpdlogAdapter::ConfigureJsonMode(bool enabled, NodeId node_id) {
  impl_->SetJsonMode(enabled, node_id);
}
