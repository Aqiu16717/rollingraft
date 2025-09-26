#pragma once

#include <format>
#include <string>

namespace rollingraft {

enum class LogLevel { TRACE, DEBUG, INFO, WARN, ERROR, FATAL };

class Logger {
 public:
  virtual ~Logger() = default;

  virtual void Log(LogLevel level, const std::string& message) = 0;
  virtual LogLevel GetLogLevel() const = 0;
  virtual void SetLogLevel(LogLevel level) = 0;

  void Trace(const std::string& message) { Log(LogLevel::TRACE, message); }
  void Debug(const std::string& message) { Log(LogLevel::DEBUG, message); }
  void Info(const std::string& message) { Log(LogLevel::INFO, message); }
  void Warn(const std::string& message) { Log(LogLevel::WARN, message); }
  void Error(const std::string& message) { Log(LogLevel::ERROR, message); }
  void Fatal(const std::string& message) { Log(LogLevel::FATAL, message); }
};

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

  mutable std::mutex mutex_;
  std::unique_ptr<Logger> logger_;
};

#define LOG_MESSAGE(level, fmt_str, ...)                       \
  do {                                                         \
    Logger* logger = LoggerFactory::Instance().GetLogger();    \
    if (logger && logger->GetLogLevel() <= level) {            \
      logger->Log(level, std::format(fmt_str, ##__VA_ARGS__)); \
    }                                                          \
  } while (0)

#define LOG_TRACE(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::TRACE, fmt_str, ##__VA_ARGS__)
#define LOG_DEBUG(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::DEBUG, fmt_str, ##__VA_ARGS__)
#define LOG_INFO(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::INFO, fmt_str, ##__VA_ARGS__)
#define LOG_WARN(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::WARN, fmt_str, ##__VA_ARGS__)
#define LOG_ERROR(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::ERROR, fmt_str, ##__VA_ARGS__)
#define LOG_FATAL(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::FATAL, fmt_str, ##__VA_ARGS__)

}  // namespace rollingraft