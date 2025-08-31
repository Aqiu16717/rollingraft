#pragma once

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

}  // namespace rollingraft