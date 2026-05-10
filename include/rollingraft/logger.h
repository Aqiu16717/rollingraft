/**
 * @file logger.h
 * @brief Logging interface and macros
 *
 * Provides abstract Logger interface and convenient macros
 * for logging throughout the codebase. Uses fmt::format
 * for type-safe formatting.
 *
 * Example:
 *   LOG_INFO("Node {} became leader in term {}", node_id, term);
 */

#pragma once

#include <memory>
#include <mutex>
#include <spdlog/fmt/fmt.h>
#include <string>

namespace rollingraft {

/** Log severity levels. */
enum class LogLevel { TRACE, DEBUG, INFO, WARN, ERROR, FATAL };

/**
 * Abstract logger interface.
 *
 * Implement this interface to integrate with your logging framework.
 * Default implementation uses stdout/stderr.
 */
class Logger {
 public:
  virtual ~Logger() = default;

  /**
   * Log a message at the specified level.
   * @param level Severity level
   * @param message Message to log
   */
  virtual void Log(LogLevel level, const std::string& message) = 0;

  /** Get current minimum log level. */
  virtual LogLevel GetLogLevel() const = 0;

  /** Set minimum log level (messages below this are filtered). */
  virtual void SetLogLevel(LogLevel level) = 0;

  /** Convenience methods for each log level. */
  void Trace(const std::string& message) { Log(LogLevel::TRACE, message); }
  void Debug(const std::string& message) { Log(LogLevel::DEBUG, message); }
  void Info(const std::string& message) { Log(LogLevel::INFO, message); }
  void Warn(const std::string& message) { Log(LogLevel::WARN, message); }
  void Error(const std::string& message) { Log(LogLevel::ERROR, message); }
  void Fatal(const std::string& message) { Log(LogLevel::FATAL, message); }
};

/**
 * Singleton factory for accessing the global logger.
 *
 * Thread-safe. SetLogger() should be called during initialization
 * to provide a custom logger implementation.
 */
class LoggerFactory {
 public:
  LoggerFactory(const LoggerFactory&) = delete;
  LoggerFactory& operator=(const LoggerFactory&) = delete;
  LoggerFactory(LoggerFactory&&) = delete;
  LoggerFactory& operator=(LoggerFactory&&) = delete;

  /** Get the singleton instance. */
  static LoggerFactory& Instance() {
    // Intentionally leak the instance to avoid destruction order issues.
    // The logger must be available until the very end of program execution
    // since other singletons and static objects may log during destruction.
    static LoggerFactory* instance = new LoggerFactory();
    return *instance;
  }

  /** Get the current logger (may be nullptr if not set). */
  Logger* GetLogger();

  /** Set a custom logger implementation. */
  void SetLogger(std::unique_ptr<Logger> logger);

 private:
  LoggerFactory() = default;
  ~LoggerFactory() = default;

  mutable std::mutex mutex_;
  std::unique_ptr<Logger> logger_;
};

/**
 * Internal macro for logging with formatting.
 * Use LOG_TRACE, LOG_DEBUG, etc. instead of calling directly.
 */
#define LOG_MESSAGE(level, fmt_str, ...)                                   \
  do {                                                                     \
    Logger* logger = LoggerFactory::Instance().GetLogger();                \
    if (logger && logger->GetLogLevel() <= level) {                        \
      logger->Log(level, fmt::format(fmt_str __VA_OPT__(, ) __VA_ARGS__)); \
    }                                                                      \
  } while (0)

/** Log a TRACE message. */
#define LOG_TRACE(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::TRACE, fmt_str __VA_OPT__(, ) __VA_ARGS__)

/** Log a DEBUG message. */
#define LOG_DEBUG(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::DEBUG, fmt_str __VA_OPT__(, ) __VA_ARGS__)

/** Log an INFO message. */
#define LOG_INFO(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::INFO, fmt_str __VA_OPT__(, ) __VA_ARGS__)

/** Log a WARN message. */
#define LOG_WARN(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::WARN, fmt_str __VA_OPT__(, ) __VA_ARGS__)

/** Log an ERROR message. */
#define LOG_ERROR(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::ERROR, fmt_str __VA_OPT__(, ) __VA_ARGS__)

/** Log a FATAL message. */
#define LOG_FATAL(fmt_str, ...) \
  LOG_MESSAGE(rollingraft::LogLevel::FATAL, fmt_str __VA_OPT__(, ) __VA_ARGS__)

}  // namespace rollingraft
