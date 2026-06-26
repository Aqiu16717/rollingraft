/**
 * @file client.h
 * @brief High-level client for RollingRaft cluster
 *
 * Provides automatic leader discovery, request retry, and connection
 * management for interacting with a Raft cluster.
 *
 * Example usage:
 * @code
 *   Client client({"127.0.0.1:8001", "127.0.0.1:8002"});
 *   auto result = client.Execute("set key value");
 *   if (result.ok()) {
 *       std::cout << "Response: " << result.value() << std::endl;
 *   }
 * @endcode
 */

#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "rollingraft/status.h"

namespace rollingraft {

/**
 * Result of a client operation.
 *
 * Similar to std::expected or Rust's Result type.
 * Contains either a successful response or an error.
 */
class ClientResult {
 public:
  /** Create an error result (default invalid state). */
  ClientResult();

  /** Create a successful result. */
  explicit ClientResult(const std::string& response);

  /** Create an error result. */
  explicit ClientResult(Status error);

  /** Check if result is success. */
  bool ok() const { return !error_.has_value(); }

  /** Check if result is error. */
  bool has_error() const { return error_.has_value(); }

  /**
   * Get the response value.
   * @pre ok() must be true
   */
  const std::string& value() const;

  /**
   * Get the error.
   * @pre has_error() must be true
   */
  const Status& error() const;

  /** Get error message string. */
  std::string error_message() const;

 private:
  std::string response_;
  std::optional<Status> error_;
};

/**
 * Configuration options for Client.
 */
struct ClientOptions {
  /** Maximum retry attempts. */
  int max_retries = 3;

  /** Initial retry delay. */
  std::chrono::milliseconds initial_retry_delay{100};

  /** Maximum retry delay. */
  std::chrono::milliseconds max_retry_delay{1000};

  /** Exponential backoff multiplier. */
  double retry_backoff_multiplier = 2.0;

  /** Per-request timeout. */
  std::chrono::milliseconds request_timeout{5000};

  /** Connection timeout. */
  std::chrono::milliseconds connect_timeout{2000};

  /** How long to cache leader address. */
  std::chrono::milliseconds leader_cache_ttl{30000};

  /** Client ID for deduplication (0 = auto-generate). */
  uint64_t client_id = 0;

  /** Maximum async task queue size (0 = unlimited). */
  size_t max_async_queue_size = 10000;
};

/**
 * High-level client for interacting with a Raft cluster.
 *
 * Thread-safety: All methods are thread-safe.
 */
class Client {
 public:
  /**
   * Create client with default options.
   * @param servers List of server addresses (host:port)
   */
  explicit Client(const std::vector<std::string>& servers);

  /**
   * Create client with custom options.
   * @param servers List of server addresses
   * @param options Configuration
   */
  Client(const std::vector<std::string>& servers, const ClientOptions& options);

  ~Client();

  // Non-copyable
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  // Movable
  Client(Client&&) noexcept;
  Client& operator=(Client&&) noexcept;

  /**
   * Execute a command (write operation).
   *
   * @param command Command data for state machine
   * @param timeout Maximum wait time
   * @return Result with response or error
   */
  ClientResult Execute(const std::string& command, std::chrono::milliseconds timeout);

  /** Execute with default timeout. */
  ClientResult Execute(const std::string& command);

  /**
   * Execute a read-only query.
   * @param query Read query
   * @param timeout Maximum wait time
   */
  ClientResult Query(const std::string& query, std::chrono::milliseconds timeout);

  /** Query with default timeout. */
  ClientResult Query(const std::string& query);

  /**
   * Execute asynchronously.
   * @param command Command to execute
   * @param callback Called on completion
   * @param timeout Maximum wait time
   */
  void ExecuteAsync(const std::string& command, std::function<void(ClientResult)> callback,
                    std::chrono::milliseconds timeout);

  /** Async with default timeout. */
  void ExecuteAsync(const std::string& command, std::function<void(ClientResult)> callback);

  /** Force refresh leader cache. */
  void RefreshLeader();

  /** Get current leader address (may be empty). */
  std::string GetLeaderAddr() const;

  /** Check if client can connect to cluster. */
  bool IsHealthy() const;

  /** Get client ID. */
  uint64_t GetClientId() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rollingraft
