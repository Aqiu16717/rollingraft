/**
 * @file runtime_config.h
 * @brief Thread-safe runtime configuration with hot reload support
 *
 * Provides atomic reads and validated updates for Raft tuning parameters.
 * Enables agents to auto-tune cluster behavior without restarts.
 */

#pragma once

#include <cstdint>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "rollingraft/status.h"

namespace rollingraft {

/**
 * Runtime-configurable Raft parameters.
 *
 * All values have validated ranges. Updates are atomic and
 * take effect on the next timer reset.
 */
struct RuntimeConfigValues {
  uint32_t election_timeout_ms = 300;
  uint32_t heartbeat_interval_ms = 50;
  uint32_t max_entries_per_append = 100;
  uint32_t rpc_timeout_ms = 500;
  uint32_t snapshot_threshold_entries = 10000;
  uint32_t snapshot_threshold_bytes = 10 * 1024 * 1024;
  uint32_t snapshot_check_interval_ms = 5000;
  uint32_t max_retry_attempts = 5;
  uint32_t base_retry_delay_ms = 10;
  uint32_t max_retry_delay_ms = 500;
  uint32_t log_retention_entries = 0;
  uint32_t max_snapshot_size_bytes = 100 * 1024 * 1024;  // 100MB
  uint32_t propose_timeout_ms = 5000;
};

/**
 * Thread-safe runtime configuration store.
 *
 * Supports atomic reads and validated partial updates.
 * All parameters have min/max bounds and cross-parameter constraints.
 */
class RuntimeConfig {
 public:
  using Values = RuntimeConfigValues;

  /** Initialize with default values. */
  RuntimeConfig();

  /** Initialize from a config struct. */
  explicit RuntimeConfig(const Values& defaults);

  /** Get a snapshot of current values (thread-safe). */
  Values Get() const;

  /**
   * Update parameters from a JSON string.
   *
   * Validates range and cross-parameter constraints before applying.
   * Either all requested changes apply, or none do.
   *
   * @param json_str JSON object with parameter names as keys
   * @return Status::OK() on success, error with details on failure
   */
  Status UpdateFromJson(const std::string& json_str);

  /** Reset all parameters to defaults. */
  void Reset();

  /** Serialize current values to JSON (thread-safe). */
  std::string ToJson() const;

  /**
   * Validate runtime configuration values.
   * @param v Values to validate
   * @return Status::OK() if valid, error with details otherwise
   */
  Status Validate(const Values& v) const;

 private:
  mutable std::shared_mutex mtx_;
  Values values_;
  Values defaults_;
};

}  // namespace rollingraft
