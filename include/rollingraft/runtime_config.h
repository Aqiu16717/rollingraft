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
};

/**
 * Thread-safe runtime configuration store.
 *
 * Supports atomic reads and validated partial updates.
 * All parameters have min/max bounds and cross-parameter constraints.
 */
class RuntimeConfig {
 public:
  /**
   * Parameter metadata: min, max, default.
   */
  struct ParamMeta {
    uint32_t min;
    uint32_t max;
    uint32_t default_value;
  };

  /** Initialize with default values. */
  RuntimeConfig();

  /** Initialize from a config struct. */
  explicit RuntimeConfig(const RuntimeConfigValues& values);

  /** Get a snapshot of current values (thread-safe). */
  RuntimeConfigValues Get() const;

  /**
   * Update specific parameters atomically.
   *
   * Validates range and cross-parameter constraints before applying.
   * Either all requested changes apply, or none do.
   *
   * @param updates Map of parameter name -> new value
   * @return Status::OK() on success, error with details on failure
   */
  Status Update(
      const std::unordered_map<std::string, uint32_t>& updates);

  /** Reset all parameters to defaults. */
  void Reset();

  /** Get metadata for all parameters. */
  static std::unordered_map<std::string, ParamMeta> GetMetadata();

 private:
  mutable std::shared_mutex mtx_;
  RuntimeConfigValues values_;
  RuntimeConfigValues defaults_;

  Status ValidateLocked(
      const std::unordered_map<std::string, uint32_t>& updates) const;
  static bool IsValidParam(const std::string& name);
};

}  // namespace rollingraft
