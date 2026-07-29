#include "rollingraft/runtime_config.h"

#include <mutex>

#include <nlohmann/json.hpp>

namespace rollingraft {

RuntimeConfig::RuntimeConfig() : values_(Values{}), defaults_(Values{}) {}

RuntimeConfig::RuntimeConfig(const Values& defaults) : values_(defaults), defaults_(defaults) {}

RuntimeConfig::Values RuntimeConfig::Get() const {
  std::shared_lock<std::shared_mutex> lock(mtx_);
  return values_;
}

Status RuntimeConfig::UpdateFromJson(const std::string& json_str) {
  nlohmann::json j;
  try {
    j = nlohmann::json::parse(json_str);
  } catch (const std::exception& e) {
    return Status::Error("INVALID_JSON", e.what());
  }

  std::unique_lock<std::shared_mutex> lock(mtx_);
  Values proposed = values_;

  // Apply each field if present
  if (j.contains("election_timeout_ms")) {
    proposed.election_timeout_ms = j["election_timeout_ms"];
  }
  if (j.contains("heartbeat_interval_ms")) {
    proposed.heartbeat_interval_ms = j["heartbeat_interval_ms"];
  }
  if (j.contains("max_entries_per_append")) {
    proposed.max_entries_per_append = j["max_entries_per_append"];
  }
  if (j.contains("rpc_timeout_ms")) {
    proposed.rpc_timeout_ms = j["rpc_timeout_ms"];
  }
  if (j.contains("snapshot_threshold_entries")) {
    proposed.snapshot_threshold_entries = j["snapshot_threshold_entries"];
  }
  if (j.contains("snapshot_threshold_bytes")) {
    proposed.snapshot_threshold_bytes = j["snapshot_threshold_bytes"];
  }
  if (j.contains("snapshot_check_interval_ms")) {
    proposed.snapshot_check_interval_ms = j["snapshot_check_interval_ms"];
  }
  if (j.contains("max_retry_attempts")) {
    proposed.max_retry_attempts = j["max_retry_attempts"];
  }
  if (j.contains("base_retry_delay_ms")) {
    proposed.base_retry_delay_ms = j["base_retry_delay_ms"];
  }
  if (j.contains("max_retry_delay_ms")) {
    proposed.max_retry_delay_ms = j["max_retry_delay_ms"];
  }
  if (j.contains("log_retention_entries")) {
    proposed.log_retention_entries = j["log_retention_entries"];
  }
  if (j.contains("max_snapshot_size_bytes")) {
    proposed.max_snapshot_size_bytes = j["max_snapshot_size_bytes"];
  }
  if (j.contains("propose_timeout_ms")) {
    proposed.propose_timeout_ms = j["propose_timeout_ms"];
  }
  if (j.contains("leader_lease_enabled")) {
    proposed.leader_lease_enabled = j["leader_lease_enabled"];
  }
  if (j.contains("max_pipeline_window")) {
    proposed.max_pipeline_window = j["max_pipeline_window"];
  }
  if (j.contains("transport_batching_enabled")) {
    proposed.transport_batching_enabled = j["transport_batching_enabled"];
  }

  auto status = Validate(proposed);
  if (!status.ok()) {
    return status;
  }

  values_ = proposed;
  return Status::OK();
}

void RuntimeConfig::Reset() {
  std::unique_lock<std::shared_mutex> lock(mtx_);
  values_ = defaults_;
}

std::string RuntimeConfig::ToJson() const {
  std::shared_lock<std::shared_mutex> lock(mtx_);
  nlohmann::json j;
  j["election_timeout_ms"] = values_.election_timeout_ms;
  j["heartbeat_interval_ms"] = values_.heartbeat_interval_ms;
  j["max_entries_per_append"] = values_.max_entries_per_append;
  j["rpc_timeout_ms"] = values_.rpc_timeout_ms;
  j["snapshot_threshold_entries"] = values_.snapshot_threshold_entries;
  j["snapshot_threshold_bytes"] = values_.snapshot_threshold_bytes;
  j["snapshot_check_interval_ms"] = values_.snapshot_check_interval_ms;
  j["max_retry_attempts"] = values_.max_retry_attempts;
  j["base_retry_delay_ms"] = values_.base_retry_delay_ms;
  j["max_retry_delay_ms"] = values_.max_retry_delay_ms;
  j["log_retention_entries"] = values_.log_retention_entries;
  j["max_snapshot_size_bytes"] = values_.max_snapshot_size_bytes;
  j["propose_timeout_ms"] = values_.propose_timeout_ms;
  j["leader_lease_enabled"] = values_.leader_lease_enabled;
  j["max_pipeline_window"] = values_.max_pipeline_window;
  j["transport_batching_enabled"] = values_.transport_batching_enabled;
  return j.dump(2);
}

Status RuntimeConfig::Validate(const Values& v) {
  // Range checks
  if (v.election_timeout_ms < 50 || v.election_timeout_ms > 5000) {
    return Status::Error("INVALID_CONFIG", "election_timeout_ms must be in [50, 5000]");
  }
  if (v.heartbeat_interval_ms < 10 || v.heartbeat_interval_ms > 1000) {
    return Status::Error("INVALID_CONFIG", "heartbeat_interval_ms must be in [10, 1000]");
  }
  if (v.max_entries_per_append < 1 || v.max_entries_per_append > 10000) {
    return Status::Error("INVALID_CONFIG", "max_entries_per_append must be in [1, 10000]");
  }
  if (v.rpc_timeout_ms < 100 || v.rpc_timeout_ms > 10000) {
    return Status::Error("INVALID_CONFIG", "rpc_timeout_ms must be in [100, 10000]");
  }
  if (v.snapshot_threshold_entries < 100 || v.snapshot_threshold_entries > 1000000) {
    return Status::Error("INVALID_CONFIG", "snapshot_threshold_entries must be in [100, 1000000]");
  }
  if (v.snapshot_threshold_bytes < 1 * 1024 * 1024 ||
      v.snapshot_threshold_bytes > 1 * 1024 * 1024 * 1024) {
    return Status::Error("INVALID_CONFIG", "snapshot_threshold_bytes must be in [1MB, 1GB]");
  }
  if (v.snapshot_check_interval_ms < 1000 || v.snapshot_check_interval_ms > 60000) {
    return Status::Error("INVALID_CONFIG", "snapshot_check_interval_ms must be in [1000, 60000]");
  }
  if (v.max_retry_attempts < 1 || v.max_retry_attempts > 100) {
    return Status::Error("INVALID_CONFIG", "max_retry_attempts must be in [1, 100]");
  }
  if (v.base_retry_delay_ms < 1 || v.base_retry_delay_ms > 1000) {
    return Status::Error("INVALID_CONFIG", "base_retry_delay_ms must be in [1, 1000]");
  }
  if (v.max_retry_delay_ms < 10 || v.max_retry_delay_ms > 10000) {
    return Status::Error("INVALID_CONFIG", "max_retry_delay_ms must be in [10, 10000]");
  }
  if (v.log_retention_entries > 100000) {
    return Status::Error("INVALID_CONFIG", "log_retention_entries must be <= 100000");
  }
  if (v.max_snapshot_size_bytes > 0 && v.max_snapshot_size_bytes < 1 * 1024 * 1024) {
    return Status::Error("INVALID_CONFIG",
                         "max_snapshot_size_bytes must be 0 (unlimited) or >= 1MB");
  }
  if (v.propose_timeout_ms < 100 || v.propose_timeout_ms > 60000) {
    return Status::Error("INVALID_CONFIG", "propose_timeout_ms must be in [100, 60000]");
  }
  if (v.max_pipeline_window < 1 || v.max_pipeline_window > 10000) {
    return Status::Error("INVALID_CONFIG", "max_pipeline_window must be in [1, 10000]");
  }

  // Cross-parameter checks
  if (v.heartbeat_interval_ms >= v.election_timeout_ms) {
    return Status::Error("INVALID_CONFIG", "heartbeat_interval_ms must be < election_timeout_ms");
  }
  if (v.base_retry_delay_ms > v.max_retry_delay_ms) {
    return Status::Error("INVALID_CONFIG", "base_retry_delay_ms must be <= max_retry_delay_ms");
  }
  if (v.rpc_timeout_ms < v.heartbeat_interval_ms) {
    return Status::Error("INVALID_CONFIG", "rpc_timeout_ms must be >= heartbeat_interval_ms");
  }

  return Status::OK();
}

}  // namespace rollingraft
