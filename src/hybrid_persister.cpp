/**
 * @file hybrid_persister.cpp
 * @brief Hybrid persister facade implementation
 */

#include "rollingraft/hybrid_persister.h"

#include <filesystem>

#include "rollingraft/logger.h"

#include <sys/stat.h>

namespace rollingraft {

HybridPersister::HybridPersister()
    : wal_(std::make_unique<WALPersister>()), state_(std::make_unique<StatePersister>()) {}

HybridPersister::~HybridPersister() { Close(); }

void HybridPersister::SetSyncOnWrite(bool sync) {
  std::lock_guard<std::mutex> lock(mtx_);
  state_->SetSyncOnWrite(sync);
}

void HybridPersister::SetCompressionType(CompressionType type) {
  std::lock_guard<std::mutex> lock(mtx_);
  state_->SetCompressionType(type);
}

Status HybridPersister::Open(const std::string& data_dir) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (opened_) {
    return Status::Error("HybridPersister already open");
  }

  data_dir_ = data_dir;

  // WAL lives in a dedicated subdirectory; StatePersister uses data_dir
  // directly so that existing tests which corrupt .ldb/.log files at the
  // top level continue to work without modification.
  std::string wal_dir = data_dir + "/wal";

  std::error_code ec;
  std::filesystem::create_directories(wal_dir, ec);
  if (ec) {
    return Status::Error("Failed to create wal directory: " + ec.message());
  }

  // Open WAL first and replay to rebuild log index
  auto status = wal_->Open(wal_dir);
  if (!status.ok()) {
    return status;
  }

  // Open StatePersister at data_dir (not a subdirectory)
  status = state_->Open(data_dir);
  if (!status.ok()) {
    wal_->Close();
    return status;
  }

  // Validate consistency: last log index should be >= snapshot last index
  auto [first_log_index, last_log_index] = wal_->GetLogRange();
  uint64_t snapshot_last_index = state_->GetSnapshotLastIndex();

  if (snapshot_last_index > 0 && last_log_index < snapshot_last_index) {
    LOG_WARN(
        "HybridPersister consistency warning: last_log_index ({}) < "
        "snapshot_last_index ({})",
        last_log_index, snapshot_last_index);
    // Not fatal; raft node will handle this during startup
  }

  opened_ = true;
  return Status::OK();
}

void HybridPersister::Close() {
  std::lock_guard<std::mutex> lock(mtx_);

  if (wal_) {
    wal_->Close();
  }
  if (state_) {
    state_->Close();
  }

  opened_ = false;
  data_dir_.clear();
}

Status HybridPersister::SaveState(const PersistentState& state) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  return state_->SaveState(state);
}

Status HybridPersister::LoadState(PersistentState& state) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  return state_->LoadState(state);
}

Status HybridPersister::AppendEntries(const std::vector<RaftLogEntry>& entries) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  for (const auto& entry : entries) {
    auto status = wal_->AppendLogEntry(entry);
    if (!status.ok()) {
      return status;
    }
  }

  return Status::OK();
}

Status HybridPersister::Sync() {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  return wal_->Sync();
}

Status HybridPersister::GetEntries(uint64_t start, uint64_t end, std::vector<RaftLogEntry>* out) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  return wal_->GetEntries(start, end, out);
}

Status HybridPersister::GetEntry(uint64_t index, RaftLogEntry& entry) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  return wal_->GetEntry(index, entry);
}

Status HybridPersister::TruncateSuffix(uint64_t from_index) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  auto [first_index, last_index] = wal_->GetLogRange();
  if (from_index > last_index) {
    return Status::OK();
  }

  return wal_->AppendTruncateSuffix(from_index);
}

Status HybridPersister::TruncatePrefix(uint64_t before_index) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  if (before_index <= 1) {
    return Status::OK();
  }

  return wal_->AppendTruncatePrefix(before_index);
}

std::pair<uint64_t, uint64_t> HybridPersister::GetLastLogInfo() {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return {0, 0};
  }

  auto [first_index, last_index] = wal_->GetLogRange();
  if (last_index == 0) {
    return {0, 0};
  }

  RaftLogEntry entry;
  auto status = wal_->GetEntry(last_index, entry);
  if (!status.ok()) {
    return {0, 0};
  }

  return {last_index, static_cast<uint64_t>(entry.term_)};
}

Status HybridPersister::SaveSnapshot(const std::string& snapshot_data, uint64_t last_index,
                                     uint64_t last_term) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  return state_->SaveSnapshot(snapshot_data, last_index, last_term);
}

Status HybridPersister::LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                                     uint64_t& last_term) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  return state_->LoadSnapshot(snapshot_data, last_index, last_term);
}

Status HybridPersister::SaveSnapshotStream(
    const std::function<bool(std::string& chunk)>& chunk_provider, uint64_t last_index,
    uint64_t last_term) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  return state_->SaveSnapshotStream(chunk_provider, last_index, last_term);
}

Status HybridPersister::LoadSnapshotStream(
    const std::function<void(const std::string& chunk)>& chunk_consumer, uint64_t& last_index,
    uint64_t& last_term) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return Status::Error("HybridPersister not open");
  }

  return state_->LoadSnapshotStream(chunk_consumer, last_index, last_term);
}

bool HybridPersister::HasSnapshot() const {
  std::lock_guard<std::mutex> lock(mtx_);

  if (!opened_) {
    return false;
  }

  return state_->HasSnapshot();
}

std::unique_ptr<Persister> CreateHybridPersister() { return std::make_unique<HybridPersister>(); }

}  // namespace rollingraft
