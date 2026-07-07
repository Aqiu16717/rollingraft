#include "multi_raft_persister.h"

#include <filesystem>

#include "rollingraft/logger.h"

namespace rollingraft {

MultiRaftPersister::MultiRaftPersister(uint64_t group_id, std::string base_data_dir,
                                       std::function<std::unique_ptr<Persister>()> inner_factory)
    : group_id_(group_id),
      base_data_dir_(std::move(base_data_dir)),
      inner_factory_(std::move(inner_factory)) {
  if (!inner_factory_) {
    inner_factory_ = []() { return CreateLevelDBPersister(); };
  }
}

MultiRaftPersister::~MultiRaftPersister() {
  if (inner_) {
    inner_->Close();
  }
}

std::string MultiRaftPersister::GroupDataDir() const {
  return base_data_dir_ + "/groups/" + std::to_string(group_id_);
}

Status MultiRaftPersister::Open(const std::string& /*data_dir*/) {
  if (opened_) {
    return Status::OK();
  }
  std::string dir = GroupDataDir();
  std::filesystem::create_directories(dir);
  inner_ = inner_factory_();
  inner_->SetCompressionType(compression_type_);
  if (sync_on_write_.has_value()) {
    inner_->SetSyncOnWrite(*sync_on_write_);
  }
  auto status = inner_->Open(dir);
  if (status.ok()) {
    opened_ = true;
  }
  return status;
}

void MultiRaftPersister::Close() {
  if (inner_) {
    inner_->Close();
  }
  opened_ = false;
}

Status MultiRaftPersister::SaveState(const PersistentState& state) {
  return inner_->SaveState(state);
}

Status MultiRaftPersister::LoadState(PersistentState& state) {
  return inner_->LoadState(state);
}

Status MultiRaftPersister::AppendEntries(const std::vector<RaftLogEntry>& entries) {
  return inner_->AppendEntries(entries);
}

Status MultiRaftPersister::Sync() {
  return inner_->Sync();
}

Status MultiRaftPersister::GetEntries(uint64_t start, uint64_t end,
                                      std::vector<RaftLogEntry>* out) {
  return inner_->GetEntries(start, end, out);
}

Status MultiRaftPersister::GetEntry(uint64_t index, RaftLogEntry& entry) {
  return inner_->GetEntry(index, entry);
}

Status MultiRaftPersister::TruncateSuffix(uint64_t from_index) {
  return inner_->TruncateSuffix(from_index);
}

Status MultiRaftPersister::TruncatePrefix(uint64_t before_index) {
  return inner_->TruncatePrefix(before_index);
}

std::pair<uint64_t, uint64_t> MultiRaftPersister::GetLastLogInfo() {
  return inner_->GetLastLogInfo();
}

Status MultiRaftPersister::SaveSnapshot(const std::string& snapshot_data, uint64_t last_index,
                                        uint64_t last_term) {
  return inner_->SaveSnapshot(snapshot_data, last_index, last_term);
}

Status MultiRaftPersister::LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                                        uint64_t& last_term) {
  return inner_->LoadSnapshot(snapshot_data, last_index, last_term);
}

Status MultiRaftPersister::SaveSnapshotStream(
    const std::function<bool(std::string& chunk)>& chunk_provider, uint64_t last_index,
    uint64_t last_term) {
  return inner_->SaveSnapshotStream(chunk_provider, last_index, last_term);
}

Status MultiRaftPersister::LoadSnapshotStream(
    const std::function<void(const std::string& chunk)>& chunk_consumer, uint64_t& last_index,
    uint64_t& last_term) {
  return inner_->LoadSnapshotStream(chunk_consumer, last_index, last_term);
}

bool MultiRaftPersister::HasSnapshot() const {
  return inner_->HasSnapshot();
}

void MultiRaftPersister::SetSyncOnWrite(bool sync) {
  sync_on_write_ = sync;
  if (inner_) {
    inner_->SetSyncOnWrite(sync);
  }
}

void MultiRaftPersister::SetCompressionType(CompressionType type) {
  compression_type_ = type;
  if (inner_) {
    inner_->SetCompressionType(type);
  }
}

}  // namespace rollingraft
