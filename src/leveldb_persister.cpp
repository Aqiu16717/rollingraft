#include "rollingraft/persister.h"

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include <cstring>
#include <shared_mutex>

#include "rollingraft/logger.h"

namespace rollingraft {

// Key prefixes for different data types
constexpr char kStateKey[] = "state";
constexpr char kLogPrefix[] = "log:";
constexpr char kSnapshotKey[] = "snapshot";
constexpr char kSnapshotMetaKey[] = "snapshot_meta";

class LevelDBPersister : public Persister {
 public:
  LevelDBPersister() = default;
  ~LevelDBPersister() override { Close(); }

  Status Open(const std::string& data_dir) override {
    std::unique_lock lock(mutex_);

    if (db_ != nullptr) {
      return Status::Error("Persister already open");
    }

    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::DB* db_ptr = nullptr;
    leveldb::Status status = leveldb::DB::Open(options, data_dir, &db_ptr);

    if (!status.ok()) {
      return Status::Error("Failed to open LevelDB: " + status.ToString());
    }

    db_.reset(db_ptr);

    // Load cached state
    LoadStateFromDB();

    return Status::OK();
  }

  void Close() override {
    std::unique_lock lock(mutex_);
    db_.reset();
  }

  Status SaveState(const PersistentState& state) override {
    std::unique_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    // Serialize: term (8 bytes) + voted_for (8 bytes)
    char buffer[16];
    std::memcpy(buffer, &state.current_term, sizeof(state.current_term));
    std::memcpy(buffer + 8, &state.voted_for, sizeof(state.voted_for));

    leveldb::Slice value(buffer, sizeof(buffer));
    leveldb::Status s = db_->Put(leveldb::WriteOptions(), kStateKey, value);

    if (!s.ok()) {
      return Status::Error("Failed to save state: " + s.ToString());
    }

    cached_state_ = state;
    return Status::OK();
  }

  Status LoadState(PersistentState& state) override {
    std::shared_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    state = cached_state_;
    return Status::OK();
  }

  Status AppendEntries(const std::vector<RaftLogEntry>& entries) override {
    std::unique_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    if (entries.empty()) {
      return Status::OK();
    }

    leveldb::WriteBatch batch;

    for (const auto& entry : entries) {
      std::string key = MakeLogKey(entry.index_);
      std::string value = SerializeEntry(entry);
      batch.Put(key, value);
    }

    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
      return Status::Error("Failed to append entries: " + s.ToString());
    }

    return Status::OK();
  }

  Status GetEntries(uint64_t start, uint64_t end,
                    std::vector<RaftLogEntry>* out) override {
    std::shared_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    out->clear();

    if (start >= end) {
      return Status::OK();
    }

    // LevelDB 是有序的，我们可以使用迭代器
    std::string start_key = MakeLogKey(start);
    std::string end_key = MakeLogKey(end);

    std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(start_key); it->Valid() && it->key().ToString() < end_key;
         it->Next()) {
      RaftLogEntry entry;
      if (DeserializeEntry(it->value(), entry)) {
        out->push_back(std::move(entry));
      }
    }

    if (!it->status().ok()) {
      return Status::Error("Failed to read entries: " + it->status().ToString());
    }

    return Status::OK();
  }

  Status GetEntry(uint64_t index, RaftLogEntry& entry) override {
    std::shared_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    std::string key = MakeLogKey(index);
    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &value);

    if (s.IsNotFound()) {
      return Status::Error("Entry not found");
    }
    if (!s.ok()) {
      return Status::Error("Failed to get entry: " + s.ToString());
    }

    if (!DeserializeEntry(value, entry)) {
      return Status::Error("Failed to deserialize entry");
    }

    return Status::OK();
  }

  Status TruncateSuffix(uint64_t from_index) override {
    std::unique_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    // 获取最后一个日志索引
    auto [last_index, _] = GetLastLogInfoLocked();

    if (from_index > last_index) {
      return Status::OK();
    }

    leveldb::WriteBatch batch;

    // 删除 [from_index, last_index] 范围内的所有条目
    for (uint64_t i = from_index; i <= last_index; ++i) {
      batch.Delete(MakeLogKey(i));
    }

    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
      return Status::Error("Failed to truncate suffix: " + s.ToString());
    }

    return Status::OK();
  }

  Status TruncatePrefix(uint64_t before_index) override {
    std::unique_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    if (before_index <= 1) {
      return Status::OK();
    }

    leveldb::WriteBatch batch;

    // 删除 [1, before_index) 范围内的所有条目
    for (uint64_t i = 1; i < before_index; ++i) {
      batch.Delete(MakeLogKey(i));
    }

    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
      return Status::Error("Failed to truncate prefix: " + s.ToString());
    }

    return Status::OK();
  }

  std::pair<uint64_t, uint64_t> GetLastLogInfo() override {
    std::shared_lock lock(mutex_);
    return GetLastLogInfoLocked();
  }

  Status SaveSnapshot(const std::string& snapshot_data, uint64_t last_index,
                      uint64_t last_term) override {
    std::unique_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    leveldb::WriteBatch batch;

    // Save snapshot data
    batch.Put(kSnapshotKey, snapshot_data);

    // Save metadata: last_index (8 bytes) + last_term (8 bytes)
    char meta[16];
    std::memcpy(meta, &last_index, sizeof(last_index));
    std::memcpy(meta + 8, &last_term, sizeof(last_term));
    batch.Put(kSnapshotMetaKey, leveldb::Slice(meta, sizeof(meta)));

    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
      return Status::Error("Failed to save snapshot: " + s.ToString());
    }

    snapshot_last_index_ = last_index;
    snapshot_last_term_ = last_term;

    return Status::OK();
  }

  Status LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                      uint64_t& last_term) override {
    std::shared_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    // Load snapshot data
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), kSnapshotKey, &snapshot_data);
    if (s.IsNotFound()) {
      return Status::Error("No snapshot available");
    }
    if (!s.ok()) {
      return Status::Error("Failed to load snapshot: " + s.ToString());
    }

    // Load metadata
    std::string meta;
    s = db_->Get(leveldb::ReadOptions(), kSnapshotMetaKey, &meta);
    if (!s.ok() || meta.size() != 16) {
      return Status::Error("Failed to load snapshot metadata");
    }

    std::memcpy(&last_index, meta.data(), sizeof(last_index));
    std::memcpy(&last_term, meta.data() + 8, sizeof(last_term));

    return Status::OK();
  }

  bool HasSnapshot() const override {
    std::shared_lock lock(mutex_);
    if (!db_) return false;

    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), kSnapshotKey, &value);
    return s.ok();
  }

 private:
  void LoadStateFromDB() {
    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), kStateKey, &value);

    if (s.ok() && value.size() == 16) {
      std::memcpy(&cached_state_.current_term, value.data(), sizeof(cached_state_.current_term));
      std::memcpy(&cached_state_.voted_for, value.data() + 8, sizeof(cached_state_.voted_for));
    }
    // If not found or error, use default values (0, -1)
  }

  std::pair<uint64_t, uint64_t> GetLastLogInfoLocked() {
    if (!db_) {
      return {0, 0};
    }

    // 找到最后一个日志条目
    std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(leveldb::ReadOptions()));

    // 定位到 log prefix 范围的末尾
    std::string prefix = kLogPrefix;
    std::string limit = prefix;
    limit.back()++;  // log: -> log;

    it->Seek(limit);
    if (it->Valid()) {
      it->Prev();
    } else {
      it->SeekToLast();
    }

    if (!it->Valid() || !it->key().starts_with(prefix)) {
      return {0, 0};
    }

    RaftLogEntry entry;
    if (DeserializeEntry(it->value(), entry)) {
      return {entry.index_, entry.term_};
    }

    return {0, 0};
  }

  std::string MakeLogKey(uint64_t index) {
    // Format: "log:{index:016x}" (16 hex digits for fixed width sorting)
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%s%016llx", kLogPrefix,
                  static_cast<unsigned long long>(index));
    return std::string(buf);
  }

  std::string SerializeEntry(const RaftLogEntry& entry) {
    // Format: index (8) + term (4) + data_len (4) + data
    std::string result;
    result.reserve(16 + entry.data_.size());

    uint32_t term = entry.term_;
    uint32_t data_len = static_cast<uint32_t>(entry.data_.size());

    result.append(reinterpret_cast<const char*>(&entry.index_), sizeof(entry.index_));
    result.append(reinterpret_cast<const char*>(&term), sizeof(term));
    result.append(reinterpret_cast<const char*>(&data_len), sizeof(data_len));
    result.append(entry.data_);

    return result;
  }

  bool DeserializeEntry(const leveldb::Slice& slice, RaftLogEntry& entry) {
    if (slice.size() < 16) {
      return false;
    }

    const char* data = slice.data();
    uint32_t term;
    uint32_t data_len;

    std::memcpy(&entry.index_, data, sizeof(entry.index_));
    std::memcpy(&term, data + 8, sizeof(term));
    std::memcpy(&data_len, data + 12, sizeof(data_len));

    if (slice.size() != 16 + data_len) {
      return false;
    }

    entry.term_ = term;
    entry.data_.assign(data + 16, data_len);

    return true;
  }

 private:
  mutable std::shared_mutex mutex_;
  std::unique_ptr<leveldb::DB> db_;
  PersistentState cached_state_;

  // Cached snapshot info
  uint64_t snapshot_last_index_ = 0;
  uint64_t snapshot_last_term_ = 0;
};

// Factory function implementation
std::unique_ptr<Persister> CreateLevelDBPersister() {
  return std::make_unique<LevelDBPersister>();
}

}  // namespace rollingraft
