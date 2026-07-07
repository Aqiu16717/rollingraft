#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "rollingraft/persister.h"
#include "rollingraft/status.h"

namespace rollingraft {

/**
 * @brief Persister adapter that isolates multiple Raft groups in one store.
 *
 * Each group gets its own sub-directory under the store's data directory.
 * This provides complete key/file isolation without requiring invasive
 * changes to the underlying persister (e.g. HybridPersister).  A future
 * optimization can collapse the sub-directories into a single LevelDB
 * instance with group-prefixed keys.
 */
class MultiRaftPersister : public Persister {
 public:
  /**
   * @param group_id Target group identifier.
   * @param base_data_dir Store-level data directory (e.g. RaftStoreConfig::data_dir).
   * @param inner_factory Optional factory for the underlying persister.  If null,
   *        CreateLevelDBPersister() is used.
   */
  MultiRaftPersister(uint64_t group_id, std::string base_data_dir,
                     std::function<std::unique_ptr<Persister>()> inner_factory = nullptr);
  ~MultiRaftPersister() override;

  // Not copyable / movable.
  MultiRaftPersister(const MultiRaftPersister&) = delete;
  MultiRaftPersister& operator=(const MultiRaftPersister&) = delete;

  Status Open(const std::string& /*data_dir*/) override;
  void Close() override;

  Status SaveState(const PersistentState& state) override;
  Status LoadState(PersistentState& state) override;

  Status AppendEntries(const std::vector<RaftLogEntry>& entries) override;
  Status Sync() override;
  Status GetEntries(uint64_t start, uint64_t end, std::vector<RaftLogEntry>* out) override;
  Status GetEntry(uint64_t index, RaftLogEntry& entry) override;
  Status TruncateSuffix(uint64_t from_index) override;
  Status TruncatePrefix(uint64_t before_index) override;
  std::pair<uint64_t, uint64_t> GetLastLogInfo() override;

  Status SaveSnapshot(const std::string& snapshot_data, uint64_t last_index,
                      uint64_t last_term) override;
  Status LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                      uint64_t& last_term) override;
  Status SaveSnapshotStream(const std::function<bool(std::string& chunk)>& chunk_provider,
                            uint64_t last_index, uint64_t last_term) override;
  Status LoadSnapshotStream(const std::function<void(const std::string& chunk)>& chunk_consumer,
                            uint64_t& last_index, uint64_t& last_term) override;
  bool HasSnapshot() const override;

  void SetSyncOnWrite(bool sync) override;
  void SetCompressionType(CompressionType type) override;

 private:
  std::string GroupDataDir() const;

  uint64_t group_id_;
  std::string base_data_dir_;
  std::function<std::unique_ptr<Persister>()> inner_factory_;
  std::unique_ptr<Persister> inner_;
  bool opened_ = false;

  // Configuration applied lazily when the inner persister is created.
  std::optional<bool> sync_on_write_;
  CompressionType compression_type_ = kNoCompression;
};

}  // namespace rollingraft
