/**
 * @file wal_persister.h
 * @brief Write-ahead log persister for Raft log entries
 *
 * Append-only segmented WAL for Raft log entries with CRC32
 * integrity checks, segment rotation, and crash recovery.
 *
 * Phase 1: Standalone component, not yet integrated into HybridPersister.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "rollingraft/raft_log.h"
#include "rollingraft/status.h"
#include "rollingraft/types.h"

namespace rollingraft {

/**
 * WAL record type identifiers.
 */
enum class WALRecordType : uint16_t {
  kLogEntry = 0x01,
  kTruncatePrefix = 0x03,
  kTruncateSuffix = 0x04,
};

/**
 * Single WAL record, exposed for Replay callback.
 */
struct WALRecord {
  WALRecordType type;
  std::string payload;
};

/**
 * In-memory index entry for fast log entry lookup.
 */
struct WALIndexEntry {
  uint64_t segment_id;   // Segment file id
  uint64_t file_offset;  // Offset within segment file
  uint64_t length;       // Record length (including header)
};

/**
 * Write-ahead log persister for Raft log entries.
 *
 * Design:
 * - Append-only segmented WAL files (<wal_dir>/<segment_id>.wal)
 * - CRC32 per record for corruption detection
 * - Trailer per segment for crash recovery
 * - In-memory index rebuilt on Open()
 * - meta.json tracks last segment id and log range
 *
 * Thread-safety: All public methods are thread-safe.
 */
class WALPersister {
 public:
  WALPersister();
  ~WALPersister();

  // Non-copyable, non-movable
  WALPersister(const WALPersister&) = delete;
  WALPersister& operator=(const WALPersister&) = delete;

  /**
   * Open or create WAL in the given directory.
   *
   * Scans existing segments, validates headers/trailers,
   * and rebuilds the in-memory index.
   *
   * @param wal_dir Directory for segment files (e.g. <data_dir>/wal)
   * @return Status::OK() on success
   */
  Status Open(const std::string& wal_dir);

  /** Close WAL and release file handles. */
  void Close();

  /**
   * Append a log entry.
   *
   * Writes to the current active segment. Rotates to a new
   * segment when entry count or size threshold is reached.
   *
   * @param entry Raft log entry to append
   * @return Status::OK() on success
   */
  Status AppendLogEntry(const RaftLogEntry& entry);

  /**
   * Append a truncate prefix record.
   *
   * @param before_index Delete entries before this index
   * @return Status::OK() on success
   */
  Status AppendTruncatePrefix(uint64_t before_index);

  /**
   * Append a truncate suffix record.
   *
   * @param from_index Delete entries from this index onwards
   * @return Status::OK() on success
   */
  Status AppendTruncateSuffix(uint64_t from_index);

  /**
   * Sync the active segment to durable storage.
   *
   * @return Status::OK() on success
   */
  Status Sync();

  /**
   * Replay all records from the beginning.
   *
   * Called during recovery to rebuild in-memory state.
   *
   * @param callback Invoked for each record. Return false to stop.
   * @return Status::OK() on success
   */
  Status Replay(const std::function<bool(const WALRecord&)>& callback);

  /**
   * Garbage collect segments before the given log index.
   *
   * Physically deletes segment files whose entries are all
   * before before_log_index.
   *
   * @param before_log_index Delete segments ending before this index
   * @return Status::OK() on success
   */
  Status GarbageCollect(uint64_t before_log_index);

  /**
   * Get the current log range.
   *
   * @return Pair of (first_index, last_index)
   */
  std::pair<uint64_t, uint64_t> GetLogRange() const;

  /**
   * Read a single log entry by index.
   *
   * @param index Log entry index
   * @param entry Output parameter for the entry
   * @return Status::OK() on success
   */
  Status GetEntry(uint64_t index, RaftLogEntry& entry);

  /**
   * Read log entries in the range [start, end).
   *
   * @param start Start index (inclusive)
   * @param end End index (exclusive)
   * @param out Output vector for entries
   * @return Status::OK() on success
   */
  Status GetEntries(uint64_t start, uint64_t end, std::vector<RaftLogEntry>* out);

 private:
  // Segment file format constants
  static constexpr uint32_t kMagic = 0x57414C30;  // "WAL0"
  static constexpr uint16_t kVersion = 1;
  static constexpr size_t kHeaderSize = 16;
  static constexpr size_t kTrailerSize = 8;
  static constexpr size_t kMaxRecordSize = 16 * 1024 * 1024;  // 16MB

  // Format version for log entry payload serialization
  static constexpr uint16_t kFormatVersionJson = 1;      // JSON + Base64
  static constexpr uint16_t kFormatVersionProtobuf = 2;  // Protobuf + raw bytes

  // Segment rotation thresholds
  static constexpr size_t kMaxSegmentEntries = 10000;
  static constexpr size_t kMaxSegmentSize = 64 * 1024 * 1024;  // 64MB

  // Write buffer threshold. Records are accumulated in memory and flushed
  // to the active segment in a single syscall when the buffer is full or
  // when durability/read consistency is required.
  static constexpr size_t kWriteBufferSize = 1024 * 1024;  // 1MB

  // Checkpoint thresholds
  static constexpr size_t kCheckpointSegmentInterval = 5;
  static constexpr size_t kCheckpointEntryInterval = 50000;

  /**
   * Dense, cache-friendly index for log entry lookups.
   *
   * Log indices in Raft are sequential, so a flat vector indexed by
   * (log_index - first_index_) gives O(1) lookup and contiguous range scans.
   * Non-sequential inserts fall back to binary-search insertion.
   */
  class DenseIndex {
   public:
    bool Empty() const { return entries_.empty(); }
    size_t Size() const { return entries_.size(); }
    uint64_t FirstIndex() const { return first_index_; }
    uint64_t LastIndex() const { return entries_.empty() ? 0 : first_index_ + entries_.size() - 1; }

    const WALIndexEntry* Get(uint64_t index) const {
      if (entries_.empty() || index < first_index_ || index > LastIndex()) {
        return nullptr;
      }
      const WALIndexEntry* e = &entries_[static_cast<size_t>(index - first_index_)];
      // segment_id == 0 means a placeholder slot (gap).
      return e->segment_id == 0 ? nullptr : e;
    }

    void Put(uint64_t index, WALIndexEntry entry);
    void TruncatePrefix(uint64_t before_index);
    void TruncateSuffix(uint64_t from_index);
    void Clear();

    const WALIndexEntry* begin() const { return entries_.data(); }
    const WALIndexEntry* end() const { return entries_.data() + entries_.size(); }

    const std::vector<WALIndexEntry>& Entries() const { return entries_; }

   private:
    uint64_t first_index_ = 0;
    std::vector<WALIndexEntry> entries_;
  };

  struct Segment {
    uint64_t id = 0;
    int fd = -1;
    uint64_t end_offset = 0;
    uint64_t entry_count = 0;
    uint16_t format_version = kFormatVersionProtobuf;
  };

  std::string wal_dir_;
  std::string meta_path_;

  mutable std::mutex mtx_;

  // Active segment (protected by mtx_)
  Segment active_segment_;

  // In-memory index: dense vector keyed by log_index
  DenseIndex index_;

  // Segment id -> format version (protected by mtx_)
  std::map<uint64_t, uint16_t> segment_format_versions_;

  // Cached read fd (protected by mtx_)
  int cached_fd_ = -1;
  uint64_t cached_segment_id_ = 0;

  // Write buffer (protected by mtx_). Records are appended here and flushed
  // to the active segment in batches.
  std::string write_buf_;

  // Internal helpers
  // Get an fd for a segment, caching it across reads so GetEntry/GetEntries
  // don't pay open()+header-validate+close() per call. The cache is
  // invalidated on GarbageCollect (segment deleted) and Close.
  Status GetCachedSegmentFd(uint64_t segment_id, int* fd);
  void InvalidateSegmentFdCache();
  Status OpenSegment(uint64_t segment_id, int* fd);
  Status CreateSegment(uint64_t segment_id);
  Status CloseSegment(Segment* seg);
  Status ReadLogEntryAt(uint64_t segment_id, uint64_t file_offset, RaftLogEntry& entry);
  Status WriteSegmentHeader(int fd, uint64_t segment_id,
                            uint16_t format_version = kFormatVersionProtobuf);
  Status WriteRecord(int fd, WALRecordType type, const std::string& payload, uint64_t* out_offset);
  Status WriteTrailer(int fd, uint64_t end_offset);
  Status ReadTrailer(int fd, uint64_t* end_offset);
  Status ScanSegment(uint64_t segment_id, const std::function<bool(const WALRecord&)>& callback,
                     uint64_t* out_truncate_offset = nullptr);
  // Physically truncate a segment file at the given offset and rewrite its
  // trailer (corruption recovery).
  Status TruncateSegmentFile(uint64_t segment_id, uint64_t truncate_offset);
  Status RotateSegmentIfNeeded();
  Status LoadMeta();
  Status SaveMeta();
  uint32_t ComputeCRC32(const std::string& data);

  // Checkpoint helpers
  Status LoadLatestCheckpointLocked(uint64_t* out_last_covered_segment_id);
  Status SaveCheckpointLocked();
  void RemoveOldCheckpointsLocked(uint64_t first_retained_segment_id);
  std::vector<std::string> ListCheckpointFilesLocked() const;
  static std::string CheckpointPathFor(const std::string& wal_dir, uint64_t segment_id);
  bool ShouldCreateCheckpointLocked() const;

  // Buffering helpers (protected by mtx_)
  Status FlushWriteBufferLocked();
  Status EnsureBufferFlushedForReadLocked();
  Status AppendRecordToBufferLocked(WALRecordType type, const std::string& payload,
                                    uint64_t* out_offset, uint64_t* out_record_len);
  Status SyncActiveSegmentLocked();
};

}  // namespace rollingraft
