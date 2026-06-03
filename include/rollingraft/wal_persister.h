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

 private:
  // Segment file format constants
  static constexpr uint32_t kMagic = 0x57414C30;  // "WAL0"
  static constexpr uint16_t kVersion = 1;
  static constexpr size_t kHeaderSize = 16;
  static constexpr size_t kTrailerSize = 8;
  static constexpr size_t kMaxRecordSize = 16 * 1024 * 1024;  // 16MB

  // Segment rotation thresholds
  static constexpr size_t kMaxSegmentEntries = 10000;
  static constexpr size_t kMaxSegmentSize = 64 * 1024 * 1024;  // 64MB

  struct Segment {
    uint64_t id = 0;
    int fd = -1;
    uint64_t end_offset = 0;
    uint64_t entry_count = 0;
  };

  std::string wal_dir_;
  std::string meta_path_;

  mutable std::mutex mtx_;

  // Active segment (protected by mtx_)
  Segment active_segment_;

  // In-memory index: log_index -> (segment_id, offset, length)
  std::map<uint64_t, WALIndexEntry> index_;

  // Current log range
  uint64_t first_index_ = 0;
  uint64_t last_index_ = 0;

  // Internal helpers
  Status OpenSegment(uint64_t segment_id, int* fd);
  Status CreateSegment(uint64_t segment_id);
  Status CloseSegment(Segment* seg);
  Status WriteSegmentHeader(int fd, uint64_t segment_id);
  Status WriteRecord(int fd, WALRecordType type, const std::string& payload,
                     uint64_t* out_offset);
  Status WriteTrailer(int fd, uint64_t end_offset);
  Status ReadTrailer(int fd, uint64_t* end_offset);
  Status ScanSegment(uint64_t segment_id,
                     const std::function<bool(const WALRecord&)>& callback);
  Status RotateSegmentIfNeeded();
  Status LoadMeta();
  Status SaveMeta();
  uint32_t ComputeCRC32(const std::string& data);
};

}  // namespace rollingraft
