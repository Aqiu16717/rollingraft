# WAL Separation Design Draft

## Status
Draft v0.1 — 2026-06-03

## Problem Statement

Current `LevelDBPersister` stores Raft log entries, persistent state, and snapshots in a single LevelDB instance. This creates several performance and operational issues:

### 1. Write Amplification

LevelDB is an LSM-tree engine. While it provides excellent sequential write performance for memtable flushes, background **compaction** generates significant write amplification:

- Each log entry write triggers compaction of overlapping SST files
- For a sustained write workload (~800 ops/sec with 10 clients), compaction can multiply writes by 3-10×
- The problem worsens as the dataset grows (more levels → more compaction work)

### 2. Mixed Workload Interference

Log entries (small, append-only, high-frequency) and snapshots (large, infrequent, overwrite) share the same LevelDB instance:

- Snapshot writes (SaveSnapshotStream) trigger major compactions that stall log writes
- Log entry compaction competes with snapshot data for I/O bandwidth
- TruncatePrefix/TruncateSuffix operations create tombstones that increase read amplification

### 3. Recovery Complexity

LevelDB recovery replays its internal WAL from the last checkpoint. For Raft, this means:
- We rely on LevelDB's internal recovery, which is opaque to Raft semantics
- We cannot implement Raft-specific recovery optimizations (e.g., replay from a specific index)
- Corruption in the snapshot key space can affect log entry retrieval

## Goals

1. **Reduce write amplification**: Separate append-only log WAL from state machine storage
2. **Eliminate workload interference**: Log writes and snapshot writes use independent storage paths
3. **Improve recovery**: Raft-level control over WAL replay, with clear recovery semantics
4. **Maintain backward compatibility**: Existing Persister interface continues to work

## Non-Goals

1. Replace LevelDB entirely — it remains suitable for snapshot/metadata storage
2. Add distributed storage support — single-node persistent storage only
3. Change Raft protocol semantics — purely an implementation optimization

## Design Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     RollingRaft Node                         │
├─────────────────────────────────────────────────────────────┤
│  RaftNodeCore                                                │
│     │                                                        │
│     ▼                                                        │
│  ┌──────────────────────┐    ┌──────────────────────────┐  │
│  │   LogPersister       │    │   SnapshotManager        │  │
│  │  (batch + group      │    │   (streaming chunks)     │  │
│  │   commit, existing)  │    │                          │  │
│  └──────────┬───────────┘    └────────────┬─────────────┘  │
│             │                              │                │
│     ┌───────▼────────┐          ┌──────────▼─────────┐     │
│     │  WALPersister  │          │  StatePersister    │     │
│     │  (NEW)         │          │  (refactored       │     │
│     │                │          │   LevelDBPersister)│     │
│     └───────┬────────┘          └──────────┬─────────┘     │
│             │                              │                │
│     ┌───────▼────────┐          ┌──────────▼─────────┐     │
│     │  append-only   │          │  LevelDB / SQLite  │     │
│     │  WAL file(s)   │          │  (metadata +       │     │
│     │                │          │   snapshot chunks) │     │
│     └────────────────┘          └────────────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

## Component Design

### 1. WALPersister (New)

Responsible for append-only write-ahead logging of Raft log entries.

#### File Format

```
WAL File: <data_dir>/wal/<segment_id>.wal

Segment layout:
┌─────────────┬─────────────┬──────────────────────────────┐
│  Header     │  Records    │  Trailer                     │
│  (32 bytes) │  (variable) │  (8 bytes)                   │
└─────────────┴─────────────┴──────────────────────────────┘

Header:
  - magic:        8 bytes  (0x5241465457414C31 = "RAFTWAL1")
  - version:      4 bytes  (uint32_t, currently 1)
  - segment_id:   8 bytes  (uint64_t, monotonic)
  - reserved:     12 bytes (padding for alignment)

Record:
  ┌─────────────┬─────────────┬─────────────┬──────────────┐
  │ crc32       │ length      │ type        │ payload      │
  │ (4 bytes)   │ (4 bytes)   │ (2 bytes)   │ (length)     │
  └─────────────┴─────────────┴─────────────┴──────────────┘

Record types:
  - 0x01: LOG_ENTRY (RaftLogEntry serialized)
  - 0x02: STATE (PersistentState serialized)
  - 0x03: TRUNCATE_PREFIX (index before which entries are deleted)
  - 0x04: TRUNCATE_SUFFIX (index from which entries are deleted)

Trailer:
  - end_offset:   8 bytes (uint64_t, offset of last valid record)
```

#### Key Properties

- **Append-only**: Never modify existing records; only append new ones
- **Segmented**: Rotate to new file when current segment reaches configurable size (default 64MB)
- **CRC32 per record**: Detects corruption at record granularity
- **Trailer for crash recovery**: On open, scan backward from trailer to find last valid record
- **No compaction**: Unlike LSM-tree, no background rewrite of old data
- **fsync on Sync()**: Only fsync when explicitly called (group commit manages this)

#### Interface

```cpp
class WALPersister {
 public:
  Status Open(const std::string& data_dir);
  void Close();

  // Append records (in-memory buffer, flushed by Sync())
  Status AppendLogEntry(const RaftLogEntry& entry);
  Status AppendState(const PersistentState& state);
  Status AppendTruncatePrefix(uint64_t before_index);
  Status AppendTruncateSuffix(uint64_t from_index);

  // Durability barrier
  Status Sync();

  // Recovery: replay all records from beginning
  Status Replay(const std::function<bool(const WALRecord&)>& callback);

  // Cleanup: delete segments before given offset
  Status GarbageCollect(uint64_t before_log_index);
};
```

### 2. StatePersister (Refactored LevelDBPersister)

Responsible for snapshot storage and lightweight metadata.

#### Scope Reduction

Current LevelDBPersister handles:
- ✅ PersistentState (term + voted_for)
- ✅ Log entries (index → RaftLogEntry)
- ✅ Snapshots (SaveSnapshot / LoadSnapshot / SaveSnapshotStream / LoadSnapshotStream)

Refactored StatePersister handles:
- ✅ Snapshots only (chunked storage with SHA-256)
- ✅ Optional: small metadata (last applied index, configuration)
- ❌ Log entries (moved to WALPersister)
- ❌ PersistentState (moved to WALPersister, or kept here if tiny)

#### Rationale

LevelDB is actually well-suited for snapshot storage:
- Snapshot chunks are key-value pairs (`snapshot_chunk:<index>` → chunk data)
- LevelDB's compression reduces snapshot storage size
- Infrequent writes don't trigger excessive compaction

### 3. HybridPersister (Facade)

Maintains backward compatibility with existing `Persister` interface.

```cpp
class HybridPersister : public Persister {
 public:
  Status Open(const std::string& data_dir) override;
  void Close() override;

  // Delegates to WALPersister
  Status SaveState(const PersistentState& state) override;
  Status LoadState(PersistentState& state) override;
  Status AppendEntries(const std::vector<RaftLogEntry>& entries) override;
  Status Sync() override;
  Status GetEntries(uint64_t start, uint64_t end,
                    std::vector<RaftLogEntry>* out) override;
  Status GetEntry(uint64_t index, RaftLogEntry& entry) override;
  Status TruncateSuffix(uint64_t from_index) override;
  Status TruncatePrefix(uint64_t before_index) override;
  std::pair<uint64_t, uint64_t> GetLastLogInfo() override;

  // Delegates to StatePersister
  Status SaveSnapshot(const std::string& data, uint64_t last_index,
                      uint64_t last_term) override;
  Status LoadSnapshot(std::string& data, uint64_t& last_index,
                      uint64_t& last_term) override;
  bool HasSnapshot() const override;
  Status SaveSnapshotStream(...) override;
  Status LoadSnapshotStream(...) override;

 private:
  std::unique_ptr<WALPersister> wal_;
  std::unique_ptr<StatePersister> state_;
  // In-memory index for fast GetEntries (rebuilt on Open)
  std::map<uint64_t, uint64_t> log_index_;  // index → file_offset
};
```

## Recovery Flow

```
On node startup:

1. Open WALPersister
   a. Scan all segment files in <data_dir>/wal/
   b. For each segment, validate header + trailer
   c. Replay records from last valid position:
      - LOG_ENTRY: insert into in-memory index
      - STATE: update current_term / voted_for
      - TRUNCATE_PREFIX: remove entries from index
      - TRUNCATE_SUFFIX: remove entries from index

2. Open StatePersister (LevelDB)
   a. Load snapshot metadata if present
   b. Verify snapshot hash integrity

3. Verify consistency
   a. Ensure last log index ≥ snapshot last index
   b. Ensure PersistentState term matches log entries
```

## Performance Analysis

### Write Path Comparison

| Metric | Current (LevelDB) | WAL Separation |
|--------|-------------------|----------------|
| Log entry write | Memtable + WAL + compaction | Append to file + optional fsync |
| Write amplification | 3-10× (depends on compaction) | ~1× (no compaction) |
| fsync latency | Per write (if sync_on_write) | Batched (group commit) |
| Snapshot write | Same LSM-tree, triggers major compaction | Independent LevelDB, no interference |

### Read Path Comparison

| Metric | Current (LevelDB) | WAL Separation |
|--------|-------------------|----------------|
| GetEntries(range) | LevelDB iterator (may hit multiple SST levels) | In-memory index + file seek (O(1) per entry) |
| GetEntry(single) | LevelDB point lookup | In-memory index + file seek |
| Memory usage | LevelDB block cache + memtable | Index only (~16 bytes per entry) |

## Migration Strategy

### Phase 1: WALPersister Implementation (1 sprint)

1. Implement `WALPersister` with file format, CRC, segment rotation
2. Add comprehensive unit tests (crash recovery, corruption detection, GC)
3. Benchmark vs LevelDB for append-only workload

### Phase 2: HybridPersister Integration (1 sprint)

1. Refactor `LevelDBPersister` to `StatePersister` (remove log entry methods)
2. Implement `HybridPersister` facade
3. Update `CreateLevelDBPersister()` factory to return `HybridPersister`
4. Integration tests: full cluster lifecycle with new persister

### Phase 3: Performance Validation (1 sprint)

1. Benchmark write amplification (current vs new)
2. Benchmark sustained throughput under load
3. Benchmark recovery time (large dataset)
4. Production readiness review

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| WAL file corruption | **High** — total data loss | Per-record CRC32 + periodic integrity checks |
| Disk full during append | **High** — node crash | Pre-allocate segments + monitor free space |
| In-memory index OOM | **Medium** — very large logs | Implement index sparsification (sample every N entries) |
| Migration downtime | **Medium** | Online migration: write to both, read from old, switch atomically |
| Performance regression | **Medium** | Extensive benchmarking before merging to main |

## Open Questions

1. **Should PersistentState stay in LevelDB or move to WAL?**
   - LevelDB: Better for tiny, frequently overwritten data
   - WAL: Simpler architecture, single recovery path
   - **Recommendation**: Move to WAL (simpler), but keep a cached copy in memory

2. **Segment size and rotation policy?**
   - Default 64MB provides good balance between file count and GC granularity
   - Should be configurable via `RaftNodeConfig`

3. **Should we support pluggable WAL backends (e.g., raw block device)?**
   - Not in MVP — start with file-based implementation
   - Design interface to allow future extension

## Appendix: File Layout

```
<data_dir>/
├── wal/
│   ├── 0000000000000001.wal
│   ├── 0000000000000002.wal
│   └── current        # Symlink to latest segment
├── state/
│   ├── CURRENT
│   ├── MANIFEST-XXXXX
│   ├── 000003.log
│   └── ... (LevelDB files)
└── meta/
    └── hybrid.json    # Version info, last segment id, etc.
```
