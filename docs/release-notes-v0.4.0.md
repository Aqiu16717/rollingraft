# RollingRaft v0.4.0 Release Notes

> **Tag**: `v0.4.0` → `a8c06a4`  
> **Theme**: Persister Performance Sprint  
> **Scope**: WAL write optimization, group commit / async WAL sync, and WAL index checkpoint for fast recovery.

---

## What's New

### 🚀 WAL Write Optimization

`WALPersister` has been rewritten to reduce per-entry syscall and serialization overhead:

- **1 MB in-memory write buffer**: records are accumulated and flushed with a single `write()` syscall instead of one syscall per entry.
- **Lazy trailer**: the segment trailer is written only at `Sync()`, `Close()`, or segment rotation; per-entry `ftruncate()` has been removed.
- **DenseIndex**: the `std::map<uint64_t, WALIndexEntry>` index has been replaced with a flat, cache-friendly vector indexed by `log_index - first_index_`, giving O(1) lookups and contiguous range scans.

**Result** (`benchmark_persister_micro`, 128 B entries, nosync):

| Metric | Before | After | Improvement |
|---|---|---|---|
| WAL append throughput | 3.87 MB/s | ~81 MB/s | **~20×** |

---

### 🚀 Group Commit / Async WAL Sync

`LogPersister` now separates flushing from durable sync through a new internal `GroupCommitController`:

- Four sync policies: `kSyncEveryWrite`, `kSyncByInterval`, `kSyncByBatchSize`, and `kSyncAdaptive`.
- Flushed batches are assigned monotonic `CommitEpoch`s; a background sync thread issues a single `fsync` for all pending epochs.
- Durable callbacks are fired after `fsync` succeeds; sync failure poisons pending epochs and invokes error callbacks.
- Explicit `Sync()` blocks until all flushed-but-not-yet-synced data is durable.

**Result** (`benchmark_group_commit`, 100 entries, 3.7 ms simulated fsync):

| Policy | Throughput |
|---|---|
| sync-every-write | ~230 ops/s |
| group-commit | ~24,968 ops/s |
| **Improvement** | **~108×** |

---

### 🚀 WAL Index Checkpoint

`WALPersister::Open()` can now recover from a binary checkpoint of the in-memory `DenseIndex` instead of scanning every segment:

- Checkpoint file: `checkpoint.<last_covered_segment_id>.idx`
- Format: header + array of `WALIndexEntry` + CRC32 footer
- Recovery loads the latest valid checkpoint and scans only segments written after it
- Checkpoints are created on `Sync()` / `Close()` when segment or entry thresholds are met
- Corrupted checkpoints are ignored and recovery falls back to full segment scan
- `GarbageCollect()` removes checkpoints that only cover deleted segments

**Result** (`benchmark_persister_micro`, 100 k entries, 128 B):

| Metric | Before | After | Improvement |
|---|---|---|---|
| WAL recovery time | ~161 ms | ~5.9 ms | **~27×** |

---

### 📊 Benchmarking & Profiling

- Added `benchmark/persister_micro_benchmark.cpp` covering protobuf serialization, raw write/fsync, WAL append/recovery, LevelDB batching, and snapshot streaming.
- Added `docs/perf-profiling-2026-06.md` identifying the top 3 persister-layer bottlenecks and ranked optimization recommendations.

---

## Design Documents

- `docs/design-group-commit.md`: group commit / async WAL sync architecture
- `docs/design-wal-checkpoint.md`: WAL index checkpoint format and recovery flow
- `docs/roadmap-v0.4.0-draft.md`: v0.4.0 roadmap (performance line, now delivered)

---

## Upgrade Notes

- No breaking public API changes.
- On-disk segment format is unchanged; existing segments remain readable.
- `LogPersistenceConfig` gains new fields (`sync_policy`, `group_commit_max_entries`, `group_commit_max_bytes`, `sync_timeout`, `durable_callback_executor`). Default behavior (`kSyncAdaptive`) provides group commit out of the box.
- Users who relied on `sync_on_critical` can migrate to `SyncPolicy::kSyncEveryWrite` for the old behavior.

---

## Test Coverage

- New unit tests:
  - 13 `GroupCommitController` tests
  - 4 `LogPersister` group-commit integration tests
  - 3 `WALPersister` checkpoint tests
- Full regression suite: **354/354 tests pass** (321 unit + 27 integration + 6 deterministic).

---

*For the detailed changelog, see [CHANGELOG.md](../CHANGELOG.md).*
