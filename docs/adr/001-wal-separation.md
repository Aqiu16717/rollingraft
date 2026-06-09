# ADR-001: WAL Separation

**Status:** Accepted  
**Date:** 2026-06-09  
**Deciders:** @Jack (Architecture), @Tom (Audit), @Cindy (Product), @Alice (Implementation)  

---

## Context & Problem Statement

RollingRaft initially stored all persistent state — Raft metadata (`term`, `voted_for`), log entries, and snapshots — in a single LevelDB database via `LevelDBPersister`. As the project evolved, this monolithic design created three problems:

1. **Write amplification**: Every log append triggered LevelDB's LSM-tree write path (memtable flush, SST compaction), resulting in 3-10× write amplification for sequential log appends.
2. **Recovery complexity**: Replaying the log required reading through multiple SST levels, making recovery time unpredictable.
3. **Multi-raft incompatibility**: A single LevelDB instance is difficult to partition cleanly for per-group storage in a multi-raft architecture.

The question: how should we restructure persistence to separate high-throughput sequential log writes from low-frequency random metadata/snapshot writes?

---

## Options Considered

### Option A: Keep Monolithic LevelDB (status quo)

- **Pros**: Zero code change; existing tests pass unchanged.
- **Cons**: Write amplification remains; recovery remains unpredictable; blocks multi-raft per-group storage.
- **Verdict**: Rejected. The status quo does not solve any of the stated problems.

### Option B: RocksDB-style Column Families

- **Pros**: Clean separation of metadata / log / snapshot into distinct LSM trees; proven in TiKV/RocksDB ecosystems.
- **Cons**: Requires migrating from LevelDB to RocksDB (new dependency, different API, build complexity); overkill for our current scale; adds operational burden.
- **Verdict**: Rejected. We want to keep LevelDB as the default backend and avoid dependency churn.

### Option C: Separate WAL File + Keep LevelDB for State/Snapshot

- **Pros**:
  - Log writes become true sequential appends (~1× write amplification).
  - Recovery is a linear scan of WAL segments with predictable O(n) time.
  - LevelDB retains its strengths for metadata and snapshot chunk storage.
  - Minimal dependency change.
- **Cons**:
  - Two storage subsystems to manage.
  - Need crash-recovery logic to validate WAL/state consistency on `Open()`.
  - `Persister` interface must be refactored or wrapped.
- **Verdict**: **Accepted**.

### Option D: Single WAL File for Everything (metadata + log + snapshot)

- **Pros**: Ultimate simplicity — one file, one fsync path.
- **Cons**: Snapshots are large binary blobs; mixing them with small log records destroys sequential-write benefits and complicates compaction.
- **Verdict**: Rejected. Snapshot data belongs in a key-value store with chunking support.

---

## Decision

Adopt **Option C**: introduce a `WALPersister` for log entries and a `StatePersister` for metadata (`PersistentState`) and snapshots. A `HybridPersister` facade implements the existing `Persister` interface by delegating:

- Log operations (`AppendEntries`, `GetEntries`, `Truncate*`) → `WALPersister`
- State operations (`SaveState`, `LoadState`) → `StatePersister`
- Snapshot operations (`SaveSnapshot*`, `LoadSnapshot*`) → `StatePersister`

Key design choices from T2 review:
- `PersistentState` stays in `StatePersister`, not WAL (safety-critical metadata should not depend on log replay).
- Segment cuts happen at **log index boundaries**, not fixed byte sizes (simplifies truncation and GC).
- `meta/hybrid.json` tracks `last_segment_id` instead of a symlink (cross-platform safety).
- WAL segment format: magic + version + segment_id header, variable-length records (CRC32 + length + type + payload), 8-byte end-offset trailer.

---

## Consequences

### Positive

- **Write amplification reduced** from 3-10× to ~1× for log appends.
- **Recovery time predictable**: linear scan of WAL segments, bounded by log size.
- **Multi-raft ready**: per-group WAL instances are natural; state can share LevelDB with key prefixing.
- **Backwards compatible**: `CreateLevelDBPersister()` now returns `HybridPersister`; existing tests require zero changes.

### Negative / Trade-offs

- **Two failure domains**: WAL corruption and LevelDB corruption are now independent failure modes.
- **Consistency check on Open**: `HybridPersister::Open()` must verify that WAL's last log index ≥ StatePersister's snapshot last index.
- **Operational complexity**: operators must monitor both `data_dir/wal/` and `data_dir/state/`.

### Metrics

T3 Phase 3 validation confirmed zero performance regression in the 100B-payload baseline; the architectural benefits dominate at this stage.

---

## 验证结果 (2026-06-09)

| 假设 | 状态 | 数据 |
|------|------|------|
| 大 payload WAL 优势 | ❌ 不成立 | 1KB+ 两者持平，disk I/O 瓶颈 |
| 高并发 LevelDB 瓶颈 | ✅ 成立(小payload) | 100B 并发: Hybrid 3.5× 吞吐, p99 1.6ms vs 66ms |

详细数据见 `docs/benchmark-large-payload.md`。
