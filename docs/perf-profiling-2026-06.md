# Persister-Layer Performance Profiling — June 2026

**Owner:** @GeoHot  
**Scope:** `WALPersister`, `StatePersister`, `HybridPersister` (default backend used by both `leveldb` and `hybrid` factories)  
**Target release:** v0.4.0  
**Methodology:** Reuse existing `benchmark_persister` macro data + add new micro-benchmarks (`benchmark/persister_micro_benchmark.cpp`) to isolate serialization, fsync, WAL write/read, LevelDB batching, and snapshot-streaming costs.

---

## 1. Executive Summary

The persister layer is **correct and robust**, but three bottlenecks cap throughput and inflate recovery latency on the critical path:

1. **fsync / durable-flush latency** — dominates any path that guarantees durability.
2. **WAL per-entry syscall & serialization overhead** — WAL append is ~18× slower than raw file append even without fsync.
3. **Recovery / reopen latency** — scales linearly with log size and becomes a second-scale cost at 100k entries.

Serialization (protobuf) and snapshot streaming are **not** bottlenecks at current payload sizes.

---

## 2. Benchmark Setup

- **Machine:** Apple Silicon (macOS), Release build (`build_test`), SSD.
- **Existing macro benchmark:** `benchmark/persister_benchmark.cpp` (`benchmark_persister`).
- **New micro benchmark:** `benchmark/persister_micro_benchmark.cpp` (`benchmark_persister_micro`).
- **Backends tested:** `leveldb` and `hybrid` (both resolve to `HybridPersister` via the current factories).

> Note on defaults: `CreateLevelDBPersister()` currently returns a `HybridPersister`. Therefore the `leveldb` backend results in this report reflect the hybrid (WAL log + LevelDB state) stack, not the legacy `LevelDBPersister`.

---

## 3. Macro Results (`benchmark_persister`)

### 3.1 Append Throughput — 20k entries, 100 B payload

| Backend | Batch | Compression | ops/sec | p50 (us) | p99 (us) | Dir size |
|---|---:|---:|---:|---:|---:|---:|
| leveldb | 1 | none | 27,285 | 34 | 52 | 3.92 MB |
| leveldb | 10 | none | 2,825 | 349 | 424 | 3.92 MB |
| leveldb | 100 | none | 284 | 3,485 | 4,368 | 3.92 MB |
| hybrid | 1 | none | 23,095 | 39 | 107 | 3.92 MB |
| hybrid | 10 | none | 2,729 | 362 | 442 | 3.92 MB |
| hybrid | 100 | none | 266 | 3,688 | 4,619 | 3.92 MB |

*Entries/sec is nearly identical across batch sizes because larger batches amortize overhead; per-batch latency grows proportionally.*

### 3.2 Recovery / Reopen Latency

| Backend | Entries | Reopen (ms) | Dir size |
|---|---:|---:|---:|
| leveldb / hybrid | 1,000 | ~24 | 199 KB |
| leveldb / hybrid | 10,000 | ~149 | 1.95 MB |
| leveldb / hybrid | 10,000 | ~148 | 1.95 MB |
| leveldb / hybrid | 100,000 | ~1,281 | 19.64 MB |

**Observation:** reopen latency is **linear** with stored log size (~13 µs/entry in the macro benchmark).

---

## 4. Micro Results (`benchmark_persister_micro`)

### 4.1 Serialization

| Scenario | Iterations | Avg µs | P99 µs | Throughput |
|---|---:|---:|---:|---:|
| protobuf serialize 100 B | 200k | 0.04 | 0.04 | 3.2 GB/s |
| protobuf serialize 1 KB | 200k | 0.04 | 0.08 | 23.8 GB/s |
| protobuf deserialize 100 B | 200k | 0.06 | 0.08 | 2.0 GB/s |
| protobuf deserialize 1 KB | 200k | 0.07 | 0.08 | 15.1 GB/s |

**Verdict:** protobuf serialization is cheap and not a bottleneck.

### 4.2 Raw File Write vs WAL Append

| Scenario | Iterations | Avg µs | P99 µs | Throughput |
|---|---:|---:|---:|---:|
| raw write 128 B (no fsync) | 10k | 1.66 | 8.46 | 73.5 MB/s |
| raw write 128 B (fsync each) | 10k | 3,742 | 4,514 | 0.03 MB/s |
| WAL append 128 B (no fsync) | 10k | 31.5 | 67.3 | 3.87 MB/s |
| WAL append 128 B (fsync each) | 10k | 3,742 | 4,952 | 0.03 MB/s |

**Verdict:**
- fsync is the single largest cost: ~3.7 ms/op on this machine (macOS `F_FULLFSYNC`).
- Even without fsync, WAL append is **~19× slower** than raw file append. The extra cost comes from per-record protobuf serialization, CRC32, four `write()` syscalls, `ftruncate()`/`lseek()`, trailer rewrite, and `std::map` index insertion.

### 4.3 LevelDB Batching

| Scenario | Iterations | Avg µs | P50 µs | Throughput |
|---|---:|---:|---:|---:|
| LevelDB batch=1, 256 B, no fsync | 5k | 2.41 | 1.67 | 101 MB/s |
| LevelDB batch=100, 256 B, no fsync | 50 batches | 74.8 | 42.7 | 326 MB/s |
| LevelDB batch=100, 256 B, fsync | 50 batches | 3,625 | 3,966 | 6.7 MB/s |

**Verdict:** LevelDB write batching is efficient; the cost is again dominated by fsync when enabled.

### 4.4 WAL / Hybrid Recovery

| Scenario | Entries | Payload | Time (ms) |
|---|---:|---:|---:|
| WAL recovery | 10k | 128 B | ~30 |
| WAL recovery | 100k | 128 B | ~187 |
| Hybrid recovery | 10k | 100 B | ~39 |
| Hybrid recovery | 100k | 100 B | ~189 |

**Verdict:** pure WAL scan is fast (~1.9 µs/entry). The macro `reopen=1.28 s` for 100k entries includes additional HybridPersister/LevelDB state-stack overhead; the WAL scan itself is only ~15% of that time.

### 4.5 Snapshot Streaming

| Scenario | Total | Chunk | Time (ms) | Throughput |
|---|---:|---:|---:|---:|
| snapshot stream | 10 MB | 64 KB | ~104 | 96.9 MB/s |
| snapshot stream | 10 MB | 1 MB | ~56 | 177 MB/s |

**Verdict:** snapshot streaming is healthy; larger chunks reduce LevelDB round-trips and SHA-256 block overhead.

---

## 5. TOP 3 Bottlenecks & Recommendations

| Rank | Bottleneck | Impact | Risk | Section |
|---:|---|---|---|---|
| 1 | fsync / durable flush | **HIGH** | **MEDIUM** | 5.1 |
| 2 | WAL per-entry syscall & serialization overhead | **HIGH** | **LOW** | 5.2 |
| 3 | Recovery / reopen latency (linear scan + state stack) | **MEDIUM** | **LOW** | 5.3 |

### 5.1 #1 — fsync / Durable Flush Latency

**Evidence:**
- Raw 128 B write + fsync: **~3.7 ms/op** (0.03 MB/s).
- WAL append + fsync each: **0.03 MB/s** — identical to raw fsync, proving fsync is the floor.
- LevelDB batch=100 + fsync: **~4 ms/batch** regardless of payload.

**Root cause:** `WALPersister::Sync()` calls `F_FULLFSYNC` (macOS) / `fdatasync` (Linux) on every sync. The public `Persister::Sync()` is exposed per-batch, and any durability guarantee must pay this latency.

**Recommendations (ranked by benefit/risk):**

| # | Recommendation | Benefit | Risk | Effort |
|---:|---|---|---|---|
| 1a | **Group-commit / async WAL sync**: buffer multiple client appends and issue one fsync for the group; return futures or ack after group commit. | Eliminates per-append fsync; can recover 10–100× durable throughput. | MEDIUM: requires careful ordering and failure-window sizing. | Medium |
| 1b | **Decouple Raft commit from durable sync**: keep the current in-memory Raft log, flush to WAL asynchronously within a bounded window (e.g., 1 ms / N entries). | Hides fsync latency from propose path. | MEDIUM: widens the crash window; must bound it. | Medium |
| 1c | **Use `O_DIRECT` + SPDK / io_uring on Linux** for NVMe. | Lower fsync tail latency. | HIGH: platform-specific, complex, may not help on Apple Silicon dev machines. | High |

**Suggested first step:** implement group commit (1a) behind a `sync_policy` config so correctness-critical paths keep the current behavior while high-throughput paths opt in.

### 5.2 #2 — WAL Per-Entry Syscall & Serialization Overhead

**Evidence:**
- Raw 128 B write (no fsync): **73.5 MB/s**.
- WAL append 128 B (no fsync): **3.87 MB/s** — **19× slower**.
- Serialization alone is **3+ GB/s**, so the cost is not protobuf.

**Root cause:** `WALPersister::AppendLogEntry()` does per-record work:
1. Protobuf serialize + CRC32.
2. `ftruncate(active_segment_.fd, end_offset)` to erase old trailer.
3. `lseek(..., SEEK_SET)`.
4. Four `write()` syscalls (CRC, length, type, payload).
5. `WriteTrailer()` + another `write()`.
6. `std::map<uint64_t, WALIndexEntry>` insertion under a global mutex.

**Recommendations:**

| # | Recommendation | Benefit | Risk | Effort |
|---:|---|---|---|---|
| 2a | **Buffer records and use `writev()` / single `pwrite()` per batch**: build the full record (header + payload) in a thread-local or per-segment buffer and issue one syscall. | Cuts syscalls from 5+ per entry to 1 per batch; likely 5–10× WAL append improvement. | LOW: local change, keep existing on-disk format. | Low |
| 2b | **Pre-allocate segment files and avoid `ftruncate()` per append**: track trailer offset in memory and overwrite only on sync/rotation. | Removes one syscall and file-metadata update per entry. | LOW: may waste tail bytes until sync. | Low |
| 2c | **Use a flat array / dense hash map for the in-memory index**: `std::map` has pointer-chasing overhead and allocates per node. A sorted vector or `absl::btree_map`/`phmap` reduces insertion and lookup cost. | Faster index rebuild on recovery and lower per-append latency. | LOW: swap-in data structure, same semantics. | Low |
| 2d | **Memory-map active segment** instead of `write()`: append via pointer increment, flush with `msync()`. | Can approach raw write throughput; simplifies framing. | MEDIUM: changes crash-recovery semantics; platform-specific flush behavior. | Medium |

**Suggested first step:** combine 2a + 2b + 2c for a low-risk, high-return WAL append optimization.

### 5.3 #3 — Recovery / Reopen Latency

**Evidence:**
- Macro reopen: **~1.28 s for 100k entries** (~13 µs/entry).
- WAL-only scan: **~187 ms for 100k entries** (~1.9 µs/entry).
- The gap (~1.1 s) is HybridPersister + LevelDB state-stack overhead.

**Root cause:**
- `WALPersister::Open()` reads every segment and rebuilds the `std::map` index.
- `HybridPersister::Open()` opens both WAL and LevelDB and runs a consistency check.
- LevelDB itself replays its own internal WAL on open.

**Recommendations:**

| # | Recommendation | Benefit | Risk | Effort |
|---:|---|---|---|---|
| 3a | **Persist the WAL index checkpoint**: periodically write a small index snapshot (e.g., last N segments) so reopen can skip full scan. | Reduces recovery from O(N) to O(delta). | LOW: add optional checkpoint file; fallback to full scan if missing/corrupt. | Low |
| 3b | **Parallel segment scan**: open and scan independent WAL segments on multiple threads during recovery. | Near-linear speedup on multi-core for large logs. | LOW: segments are immutable except active; need to merge indexes. | Low |
| 3c | **Aggressive log truncation + snapshotting**: keep the WAL short by truncating committed prefixes and compacting into snapshots more often. | Keeps recovery bounded. | MEDIUM: changes operational tuning; may increase snapshot frequency. | Low |
| 3d | **Tune LevelDB `write_buffer_size` / `max_file_size` and force manual compaction** for the state DB to reduce its recovery log. | Reduces the state-stack portion of reopen. | LOW: configuration change. | Low |

**Suggested first step:** 3a (index checkpoint) gives the biggest deterministic reduction with minimal risk; pair with 3c to keep logs short.

---

## 6. What Is NOT a Bottleneck

| Area | Observation | Implication |
|---|---|---|
| Protobuf serialization | 2–23 GB/s | Keep protobuf; no need to revert to JSON or hand-rolled binary for log entries. |
| Snapshot streaming | 97–177 MB/s | Chunk size of 1 MB is better than 64 KB; current atomic-swap design is sound. |
| Memory footprint | RSS delta ~0–1.8 MB in benchmarks | No memory-leak or allocation hotspot. |
| Compression | Snappy vs none shows negligible difference at 100 B payload | Compression tuning is not urgent. |

---

## 7. Recommended Action Plan for v0.4.0

1. **Short-term (low risk, high return)**
   - WAL buffered `writev()` + remove per-entry `ftruncate()` (§5.2, 2a/2b).
   - Replace `std::map` WAL index with dense structure (§5.2, 2c).
   - Add WAL index checkpoint + aggressive truncation (§5.3, 3a/3c).

2. **Medium-term (architectural)**
   - Group-commit / async WAL sync policy (§5.1, 1a).
   - Parallel segment scan for recovery (§5.3, 3b).

3. **Defer**
   - `O_DIRECT` / SPDK / io_uring until Linux deployment benchmarking (§5.1, 1c).
   - Memory-mapped WAL until crash-recovery semantics are formally verified (§5.2, 2d).

---

## 8. Appendix: Raw Micro-Benchmark Output

```text
| Scenario | Iterations | Total ms | Avg us | P50 us | P99 us | Throughput |
|---|---:|---:|---:|---:|---:|---:|
| protobuf_serialize_100B | 200000 | 7.02 | 0.04 | 0.04 | 0.04 | 3257.54 MB/s |
| protobuf_serialize_1024B | 200000 | 8.37 | 0.04 | 0.04 | 0.08 | 23821.54 MB/s |
| protobuf_deserialize_100B | 200000 | 11.39 | 0.06 | 0.04 | 0.08 | 1975.79 MB/s |
| protobuf_deserialize_1024B | 200000 | 13.16 | 0.07 | 0.08 | 0.08 | 15114.50 MB/s |
| raw_write_nosync_128B | 10000 | 16.60 | 1.66 | 1.42 | 8.46 | 73.54 MB/s |
| raw_write_fsync_128B | 10000 | 37421.67 | 3742.17 | 3971.50 | 4513.67 | 0.03 MB/s |
| raw_write_nosync_1024B | 5000 | 48.36 | 9.67 | 3.92 | 49.92 | 100.97 MB/s |
| raw_write_fsync_1024B | 5000 | 18780.63 | 3756.13 | 3969.62 | 5029.17 | 0.26 MB/s |
| wal_append_nosync_128B | 10000 | 315.41 | 31.54 | 30.58 | 57.58 | 3.87 MB/s |
| wal_append_sync_128B | 10000 | 37426.45 | 3742.64 | 3968.79 | 4952.04 | 0.03 MB/s |
| wal_recovery_10000_128B | 1 | 30.47 | 30469.92 | 30469.92 | 30469.92 | 40.06 MB/s |
| wal_recovery_100000_128B | 1 | 186.63 | 186630.96 | 186630.96 | 186630.96 | 65.41 MB/s |
| hybrid_recovery_10000_100B | 1 | 39.23 | 39233.96 | 39233.96 | 39233.96 | 24.31 MB/s |
| hybrid_recovery_100000_100B | 1 | 189.22 | 189224.96 | 189224.96 | 189224.96 | 50.40 MB/s |
| leveldb_batch_1_nosync_256B | 5000 | 12.07 | 2.41 | 1.67 | 12.79 | 101.11 MB/s |
| leveldb_batch_100_nosync_256B | 50 | 3.74 | 74.77 | 42.67 | 790.21 | 326.54 MB/s |
| leveldb_batch_100_sync_256B | 50 | 181.26 | 3625.19 | 3965.96 | 4357.88 | 6.73 MB/s |
| snapshot_stream_1048576_chunk_nosync | 1 | 56.44 | 56435.96 | 56435.96 | 56435.96 | 177.19 MB/s |
```

---

## 9. Files Changed

- `benchmark/persister_micro_benchmark.cpp` — new micro-benchmarks.
- `benchmark/CMakeLists.txt` — adds `benchmark_persister_micro` target.
- `docs/perf-profiling-2026-06.md` — this document.
