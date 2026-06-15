# Group Commit / Async WAL Sync Design Spec

> **Status**: Draft — ready for review  
> **Date**: 2026-06-15  
> **Author**: @Jack (Distributed Systems Architect)  
> **Target release**: v0.4.0  
> **Related docs**: `docs/perf-profiling-2026-06.md` §5.1, `docs/roadmap-v0.4.0-draft.md`

---

## 1. Problem Statement

The v0.3.x persister layer pays **one `fsync` per flush batch** when `sync_on_critical` is enabled. Profiling (`docs/perf-profiling-2026-06.md` §4.2) shows that on macOS a single `F_FULLFSYNC` costs **~3.7 ms**, capping durable throughput to **~270 ops/sec** regardless of batch size. The current `LogPersister::BackgroundSyncLoop` only issues a blind periodic `Sync()`; it does not:

* coordinate with the flush loop,
* report durability completion to callers,
* bound the number of unsynced bytes/entries, or
* guarantee ordering between flushed records and sync acknowledgements.

This spec defines a **Group Commit Controller** that batches independent append requests into shared `fsync` operations while preserving Raft safety invariants.

---

## 2. Goals & Non-Goals

### 2.1 Goals

| ID | Goal | Success Criteria |
|---|---|---|
| G1 | Amortize `fsync` across multiple log entries | ≥10× durable throughput vs. per-batch sync at low load |
| G2 | Preserve **total order** of durable acknowledgements | If entry A is acknowledged as durable before entry B, then A is guaranteed to be on stable storage before B |
| G3 | Bound the crash window | Configurable max time / bytes / entries between a successful flush and the next `fsync` |
| G4 | Notify callers when their entries are durable | Per-entry `DurableCallback` invoked exactly once |
| G5 | Keep the existing `Persister` interface unchanged | New behavior is injected through `LogPersister`; backends stay optional |

### 2.2 Non-Goals

* Changing the on-disk WAL record format (handled by @GeoHot WAL Phase 1).
* Linux-specific `O_DIRECT` / `io_uring` / SPDK optimizations (deferred to Linux deployment benchmarking).
* Cross-group group commit in multi-raft mode (group commit is per `LogPersister` / per group).

---

## 3. Design Principles

1. **Decouple *flush* from *sync***. Flush means bytes are written to the kernel page cache; sync means bytes are durably stored. A caller is acknowledged as durable only after both.
2. **Single writer to the WAL**. The existing `WALPersister` mutex serializes appends; group commit runs on top of that serialization without adding contention on the hot path.
3. **Monotonic epochs**. Every flushed batch receives a strictly increasing `CommitEpoch`. A background syncer advances the `DurableEpoch`. Callbacks fire when `entry_epoch <= durable_epoch`.
4. **Fail-fast on sync failure**. If an `fsync` fails, all entries in the *current* and *future* unsynced epochs are marked failed. We never acknowledge durability for a partially-synced batch.
5. **Backpressure via bounded queues**. If the syncer cannot keep up, producers block or receive `Status::Busy()` instead of unbounded buffering.

---

## 4. Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────┐
│                         LogPersister                                │
│  ┌─────────────────┐    ┌───────────────────────────────────────┐  │
│  │  Append Buffer  │───▶│  GroupCommitController                │  │
│  │  (existing)     │    │                                       │  │
│  └─────────────────┘    │  ┌─────────────┐   ┌───────────────┐  │  │
│                         │  │ Flush Queue │──▶│  Sync Worker  │  │  │
│                         │  │  (epochs)   │   │  (fsync batch)│  │  │
│                         │  └─────────────┘   └───────────────┘  │  │
│                         │          │                  │          │  │
│                         │          ▼                  ▼          │  │
│                         │  ┌─────────────────────────────────┐   │  │
│                         │  │   DurableEpoch + Callback FSM   │   │  │
│                         │  └─────────────────────────────────┘   │  │
│                         └───────────────────────────────────────┘  │
│                                      │                              │
│                                      ▼                              │
│                           Persister::AppendEntries                  │
│                           Persister::Sync()                         │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.1 Components

| Component | Responsibility | Threading |
|---|---|---|
| `AppendBuffer` | Batches incoming entries, triggers flush | Producer threads + flush thread |
| `GroupCommitController` | Owns epochs, queues, sync policy, and callback dispatch | Internal mutex; accessed by flush & sync threads |
| `FlushWorker` | Moves buffered entries to `Persister::AppendEntries` | Existing `BackgroundFlushLoop` |
| `SyncWorker` | Decides when to call `Persister::Sync()` and advances `DurableEpoch` | Existing `BackgroundSyncLoop` or strand |
| `CallbackExecutor` | Invokes per-entry durable callbacks without holding locks | Optional executor (default: inline on sync thread) |

---

## 5. Interface Definitions (IDL)

### 5.1 Public Config

```cpp
struct LogPersistenceConfig {
  // --- existing fields ---
  size_t batch_size = 100;
  uint32_t batch_interval_ms = 10;
  bool sync_on_critical = true;
  uint64_t min_disk_space_bytes = 100 * 1024 * 1024;
  std::string data_dir;
  std::function<void(std::function<void()>)> executor;

  // --- new group-commit fields ---

  /**
   * Group commit policy.
   *
   * - kSyncEveryWrite:  no group commit; Sync() after every AppendEntries.
   * - kSyncByInterval:  sync at most every group_commit_interval_ms.
   * - kSyncByBatchSize: sync when unsynced entries or bytes exceed threshold.
   * - kSyncAdaptive:    interval OR batch-size, whichever is reached first.
   */
  enum class SyncPolicy {
    kSyncEveryWrite,
    kSyncByInterval,
    kSyncByBatchSize,
    kSyncAdaptive,
  } sync_policy = SyncPolicy::kSyncAdaptive;

  /** Interval-based sync (ms). 0 means "as fast as possible" (not recommended). */
  uint32_t group_commit_interval_ms = 50;

  /** Batch-size sync: maximum unsynced entries before forcing fsync. */
  size_t group_commit_max_entries = 1000;

  /** Batch-size sync: maximum unsynced bytes before forcing fsync. */
  size_t group_commit_max_bytes = 4 * 1024 * 1024;  // 4 MiB

  /** Maximum time a caller may wait for durability (for AppendSync). */
  std::chrono::milliseconds sync_timeout = std::chrono::seconds(5);
};
```

### 5.2 Public API Additions to `LogPersister`

```cpp
class LogPersister {
 public:
  using DurableCallback = std::function<void(Status)>;

  /**
   * Append an entry and receive a callback when it is durably synced.
   *
   * The callback is invoked exactly once. It may be called:
   * - on the internal sync thread (default), or
   * - on the configured executor if set.
   *
   * Ordering: if A and B are appended in that order and both callbacks fire
   * successfully, then A is durable before B.
   */
  void Append(const RaftLogEntry& entry, DurableCallback callback = nullptr);

  /**
   * Synchronous variant: blocks until the entry is durable or timeout.
   */
  Status AppendSync(const RaftLogEntry& entry,
                    std::chrono::milliseconds timeout = std::chrono::seconds(5));

  /**
   * Explicitly request a sync. Returns when the current flushed data is durable.
   */
  Status Sync();

  /**
   * Force flush + sync of all pending entries. Used by TruncatePrefixAsync.
   */
  Status FlushSync(std::chrono::milliseconds timeout);

  // --- existing methods remain unchanged ---
};
```

### 5.3 Internal `GroupCommitController` API

```cpp
/**
 * Internal abstraction; not exposed outside LogPersister.
 *
 * All methods are thread-safe.
 */
class GroupCommitController {
 public:
  explicit GroupCommitController(LogPersistenceConfig config);

  /**
   * Register a newly flushed batch.
   *
   * @param epoch        Monotonic commit epoch assigned by the controller
   * @param entries      Entries in this batch
   * @param byte_size    Approximate byte size of this batch on disk
   * @param callbacks    Per-entry durable callbacks (may contain nullptr)
   * @return Status::OK() if the batch was accepted; error if controller is unhealthy
   */
  Status RegisterFlushedBatch(
      CommitEpoch epoch,
      const std::vector<RaftLogEntry>& entries,
      size_t byte_size,
      std::vector<LogPersister::DurableCallback> callbacks);

  /**
   * Called by the sync worker when it is about to issue an fsync.
   *
   * Returns the inclusive epoch range [begin, end] that this sync must cover,
   * or std::nullopt if there is nothing to sync.
   */
  std::optional<std::pair<CommitEpoch, CommitEpoch>> AcquireSyncRange();

  /**
   * Called after a successful fsync.
   *
   * Advances the durable epoch and fires callbacks for all epochs <= epoch.
   */
  void OnSyncSuccess(CommitEpoch epoch);

  /**
   * Called after a failed fsync.
   *
   * Fails all callbacks for epochs >= epoch and disables further appends
   * until the controller is reset (typically process restart).
   */
  void OnSyncFailure(CommitEpoch epoch, Status error);

  /**
   * Poll whether the syncer should wake up immediately.
   *
   * Returns true if interval, entry-count, or byte-count threshold is reached.
   */
  bool ShouldSyncNow() const;

  /**
   * Compute the next sleep duration for the sync worker.
   */
  std::chrono::milliseconds NextSyncDelay() const;

  /**
   * Current state for metrics / debugging.
   */
  GroupCommitStats GetStats() const;
};
```

---

## 6. Core Data Structures

### 6.1 `CommitEpoch`

```cpp
using CommitEpoch = uint64_t;

struct PendingEpoch {
  CommitEpoch epoch;
  size_t entry_count;
  size_t byte_size;
  std::vector<LogPersister::DurableCallback> callbacks;
};

struct GroupCommitState {
  CommitEpoch next_epoch = 1;           // next batch gets this epoch
  CommitEpoch durable_epoch = 0;        // all epochs <= this are fsync'd
  CommitEpoch sync_in_progress = 0;     // epoch currently being fsync'd (0 = none)
  std::deque<PendingEpoch> pending;     // flushed but not yet durable
  bool healthy = true;
  Status last_error;
};
```

### 6.2 Invariants

| # | Invariant | Enforced By |
|---|---|---|
| I1 | `durable_epoch < next_epoch` always | `next_epoch` only increments on flush; `durable_epoch` only increments on success |
| I2 | Epochs in `pending` are strictly increasing and contiguous | `RegisterFlushedBatch` appends with `epoch = next_epoch++` |
| I3 | At most one sync is in flight at a time | `AcquireSyncRange` returns empty while `sync_in_progress != 0` |
| I4 | Callbacks fire in epoch order | `OnSyncSuccess` drains from the front of `pending` |
| I5 | A sync failure poisons all later epochs | `OnSyncFailure` drains the entire `pending` queue with the error |

---

## 7. State Machine

### 7.1 `GroupCommitController` State

```text
                    RegisterFlushedBatch
                           │
                           ▼
┌─────────┐    fail    ┌──────────┐    success    ┌─────────────┐
│ Healthy │ ─────────▶ │ Unhealthy│ ◀──────────── │  SyncFailure │
└────┬────┘            └────┬─────┘               └─────────────┘
     │                      │
     │ AcquireSyncRange     │ all calls return error
     ▼                      │
┌───────────┐               │
│ Syncing   │───────────────┘
└─────┬─────┘
      │ SyncSuccess
      ▼
   Healthy (durable_epoch advanced)
```

* **Healthy**: normal operation; flush, sync, and callbacks proceed.
* **Syncing**: an fsync is in flight; new flushes are queued but no second fsync starts.
* **Unhealthy**: a previous sync failed. All pending callbacks are failed; the controller rejects new registrations. The node should treat this as a fatal disk error and step down.

### 7.2 Per-Entry Callback Lifecycle

```text
Append(entry, cb)
       │
       ▼
┌──────────────┐
│   Buffered   │  (in LogPersister buffer)
└──────┬───────┘
       │ flush
       ▼
┌──────────────┐
│   Flushed    │  (epoch assigned, cb not yet called)
└──────┬───────┘
       │ fsync
       ▼
┌──────────────┐
│   Durable    │  (cb invoked with OK)
└──────────────┘
       │
       ▼ (on flush or sync failure)
┌──────────────┐
│    Failed    │  (cb invoked with error)
└──────────────┘
```

---

## 8. Ordering Guarantees

### 8.1 Within One `LogPersister`

* **Flush order** is the order in which `DoFlush` writes batches to `Persister::AppendEntries`.
* **Epoch order** matches flush order (I2).
* **Durable order**: all callbacks of epoch `E` are invoked before any callback of epoch `E+1`.

Therefore, if entry A is appended before entry B and both are successfully flushed, A cannot be reported durable after B.

### 8.2 Interaction with Raft Protocol

* **Leader append**: the leader calls `AppendSync()` (or `Append()` + waits for callback) before responding `AppendEntries` to followers. With group commit, the leader may buffer follower entries briefly, but the response is still sent only after the sync epoch covering those entries is durable.
* **Follower append**: the follower may also use group commit; its `AppendEntries` response is delayed until durability is confirmed. This widens the commit latency by at most `group_commit_interval_ms`.
* **Term/vote persistence**: `SaveState()` remains synchronous (not group-committed). Elections must not lose voted-for information.

---

## 9. Failure Window & Bounds

The maximum data loss window is bounded by the **first** of these thresholds to fire:

| Bound | Config | Meaning |
|---|---|---|
| Time | `group_commit_interval_ms` | Max time between a successful flush and the next fsync |
| Entries | `group_commit_max_entries` | Max unsynced entries before forced fsync |
| Bytes | `group_commit_max_bytes` | Max unsynced bytes before forced fsync |
| Shutdown | `Stop()` | Final fsync on graceful shutdown |

### 9.1 Default Configuration

```cpp
sync_policy = kSyncAdaptive;
group_commit_interval_ms = 50;
group_commit_max_entries = 1000;
group_commit_max_bytes = 4 * 1024 * 1024;
```

At 128 B entries this means:
* Time bound: 50 ms max unsynced data.
* Entry bound: 1,000 entries ≈ 128 KB (well below byte bound).
* Byte bound only matters for large payloads (≥4 KB average).

### 9.2 Trade-off Table

| Scenario | Recommended Policy | Expected Latency | Expected Throughput |
|---|---|---|---|
| Low-latency leader | `kSyncEveryWrite` | ~3.7 ms | ~270 ops/s |
| High-throughput batch | `kSyncAdaptive` | p50≈5 ms, p99≈55 ms | ~10–20k ops/s |
| Latency-sensitive, moderate load | `kSyncByBatchSize` with small max | p50≈2 ms, p99≈10 ms | ~2–5k ops/s |
| Strict durability, no group commit | `kSyncEveryWrite` | ~3.7 ms | ~270 ops/s |

---

## 10. Callback Mechanism

### 10.1 Semantics

* **Exactly-once**: every registered callback is invoked exactly once, either with `Status::OK()` or an error.
* **No blocking**: callback invocation must not block the sync thread. If the configured executor is unavailable, callbacks are invoked inline; they must be lightweight.
* **Ordering-preserving**: callbacks of earlier epochs complete (are invoked) before callbacks of later epochs.

### 10.2 Executor Policy

```cpp
struct LogPersistenceConfig {
  /**
   * Optional executor for durable callbacks.
   *
   * If set, GroupCommitController posts callback batches to this executor,
   * avoiding blocking the sync thread. The executor must outlive LogPersister.
   */
  std::function<void(std::function<void()>)> durable_callback_executor;
};
```

If no executor is configured, callbacks run on the sync thread. For production deployments using ASIO, the ASIO strand should be supplied here.

### 10.3 Batch Callback Optimization

For `AppendBatch()` (future API), a single callback can cover an entire epoch, reducing callback overhead from O(entries) to O(1) per sync.

---

## 11. Threading Model

```text
Producer threads
        │ Append()
        ▼
   ┌─────────┐
   │ buffer_ │  (buffer_mutex_)
   └────┬────┘
        │ notify
Flush thread (BackgroundFlushLoop)
        │ swap + AppendEntries
        ▼
┌───────────────────┐
│ GroupCommitController::RegisterFlushedBatch
│   assigns epoch, enqueues callbacks
└─────────┬─────────┘
          │ notify
Sync thread (BackgroundSyncLoop)
          │ AcquireSyncRange
          │ Persister::Sync()
          │ OnSyncSuccess / OnSyncFailure
          │ invoke callbacks (or post to executor)
```

### 11.1 Lock Hierarchy

1. `buffer_mutex_` (LogPersister buffer)
2. `controller_mutex_` (GroupCommitController state)

The flush thread acquires `buffer_mutex_`, swaps the buffer, releases it, then calls `RegisterFlushedBatch` which acquires `controller_mutex_`. This avoids holding both locks simultaneously.

### 11.2 Wake-up Conditions for Sync Thread

The sync thread sleeps for `NextSyncDelay()` but wakes early when:
* a new epoch is registered and `ShouldSyncNow()` is true,
* `FlushSync()` / `Sync()` is called explicitly,
* `Stop()` is called.

---

## 12. Interaction with WAL Phase 1 Optimizations

@GeoHot's WAL Phase 1 changes (`writev`, no per-entry `ftruncate`, dense index) are **orthogonal** to group commit. The combined pipeline is:

```text
Append() ──▶ buffer ──▶ DoFlush()
                              │
                              ▼
              WALPersister buffered writev() + in-memory trailer
                              │
                              ▼
              GroupCommitController::RegisterFlushedBatch(epoch)
                              │
                              ▼
              SyncWorker ──▶ WALPersister::Sync() ──▶ fsync
                              │
                              ▼
              callbacks fire
```

* WAL Phase 1 reduces **flush** cost.
* Group commit reduces **sync** cost.
* Together they address both bottlenecks identified in profiling §5.1 and §5.2.

---

## 13. Migration Path

### 13.1 From Current Code

1. Introduce `GroupCommitController` as a private member of `LogPersister`.
2. Replace the existing `BackgroundSyncLoop` body with `GroupCommitController` API calls.
3. Move callback notification from `DoFlush()` (current behavior) to `OnSyncSuccess()`.
4. Update `LogPersistenceConfig` defaults to `kSyncAdaptive` after benchmarks confirm safety.
5. Keep `group_commit_interval_ms = 0` as a backward-compatible escape hatch (disables group commit).

### 13.2 Backward Compatibility

* Existing configs with `group_commit_interval_ms > 0` continue to work; they map to `kSyncByInterval`.
* `sync_on_critical = true` with `group_commit_interval_ms = 0` maps to `kSyncEveryWrite`.
* No change to `Persister` interface; custom persisters automatically benefit.

---

## 14. Metrics & Observability

| Metric | Type | Source |
|---|---|---|
| `log_group_commit_pending_epochs` | Gauge | `pending.size()` |
| `log_group_commit_unsynced_entries` | Gauge | sum of `entry_count` in pending |
| `log_group_commit_unsynced_bytes` | Gauge | sum of `byte_size` in pending |
| `log_group_commit_sync_latency_ms` | Histogram | duration of `Persister::Sync()` |
| `log_group_commit_epoch_lag_ms` | Histogram | time from `RegisterFlushedBatch` to `OnSyncSuccess` |
| `log_group_commit_callbacks_fired` | Counter | total callbacks invoked |
| `log_group_commit_sync_failures` | Counter | total `OnSyncFailure` calls |

---

## 15. Risks & Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| Sync thread stalls, widening latency tail | Medium | Bound unsynced entries/bytes; add watchdog; alert on `epoch_lag_ms` |
| Callback executor overflow | Medium | Default to inline callbacks if executor queue is full; document executor sizing |
| Partial sync acknowledged due to epoch mismatch | High | Invariant I3: only one sync in flight; `OnSyncSuccess` advances a single contiguous epoch |
| Disk failure goes unnoticed until next sync | Medium | Sync failures poison controller; Raft node steps down; `IsHealthy()` exposes state |
| Configuration mismatch between nodes | Low | Sync policy is local to each node; Raft safety depends only on individual durability |
| Interaction with snapshot truncation | Low | `TruncatePrefixAsync` calls `FlushSync()` first, ensuring no unsynced data is truncated |

---

## 16. Open Questions

1. **Should `SaveState()` also participate in group commit?**  
   *Recommendation*: No. Term/vote must be synchronous for election safety.

2. **Should followers use the same group commit settings as the leader?**  
   *Recommendation*: Configurable per node. Followers may use a slightly longer interval because they are not on the client critical path.

3. **Do we need a per-group controller in multi-raft?**  
   *Recommendation*: Yes. Each `LogPersister` instance (one per group) owns its own controller. Cross-group fsync coalescing is a future optimization.

---

## 17. Appendix A — Sequence Diagram

```text
Producer 1   Producer 2   Flush Thread   Sync Thread   Persister   Callbacks
   │            │             │             │            │           │
   │ Append(A)  │             │             │            │           │
   │───────────▶│             │             │            │           │
   │            │ Append(B)   │             │            │           │
   │            │────────────▶│             │            │           │
   │            │             │ timeout/    │            │           │
   │            │             │ batch full  │            │           │
   │            │             │             │            │           │
   │            │             │ DoFlush()   │            │           │
   │            │             │ ───────────────────────▶ │           │
   │            │             │             │            │ AppendEntries(A,B)
   │            │             │             │            │           │
   │            │             │ RegisterFlushedBatch(epoch=7)
   │            │             │ ───────────▶│            │           │
   │            │             │             │ AcquireSyncRange() -> [7,7]
   │            │             │             │ ─────────────────────▶│
   │            │             │             │            │ Sync()    │
   │            │             │             │            │ ───────▶  │
   │            │             │             │            │ fsync OK  │
   │            │             │             │            │ ◀───────  │
   │            │             │             │ OnSyncSuccess(7)       │
   │            │             │             │ ─────────────────────────────▶
   │            │             │             │            │           │ cb(A)=OK
   │            │             │             │            │           │ cb(B)=OK
```

---

## 18. Appendix B — Pseudocode for Sync Worker

```cpp
void LogPersister::BackgroundSyncLoop() {
  while (running_) {
    auto delay = controller_.NextSyncDelay();
    {
      std::unique_lock<std::mutex> lock(controller_mutex_);
      sync_cv_.wait_for(lock, delay,
                        [this] { return !running_ || controller_.ShouldSyncNow(); });
    }
    if (!running_) break;

    auto range = controller_.AcquireSyncRange();
    if (!range) {
      continue;
    }

    auto status = persister_->Sync();
    if (status.ok()) {
      controller_.OnSyncSuccess(range->second);
    } else {
      controller_.OnSyncFailure(range->second, status);
      healthy_ = false;
      RecordError(status.ToString());
    }
  }
}
```

---

## 19. Appendix C — Suggested File Layout

```text
include/rollingraft/
  log_persister.h          # extended with SyncPolicy and DurableCallback
  group_commit_controller.h  # new internal header (optional; can be private in .cpp)

src/
  log_persister.cpp        # integrate GroupCommitController
  group_commit_controller.cpp  # new implementation

tests/
  log_persister_group_commit_test.cpp  # new tests

benchmark/
  group_commit_benchmark.cpp         # optional: measure throughput vs latency trade-off
```

---

## 20. Acceptance Criteria

- [ ] `docs/design-group-commit.md` reviewed and approved.
- [ ] `GroupCommitController` unit tests cover:
  - single entry durability,
  - batch durability and callback ordering,
  - sync failure poisoning,
  - `kSyncAdaptive` threshold triggers,
  - explicit `FlushSync()` / `Sync()` coordination.
- [ ] Integration: 332/332 tests pass with `kSyncAdaptive` default.
- [ ] Benchmark shows ≥10× durable throughput improvement vs. `kSyncEveryWrite` at 128 B payloads.
- [ ] No regression in `AppendSync()` p99 latency when group commit is disabled.
