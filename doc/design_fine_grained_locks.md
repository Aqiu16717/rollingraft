# Design Note: Fine-Grained Locks for RaftNodeImpl

**Author:** @GeoHot  
**Date:** 2026-05-10  
**Branch:** `feature/task1-fine-grained-locks`  
**Status:** Draft — awaiting @Jack audit

## Problem Statement

`RaftNodeImpl` currently uses a single monolithic `mtx_` to protect all mutable state.
This creates two problems:

1. **Contention bottleneck** — Leader election, log replication, snapshot transfer,
   membership changes, and state machine application all serialize through one mutex.
   Under load, RPC handlers and timer callbacks queue up behind unrelated work.

2. **Callback safety** — Several code paths manually unlock `mtx_` to invoke user
   callbacks (e.g. `ProcessPendingReadsLocked`, `ApplyCommittedLocked`), then
   re-acquire it. This pattern is fragile and error-prone.

## Proposed Lock Hierarchy

Replace `mtx_` with five manager-specific mutexes, ordered strictly to prevent
inversion deadlocks:

```
ElectionManager → LogReplicator → SnapshotManager → MembershipManager → StateMachineApplier
```

A thread may hold locks **left-to-right** only. If a caller needs to access a
"downstream" manager, it must either:
* Drop its own lock before calling downstream (preferred), or
* Acquire downstream locks in hierarchy order while still holding upstream lock
  (only for atomic read-only snapshots).

## State Partitioning

### 1. ElectionManager (`election_mtx_`)
**Owns:**
* `current_term_`
* `voted_for_`
* `vote_count_`
* `role_`
* `leader_id_` / `leader_addr_`
* `election_timer_`

**Methods:** `BecomeFollowerLocked`, `BecomeCandidateLocked`, `BecomeLeaderLocked`,
`BroadcastRequestVoteLocked`, `HandleRequestVote`, `HandleRequestVoteResponse`,
`ResetElectionTimerLocked`, `CancelElectionTimerLocked`

### 2. LogReplicator (`replication_mtx_`)
**Owns:**
* `log_`
* `commit_index_`
* `next_index_`
* `match_index_`
* `retry_state_`
* `heartbeat_timer_`
* `flushed_index_`

**Methods:** `BroadcastAppendEntriesLocked`, `SendAppendEntriesToPeerLocked`,
`HandleAppendEntries`, `HandleAppendEntriesResponse`, `TryCommitLocked`,
`ScheduleAppendEntriesRetry`, `StartHeartbeatTimerLocked`, `StopHeartbeatTimerLocked`

### 3. SnapshotManager (`snapshot_mtx_`)
**Owns:**
* `last_snapshot_index_`
* `snapshot_sends_`
* `snapshot_temp_data_`
* `snapshot_check_timer_`

**Methods:** `MaybeTriggerAutoSnapshotLocked`, `SendInstallSnapshotToPeerLocked`,
`SendNextSnapshotChunkLocked`, `HandleInstallSnapshot`, `HandleInstallSnapshotResponse`

### 4. MembershipManager (`membership_mtx_`) — *already has `config_mutex_`*
**Owns:**
* `cluster_config_` (merge `config_mutex_` into `membership_mtx_`)

**Methods:** `AddNode`, `RemoveNode`, `GetConfig`, `ApplyConfigChangeLocked`

### 5. StateMachineApplier (`applier_mtx_`)
**Owns:**
* `last_applied_`
* `pending_proposals_`
* `pending_reads_`
* `next_read_id_`
* `client_sessions_`

**Methods:** `ApplyCommittedLocked`, `ProcessPendingReadsLocked`,
`HandleReadIndexAckLocked`, `BroadcastReadIndexHeartbeatsLocked`,
`Propose`, `ProposeBatch`, `ReadIndex`

### 6. Core / Coordinator (no dedicated mutex)
**Owns:**
* `state_` (already `std::atomic`)
* `server_id_`
* `peer_addrs_` / `peer_map_` (immutable after ctor)
* `next_correlation_id_` (already `std::atomic`)

The coordinator delegates to managers and does not hold persistent state beyond
immutable config and atomic runtime flags.

## Cross-Manager Call Patterns

### Pattern A: Read-only snapshot (lock ordering)
```cpp
// ElectionManager needs commit_index for leader logic
std::lock_guard lock_e(election_mtx_);
std::lock_guard lock_r(replication_mtx_);  // downstream, OK
Term term = current_term_;
Index commit = commit_index_;
```

### Pattern B: Drop-then-call (preferred for mutations)
```cpp
// LogReplicator wants to notify applier of new commits
Index new_commit;
{
  std::lock_guard lock(replication_mtx_);
  commit_index_ = ...;
  new_commit = commit_index_;
}
// Drop replication lock before calling downstream
applier_.NotifyCommitted(new_commit);
```

### Pattern C: Callback invocation (never under lock)
```cpp
// StateMachineApplier completes a read
std::function<void()> cb;
{
  std::lock_guard lock(applier_mtx_);
  cb = std::move(pending_reads_[id].callback);
  pending_reads_.erase(id);
}
// Invoke outside lock
if (cb) cb();
```

## Async TruncatePrefix Safety

`TruncatePrefix` removes old log entries. It must not truncate:
* Entries > `last_applied_` (StateMachineApplier hasn't applied them)
* Entries referenced by an in-flight snapshot transfer (SnapshotManager)

**Protocol:**
1. StateMachineApplier calls `applier_.GetSafeTruncateIndex()` under `applier_mtx_`
2. If a snapshot is in progress, SnapshotManager returns `min(last_applied_, snapshot_start_index)`
3. LogReplicator performs the actual truncation under `replication_mtx_`
4. No lock is held across the truncate I/O operation

## Open Questions for @Jack

1. **Metrics access** — `metrics_` is touched from all managers. Should it have its
   own spinlock/mutex, or use atomics only?
2. **Log persistence callback** — `log_persister_->Append()` fires a callback that
   currently re-acquires `mtx_`. With fine-grained locks, should the callback post
   to `asio::post` and let the executor thread pick it up, or can it acquire
   `replication_mtx_` directly?
3. **`config_mutex_` deprecation** — Should we keep `config_mutex_` as a separate
   lightweight lock (read-heavy), or merge it into `membership_mtx_`?

## Implementation Plan

| Phase | Scope | Files |
|-------|-------|-------|
| 1 | Mutex declarations in `raft_node_impl.h` | `raft_node_impl.h` |
| 2 | ElectionManager lock refactoring | `election_manager.cpp`, `rpc_handlers.cpp` |
| 3 | LogReplicator lock refactoring | `log_replicator.cpp`, `raft_node_core.cpp` |
| 4 | SnapshotManager + MembershipManager | `snapshot_manager.cpp`, `membership_manager.cpp` |
| 5 | StateMachineApplier lock refactoring | `state_machine_applier.cpp` |
| 6 | Remove `mtx_` and validate with TSan | All |

Each phase is an independent commit for incremental audit.
