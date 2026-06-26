# Raft Core Metrics Design

## Overview

This document describes the Prometheus-style metrics exposed by RollingRaft
nodes, their semantics, update sites, and the design rationale.  Metrics are
emitted through the existing `MetricsRegistry` and served by
`MetricsHttpServer` on the configured `/metrics` endpoint.

## Goals

- Provide operators with real-time visibility into Raft node health,
  leadership, replication progress, and durability lag.
- Keep metric cardinality bounded: labels are limited to `node_id` and
  `peer_id`.
- Update metrics only at existing lock boundaries to avoid adding new
  synchronization or hot-path overhead.
- Ensure every metric is covered by at least one unit or integration test.

## Metric Taxonomy

### 1. Node / Leadership State

| Name | Type | Labels | Description |
|------|------|--------|-------------|
| `raft_role` | gauge | `node_id` | Current role: `FOLLOWER=0`, `CANDIDATE=1`, `LEADER=2` |
| `raft_current_term` | gauge | `node_id` | Current Raft term |
| `raft_leader_lease_valid` | gauge | `node_id` | `1` if the node is leader and its leader lease is currently valid, otherwise `0` |
| `raft_leader_lease_seconds` | gauge | `node_id` | Remaining leader-lease time in seconds (`0` if not leader or lease expired) |

**Update sites**
- `raft_role` / `raft_current_term`: `BecomeFollowerLocked`, `BecomeCandidateLocked`, `BecomeLeaderLocked`.
- `raft_leader_lease_valid` / `raft_leader_lease_seconds`:
  `UpdateLeaderLeaseMetricLocked()` called after quorum acks in
  `HandleAppendEntriesResponse` / `HandleHeartbeatResponse` and on every role
  transition to follower.

### 2. Log Persistence & Durability

| Name | Type | Labels | Description |
|------|------|--------|-------------|
| `raft_group_commit_unsynced_entries` | gauge | `node_id` | Number of flushed-but-not-yet-fsynced log entries |
| `raft_group_commit_pending_epochs` | gauge | `node_id` | Number of commit epochs waiting for fsync |
| `raft_group_commit_lag_ms` | gauge | `node_id` | Wall-clock lag (ms) of the oldest epoch when it becomes durable |
| `raft_log_persister_sync_latency_ms` | histogram | `node_id` | Latency of `Persister::Sync()` / fsync in milliseconds |

**Update sites**
- `GroupCommitController::RegisterFlushedBatch` increments pending epochs and
  unsynced entries.
- `GroupCommitController::OnSyncSuccess` advances the durable epoch, records
  `raft_group_commit_lag_ms` for the oldest synced epoch, and resets
  pending/unsynced gauges.
- `GroupCommitController::OnSyncFailure` poisons pending epochs and resets the
  gauges.
- `LogPersister::BackgroundSyncLoop` and the `kSyncEveryWrite` inline path
  record `raft_log_persister_sync_latency_ms` around every `persister_->Sync()`.

**Rationale for unit names**
- `raft_group_commit_unsynced_entries` directly answers "how many entries are
  not yet durable?", which is the operational lag metric.
- `raft_group_commit_pending_epochs` exposes the batch/epoch backlog.
- `raft_group_commit_lag_ms` measures the end-to-end group-commit latency,
  not just the fsync time.
- `raft_log_persister_sync_latency_ms` is kept in milliseconds because
  production fsync latencies are typically sub-second and the histogram buckets
  (0.5 ms – 5 s) give good resolution.

### 3. Log Replication

| Name | Type | Labels | Description |
|------|------|--------|-------------|
| `raft_transport_peer_lag_entries` | gauge | `node_id`, `peer_id` | Replication lag for a peer: `last_log_index - match_index` |
| `raft_transport_peer_connected` | gauge | `peer_id` | `1` if transport considers peer connected, else `0` |
| `transport_peer_state` | gauge | `peer_id` | Transport-level peer state enum |

**Update sites**
- `raft_transport_peer_lag_entries`:
  `SetPeerReplicationLagMetricLocked(peer_id)` is invoked whenever
  `match_index_` changes:
  - successful `HandleAppendEntriesResponse`
  - log-mismatch back-off in `HandleAppendEntriesResponse`
  - `HandleInstallSnapshotResponse` on snapshot completion
  - `BecomeLeaderLocked` initialization
  - membership change apply paths (`AddNode`, `AddLearner`, `RemoveNode`)
- `raft_transport_peer_connected` / `transport_peer_state`: network transport
  callbacks registered in `RaftNode::RaftNodeImpl::Start`.

### 4. Client / Read Path

| Name | Type | Labels | Description |
|------|------|--------|-------------|
| `raft_propose_total` | counter | `node_id`, `result` | Total proposals by result |
| `raft_readindex_total` | counter | `node_id` | Total ReadIndex requests |
| `raft_readindex_lease_total` | counter | `node_id` | Lease-backed ReadIndex requests |
| `raft_proposal_latency_seconds` | histogram | `node_id` | End-to-end proposal latency |
| `raft_readindex_latency_seconds` | histogram | `node_id` | End-to-end ReadIndex latency |

(These existed before this work; they are listed for completeness.)

## Implementation Details

### Lock Context

All new metric helpers read state that is already protected by the caller's
locks.  No additional mutexes are introduced.

- `UpdateLeaderLeaseMetricLocked` must be called while holding `election_mtx_`.
  `leader_lease_expiry_` is only written while `election_mtx_` is held (see
  `log_replicator.cpp`), so reading it under the same lock is race-free.
- `SetPeerReplicationLagMetricLocked` reads `match_index_` and therefore must
  be called from a context that already protects `match_index_`:
  - `log_replicator.cpp` paths hold `election_mtx_` + `replication_mtx_`.
  - `snapshot_manager.cpp` holds the same hierarchy.
  - `election_manager.cpp` `BecomeLeaderLocked()` holds `election_mtx_`; leader
    state initialization is serialized there.
  - Membership-change paths in `raft_node_core.cpp` hold the full hierarchy.
  - `membership_manager.cpp` `ApplyConfigChangeLocked()` holds `membership_mtx_`
    when mutating `match_index_`; the helper is called immediately after the
    mutation.

### Metric Helpers

Two helpers were added to `RaftNodeImpl` to keep metric updates centralized:

```cpp
// Must hold election_mtx_.
void UpdateLeaderLeaseMetricLocked();

// Must hold replication_mtx_ (or a path that accesses match_index_ safely).
void SetPeerReplicationLagMetricLocked(NodeId peer_id);
```

Both are no-ops when `metrics_` is null (i.e. `config.metrics_enabled == false`).

### Stale Time-Series Cleanup

`MetricsRegistry` provides `RemoveCounter`, `RemoveGauge`, and
`RemoveHistogram`.  When a node is removed from the cluster, the corresponding
`raft_transport_peer_lag_entries` gauge is removed so the time series does not
linger at the last reported value.

### Group Commit Metrics

`GroupCommitController` now accepts an optional `MetricsRegistry*` and an
optional label map.  It exposes:

```cpp
void UpdateMetricsLocked() const;
```

which is called on every state change that affects pending/unsynced counts.
The lag-ms metric is recorded only when an epoch becomes durable, because the
goal is to measure how long a batch waited between flush and sync.

### WAL Fsync Latency

`LogPersister` already measures `Sync()` latency for group-commit and
`kSyncEveryWrite` paths.  The histogram is emitted as
`logpersister_sync_latency_ms`.  This is the fsync latency observable from the
log persistence layer.

## Label Cardinality

- `node_id`: fixed to the configured node ID.
- `peer_id`: one time series per peer currently in the cluster configuration.

No high-cardinality labels (e.g. log index, term) are used.

## Testing

| Test | Type | Coverage |
|------|------|----------|
| `GroupCommitControllerTest.MetricsExposePendingAndUnsynced` | unit | Pending epochs / unsynced entries gauges reset after sync |
| `MetricsEndpointTest.MetricsShowLeaderLeaseValid` | integration | Leader reports `raft_leader_lease_valid{node_id="N"} 1` after quorum acks |
| `MetricsEndpointTest.MetricsShowPeerLag` | integration | Leader exposes `raft_transport_peer_lag_entries` lines for every follower |

All existing metrics tests continue to pass.

## Future Work

- Add `raft_log_disk_usage_bytes` once WAL and LevelDB directory sizes are
  cheaply queryable.
- Consider summarizing transport peer RTT if the network layer exposes it.
- Convert `logpersister_sync_latency_ms` to seconds if the project later
  standardizes on second-based histograms.
