# Multi-Raft T4 Design Spike

**Author:** @GeoHot  
**Date:** 2026-07-01  
**Scope:** Minimum viable activation design for Multi-Raft on top of the existing `group_id` wire-protocol stub (`b3917e2`).  
**Status:** Design draft — pending architecture review by @Jack and product confirmation by @aq1uuu.

---

## 1. Goal

Define the smallest set of changes that turns the existing single-group `RaftNode` into a multi-group system where one process can host **N independent Raft consensus groups** sharing node-level resources (network, timers, metrics HTTP server) while keeping group-local state isolated.

Non-goals for this spike:
- Automatic sharding / rebalancing / placement driver.
- Cross-group transaction or atomic multi-group commit.
- Heartbeat coalescing (evaluated as Phase 2 optimization).

Target validation scale: **100 groups per node** with reasonable memory/CPU overhead.

---

## 2. Reference Point: Existing `group_id` Stub

Commit `b3917e2` already added the protocol envelope:

| Layer | Current State | Gap to Multi-Raft |
|-------|---------------|-------------------|
| `include/rollingraft/rpc.h` | `RaftRequest`/`RaftResponse` carry `uint64_t group_id = 0` | Used only as a passthrough today; no routing logic consumes it. |
| `src/json_protocol.cpp` | Serializes/deserializes `group_id` with backward-compatible default `0` | Complete for JSON; protobuf protocol (if later added) needs the same field. |
| `src/asio_network_transport.cpp` | `ExtractGroupId()` parses JSON and dispatches to `group_request_handler_` stub, or returns `MULTI_RAFT_NOT_ENABLED` | Need a real `RaftStore` handler registered via `SetGroupRequestHandler()`. |

This gives us a **backward-compatible wire format** for free: old single-group peers ignore unknown `group_id`, new multi-raft peers default missing `group_id` to 0.

---

## 3. Recommended Architecture: "Store" Model (Option A Lightweight)

We introduce a single top-level entity `RaftStore` that owns shared infrastructure and manages a table of `RaftGroup` instances.

```text
┌─────────────────────────────────────────────────────────────┐
│                         RaftStore                           │
│  ┌─────────────────────────────────────────────────────┐    │
│  │           Shared Node Infrastructure                │    │
│  │  NetworkTransport (1)   TimerService (1)            │    │
│  │  Protocol (1)           MetricsHttpServer (1)       │    │
│  │  RuntimeConfig (1)      ConnectionPool / thread pool│    │
│  └─────────────────────────────────────────────────────┘    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
│  │ Group 1  │  │ Group 2  │  │ Group N  │   (RaftGroup)     │
│  │ RaftGroup│  │ RaftGroup│  │ RaftGroup│                   │
│  └──────────┘  └──────────┘  └──────────┘                   │
└─────────────────────────────────────────────────────────────┘
```

Why Option A over the "lightweight group" or "library-first" models:
- Keeps the public `RaftNode` API intact for single-group users.
- Provides a natural place for future heartbeat coalescing, group lifecycle, and placement hooks.
- Matches the proven TiKV/CockroachDB shape without requiring a full rewrite.
- The pre-research memo estimated 5-6 weeks for this model; this spike scopes the first 2-3 week MVP slice.

---

## 4. `RaftStore` Top-Level API Draft

```cpp
namespace rollingraft {

struct RaftStoreConfig {
  NodeId node_id;                       // This physical node's identity
  std::string listen_addr;              // Shared listen address for all groups
  std::string data_dir;                 // Base directory; groups store under <data_dir>/groups/<gid>/
  bool tls_enabled = false;
  std::string tls_cert_file;
  std::string tls_key_file;
  std::string tls_ca_file;

  // Shared infra factories (optional; defaults constructed if null)
  std::function<std::unique_ptr<NetworkTransport>()> network_factory = nullptr;
  std::function<std::unique_ptr<TimerService>()> timer_factory = nullptr;
  std::function<std::unique_ptr<Protocol>()> protocol_factory = nullptr;

  // Global limits
  size_t max_groups = 100;              // Hard limit for MVP
  bool metrics_enabled = true;
  std::string metrics_addr;             // e.g. "0.0.0.0:9001"
};

struct RaftGroupOptions {
  uint64_t group_id;                    // Must be unique within the store
  std::vector<std::string> peers;       // Peer node addresses (same for all groups in homogeneous setup)
  std::vector<NodeId> peer_node_ids;    // Optional explicit peer IDs
  // Per-group overrides of RaftNodeConfig timing/snapshot params
  uint32_t election_timeout_ms = 300;
  uint32_t heartbeat_interval_ms = 50;
  uint32_t snapshot_threshold_entries = 10000;
  // ... other group-local settings
};

class RaftStore {
 public:
  explicit RaftStore(RaftStoreConfig config);
  ~RaftStore();

  // Lifecycle
  Status Start();
  Status Stop();

  // Group lifecycle
  Status CreateGroup(uint64_t group_id,
                     const RaftGroupOptions& options,
                     std::shared_ptr<StateMachine> state_machine);
  Status RemoveGroup(uint64_t group_id);   // Graceful shutdown + data deletion
  Status ShutdownGroup(uint64_t group_id); // Stop but keep data

  // Access
  RaftGroup* GetGroup(uint64_t group_id);
  std::vector<uint64_t> ListGroups() const;

  // Shared infra accessors (for tests/advanced use)
  NetworkTransport& GetNetworkTransport();
  TimerService& GetTimerService();
  MetricsRegistry& GetMetricsRegistry();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rollingraft
```

### 4.1 `RaftGroup` Public Surface

`RaftGroup` is the multi-raft equivalent of today's `RaftNode`, but it does **not** own transport or timers. It exposes the same logical operations:

```cpp
class RaftGroup {
 public:
  bool IsLeader() const;
  Status Propose(const std::string& command, ProposeCallback cb,
                 uint64_t session_id = 0, uint64_t seq_num = 0);
  Status ReadIndex(ReadIndexCallback cb);
  Status AddNode(NodeId id, const NodeAddr& addr);
  Status RemoveNode(NodeId id);
  Status TriggerSnapshot();
  Status TransferLeadershipTo(NodeId target);
  // ...
};
```

Backward compatibility: a standalone `RaftNode` can be implemented internally as a `RaftStore` with exactly one group, preserving the existing API.

---

## 5. `group_id` Propagation Path

### 5.1 RPC

```text
Client/Peer                         Receiver
   │                                   │
   ├─ RaftRequest.group_id set ───────>│
   │   JsonProtocol serializes         │
   │   { "group_id": 7, ... }          │
   │                                   │
   │                                   ├─ AsioNetworkTransport.ExtractGroupId()
   │                                   ├─ lookup group_id in RaftStore table
   │                                   └─ dispatch to RaftGroup::HandleIncomingRpc()
```

Required changes:
- `AsioNetworkTransport` already extracts `group_id` from JSON. Register `RaftStore::OnIncomingRpc` as `group_request_handler_`.
- Outbound messages must set `group_id` before serialization. `RaftGroup` stores its own `group_id_` and sets it on every `RaftRequest`/`RaftResponse` it builds.

### 5.2 WAL / Persister

Current `Persister` interface operates on a single `data_dir`. For multi-raft we need per-group namespaces.

**Chosen approach: shared LevelDB instance with key prefixing (Option B from pre-research).**

Rationale:
- Avoids N open databases and file-descriptor exhaustion.
- Natural fit with existing `LevelDBPersister` and `HybridPersister`.
- WAL separation work (T3) already treats log as files in a directory; per-group sub-directory is trivial.

Key namespace layout:

```text
<data_dir>/
  groups/
    <group_id>/
      state/              -> LevelDB column-family-equivalent via key prefix
        key: "m:<binary_key>"
      wal/                -> WAL segment files (per-group WAL directory)
      snapshots/          -> snapshot chunks
```

Internal key format for shared LevelDB:

```cpp
std::string MakePrefixedKey(uint64_t group_id, const std::string& type, const std::string& key) {
  // Fixed-width big-endian group_id for clean iteration boundaries.
  char prefix[8];
  prefix[0] = static_cast<char>((group_id >> 56) & 0xFF);
  prefix[1] = static_cast<char>((group_id >> 48) & 0xFF);
  prefix[2] = static_cast<char>((group_id >> 40) & 0xFF);
  prefix[3] = static_cast<char>((group_id >> 32) & 0xFF);
  prefix[4] = static_cast<char>((group_id >> 24) & 0xFF);
  prefix[5] = static_cast<char>((group_id >> 16) & 0xFF);
  prefix[6] = static_cast<char>((group_id >>  8) & 0xFF);
  prefix[7] = static_cast<char>((group_id      ) & 0xFF);
  return std::string(prefix, sizeof(prefix)) + type + key;
}
```

This implies a new `MultiRaftPersister` adapter:
- Wraps one shared `leveldb::DB`.
- Receives `group_id` on every `Persister` operation.
- Routes keys through `MakePrefixedKey()`.
- For WAL: instantiates one `WALPersister` per group pointing to `<data_dir>/groups/<gid>/wal/`.

### 5.3 Snapshot

- Snapshot metadata and chunks live under `<data_dir>/groups/<gid>/snapshots/`.
- `InstallSnapshot` RPC carries `group_id`; `RaftStore` routes to the target `RaftGroup`.
- Streaming snapshot chunks are keyed by `group_id` in the receiver's `snapshot_manager_` map.

### 5.4 Metrics

Current `MetricsRegistry` uses `{name, labels}` as key. Multi-raft simply adds a `group_id` label:

```text
raft_role{group_id="7",node_id="1"} 2
raft_group_commit_lag_ms{group_id="7",node_id="1"} ...
```

Required changes:
- Each `RaftGroup` passes `{"group_id", std::to_string(group_id)}` as a base label set.
- `RaftStore` owns a single `MetricsRegistry` and `MetricsHttpServer`; groups register/remove their series on creation/removal.
- Add `MetricsRegistry::RemoveCounter/Gauge/Histogram` calls on `RemoveGroup()` to prevent stale time series (API already exists after PR #8).

---

## 6. Lock Hierarchy & Resource Isolation

### 6.1 Group-Local vs. Shared State

| Concern | Today (`RaftNodeImpl`) | Multi-Raft Placement |
|---------|------------------------|----------------------|
| `current_term_`, `voted_for_`, `log_` | `RaftNodeImpl` | `RaftGroup` |
| `commit_index_`, `last_applied_`, `role_` | `RaftNodeImpl` | `RaftGroup` |
| Leader state (`next_index_`, `match_index_`, `client_sessions_`) | `RaftNodeImpl` | `RaftGroup` |
| `network_`, `timer_`, `protocol_` | `RaftNodeImpl` | `RaftStore` (shared infra) |
| `metrics_server_`, `runtime_config_` | `RaftNodeImpl` | `RaftStore` (shared infra) |
| `event_bus_` | `RaftNodeImpl` | Per-group `EventBus` or filtered shared bus |
| `StateMachine` | User-provided | User-provided, one per group |
| `Persister` | `RaftNodeImpl` owns | `MultiRaftPersister` adapter, shared backend |

### 6.2 Proposed Lock Hierarchy

```text
RaftStore::groups_mtx_              // Guards group table (create/remove/lookup)
  └── RaftGroup::election_mtx_      // Per-group Raft state machine
        └── RaftGroup::replication_mtx_
              └── Persister shard lock (if any)
```

Rules:
1. Always acquire `groups_mtx_` before touching the group table.
2. Never hold `groups_mtx_` while calling into a `RaftGroup` method that may block on I/O.
3. `RaftGroup` internal locks remain unchanged; only the transport/timer callbacks now need to locate the group via the store table.
4. Shared infrastructure (transport, timer, metrics) must be lock-free or strand-isolated, which the current ASIO implementation already guarantees.

### 6.3 Shared Timer Tick

Instead of N per-group timers, use a single periodic tick in `RaftStore`:

```cpp
void RaftStore::OnTick() {
  std::shared_lock lock(groups_mtx_);
  for (auto& [gid, group] : groups_) {
    group->Tick();
  }
}
```

- `RaftGroup::Tick()` advances election/heartbeat timers internally.
- For 100 groups this is ~100 cheap state checks per tick; negligible CPU.
- Later optimizations (Hibernate Region, coalescing) build on this hook.

### 6.4 Transport Routing

```cpp
void RaftStore::OnIncomingRpc(NodeId from, uint64_t group_id,
                              const std::string& data, std::string& response) {
  std::shared_lock lock(groups_mtx_);
  auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    response = JsonError("GROUP_NOT_FOUND", group_id);
    return;
  }
  it->second->HandleIncomingRpc(from, data, response);
}
```

The transport's existing single-group handler remains for `group_id == 0`, ensuring backward compatibility.

---

## 7. 100-Group Target: Risks & Fallback

| Risk | Severity | Mitigation / Fallback |
|------|----------|-----------------------|
| Memory explosion (N × group state) | High | Store enforces `max_groups`; Hibernate Region (Phase 2) limits idle group memory; fallback to lower limit. |
| Apply thread explosion (N groups × 1 thread) | High | Share one apply thread pool across groups; default pool size = `std::min(8, hardware_concurrency)`. |
| Lock contention on shared transport | Low-Medium | Transport is strand-per-connection; benchmark with 100 groups to confirm. |
| Shared LevelDB write amplification | Medium | Per-group WAL directory isolates log fsync; state DB uses WriteBatch; fallback to separate DB per group if profiling shows L0 compaction pressure. |
| Timer tick latency head-of-line | Low | Tick work is O(groups) and non-blocking; if one group is slow, move heavy work to background tasks. |
| Metrics cardinality explosion | Medium | 100 groups × ~20 metrics = 2k series; acceptable for Prometheus; Remove APIs prevent leaks. |
| Testing matrix explosion | Medium | Deterministic tests can host multiple groups in one process; integration tests run 2 groups on 3 nodes. |
| Snapshot transfer fan-out | Medium | Limit concurrent snapshot sends per group (already exists); per-group `snapshot_manager_`. |

### 7.1 MVP Scope Fallback

If the full Store model proves too large for the initial spike:

1. **Phase 0 (minimum):** `RaftStore` manages exactly 2 groups sharing one transport; no shared timer, no shared persister — just prove routing works.
2. **Phase 1 (MVP):** Add shared timer + per-group `RaftGroup` with separate persister instances (separate LevelDB per group).
3. **Phase 2 (optimization):** Shared LevelDB with prefixing, Hibernate Region, heartbeat coalescing.

Recommended path: implement **Phase 0 + Phase 1** in the first implementation PR, leaving Phase 2 as follow-up.

---

## 8. Open Questions / Decisions Needed

1. **Target group count:** Is 100 the hard ceiling for v0.5.0, or a profiling milestone?
2. **Homogeneous vs. heterogeneous groups:** Can different groups have different peer sets, or do all groups share the same node topology?
3. **StateMachine lifecycle:** Should `RaftStore` create `StateMachine` instances, or does the user pass them per `CreateGroup()`?
4. **Client API:** Does `Client` take an explicit `group_id` in `Execute(group_id, command)`, or do we introduce `Client::BindGroup(group_id)`?
5. **Protobuf protocol:** If protobuf RPC is added later, `group_id` must be in the proto envelope. The JSON path already validates the concept.
6. **Placement driver:** Out of scope, but should `RaftStore` API leave room for `MigrateGroup` / `SplitGroup` hooks?

---

## 9. Suggested Implementation Plan

| Step | Work | Est. | Validation |
|------|------|------|------------|
| 1 | Introduce `RaftStoreConfig`, `RaftGroupOptions`, `RaftStore` skeleton; keep `RaftNode` API as wrapper. | 0.5 d | Compiles; existing tests pass. |
| 2 | Extract `RaftGroup` from `RaftNodeImpl`: move group-local state out, inject shared infra via `SharedNodeInfra`. | 1 d | Single-group `RaftNode` wrapper still passes 358 tests. |
| 3 | Wire transport routing: register `RaftStore::OnIncomingRpc` as `group_request_handler_`. | 0.5 d | New integration test: two groups on three nodes elect leaders independently. |
| 4 | Shared timer tick + per-group timer advance. | 0.5 d | Deterministic multi-group test passes. |
| 5 | `MultiRaftPersister` adapter with group key prefixing; per-group WAL directory. | 1 d | 100-group unit test: create, propose, restart, verify isolation. |
| 6 | Metrics `group_id` label + cleanup on group removal. | 0.5 d | `/metrics` shows per-group series. |
| 7 | Documentation + integration test `test_multi_raft_2groups.cpp`. | 0.5 d | CI green. |

**Total estimate:** 4-5 engineering days for MVP (slightly under the 2-day spike, but the spike output is a design document; implementation is a follow-up task).

---

## 10. Conclusion

Multi-Raft activation is feasible with a **Store model** that shares transport, timers, protocol, and metrics while isolating group-local state, persister namespaces, and state machines. The existing `group_id` wire-protocol stub removes the serialization/routing unknown and provides backward compatibility.

Next action: architecture review by @Jack, then product confirmation by @aq1uuu before cutting implementation tasks.
