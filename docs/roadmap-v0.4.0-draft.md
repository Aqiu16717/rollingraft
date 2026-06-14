# RollingRaft v0.4.0 Roadmap (Draft)

> **Status**: Draft — awaiting product decision on T4 Multi-raft scope  
> **Date**: 2026-06-14  
> **Author**: @Jack (Distributed Systems Architect)  
> **Assumed direction**: Multi-raft + observability enhancement  
> **Backup direction**: Performance + operations tooling (if T4 deferred)

---

## 1. Executive Summary

This roadmap proposes **v0.4.0** as the "Multi-Raft & Production Observability" release. It builds on the solid foundation of v0.3.x (WAL separation, protobuf serialization, atomic snapshots, CI/docs maturity) and adds the architectural capability to run **multiple independent Raft groups** within a single process.

**Why now?**
- v0.3.x has closed the major safety, performance, and durability gaps
- The `group_id` wire protocol spike (`b3917e2`) proved zero-regression feasibility
- T3 WAL separation creates natural per-group storage isolation
- The team has bandwidth after four consecutive releases

**Decision required before execution**: User must confirm target group count, target scenario, and timeline.

---

## 2. Target Scenarios

### 2.1 Primary Scenario: Sharded Metadata / Coordination Service

A single RollingRaft process hosts **10–1,000 Raft groups**, where each group manages an independent shard of metadata (e.g., service discovery entries, job queues, configuration namespaces).

**Success criteria**:
- Create N groups programmatically via `RaftStore::CreateGroup(group_id, config, peers)`
- Each group elects its own leader independently
- Groups share one TCP connection per peer node
- 100-group cluster passes deterministic chaos tests

### 2.2 Secondary Scenario: Multi-Tenant Coordination Plane

Multiple tenants each get one or more Raft groups. Groups are logically isolated but share physical infrastructure.

**Success criteria**:
- Per-group metrics and event streams
- Per-group snapshot lifecycle
- No cross-group state leakage

### 2.3 Non-Goal for v0.4.0

- **Automatic sharding / rebalancing** (Placement Driver equivalent)
- **Cross-group transactions**
- **Heterogeneous state machine types per group**

These are v0.5.0+ capabilities.

---

## 3. Proposed Architecture

### 3.1 High-Level Design: Store + Group Two-Layer Model

We adopt a **lightweight Store model** (inspired by TiKV Raftstore but without the monolithic event loop):

```
┌─────────────────────────────────────────────────────────────┐
│                        RaftStore                              │
│  ┌───────────────────────────────────────────────────────┐  │
│  │           Shared Infrastructure                        │  │
│  │  - NetworkTransport (1 instance per peer-node pair)    │  │
│  │  - TimerService (1 shared tick wheel)                  │  │
│  │  - Protocol (JSON + optional protobuf)                 │  │
│  │  - MetricsHttpServer (with group_id labels)            │  │
│  │  - RuntimeConfig (hot reload)                          │  │
│  │  - Shared apply thread pool                            │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌─────────────┐ ┌─────────────┐     ┌─────────────┐       │
│  │  Group #1   │ │  Group #2   │ ... │  Group #N   │       │
│  │ RaftGroup   │ │ RaftGroup   │     │ RaftGroup   │       │
│  │  (lightweight│ │  (lightweight│     │  (lightweight│       │
│  └─────────────┘ └─────────────┘     └─────────────┘       │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Public API Sketch

```cpp
// Shared infrastructure
auto store = RaftStore::New(RaftStoreConfig{...});

// Create or join a group
auto group = store->CreateGroup(
    /*group_id=*/1,
    RaftGroupConfig{...},
    initial_peers,
    std::make_unique<MyStateMachine>()
);

// Use group like today's RaftNode
group->Propose(command, callback);
group->ReadIndex(callback);
```

**Backward compatibility**: existing `RaftNode` API continues to work as a thin wrapper around `group_id = 0`.

### 3.3 Key Subsystems

| Subsystem | v0.3.x State | v0.4.0 Change |
|-----------|-------------|---------------|
| Wire protocol | `group_id` field added, dispatch stub | Real group-aware dispatch |
| Transport | One connection per peer | Shared per peer, multiplexed by group_id |
| Storage | Single-group HybridPersister | Per-group WAL + shared StatePersister with key prefixing |
| Timer | One timer per RaftNode | Shared tick wheel driving all groups |
| Apply | One apply thread per RaftNode | Shared apply thread pool |
| Metrics | Node-level only | Group-level labels |

---

## 4. Phase Plan

### Phase 0: Foundation (v0.3.x already done)

- ✅ `group_id` in RPC base types
- ✅ JSON protocol backward-compatible round-trip
- ✅ Transport dispatch stub
- ✅ WAL separation (T3)
- ✅ Protobuf WAL serialization
- ✅ Atomic snapshot replacement

### Phase 1: Store Abstraction (Week 1–2)

**Goal**: Introduce `RaftStore` and `RaftGroup` without changing behavior for single-group mode.

**Deliverables**:
1. `RaftStore` class owning shared infrastructure
2. `RaftGroup` extracted from `RaftNodeImpl` (group-local state only)
3. `RaftNode` becomes a backward-compatible wrapper
4. All 332 existing tests pass unchanged

**Milestone**: Single-group mode works identically through the new Store/Group abstraction.

### Phase 2: Multi-Group Lifecycle (Week 3–4)

**Goal**: Create, start, stop, and destroy multiple groups in one process.

**Deliverables**:
1. `RaftStore::CreateGroup()` / `DestroyGroup()` / `GetGroup()`
2. Per-group storage isolation (WAL per group + LevelDB key prefixing)
3. Real `GroupRequestHandler` routing inbound messages to the correct group
4. Integration test: 3-node cluster with 5 groups, elect leaders, propose per group

**Milestone**: Multiple groups coexist and operate independently.

### Phase 3: Resource Sharing & Scaling (Week 5–6)

**Goal**: Share expensive resources across groups to support 100+ groups.

**Deliverables**:
1. Shared apply thread pool
2. Shared tick wheel (one timer driving all groups)
3. Hibernate Region: idle groups skip heartbeats
4. Coalesced metrics with `group_id` labels
5. Benchmark: 100-group cluster stability test

**Milestone**: 100 groups run on 3 nodes without resource exhaustion.

### Phase 4: Observability Enhancement (Parallel Track, Week 2–6)

**Goal**: Make multi-raft production-debuggable.

**Deliverables**:
1. Per-group Prometheus metrics (`raft_group_id` label)
2. Group topology endpoint: `/v1/groups`
3. Group event stream via SSE with `group_id` filter
4. Admin API: transfer leadership per group, trigger snapshot per group
5. Operations guide chapter: "Running Multi-Raft Clusters"

**Milestone**: SRE can monitor and operate 100+ groups using existing tooling.

---

## 5. Compatibility

### 5.1 Backward Compatibility

- **Wire protocol**: `group_id = 0` continues to mean "single-group mode." Old v0.3.x peers can interoperate with v0.4.0 peers as long as only group 0 is used.
- **Public API**: `RaftNode` is preserved as a wrapper. Existing users do not need to migrate unless they want multi-raft.
- **Storage**: v0.3.x single-group databases remain compatible. Multi-raft will use a new directory layout.

### 5.2 Upgrade Path

1. Stop node
2. Replace binary with v0.4.0
3. Start node in single-group mode (default)
4. (Optional) Migrate to multi-raft via admin API in a future release

---

## 6. Risks & Mitigations

| Risk | Severity | Mitigation |
|------|----------|------------|
| `RaftNodeImpl` refactor scope expands | High | Strict Phase 1 boundary: extract state, do not change behavior |
| Apply thread pool contention | Medium | Benchmark before/after; keep per-group apply queue optional |
| Storage migration complexity | Medium | Keep single-group storage layout unchanged; new groups use prefixed keys |
| Testing matrix explosion | Medium | Reuse deterministic test harness; simulate multi-group in one process |
| Memory explosion with 100+ groups | High | Hibernate Region + shared infra + configurable max groups |
| Scope creep into auto-sharding | High | Explicit non-goals; require separate v0.5.0 proposal |

---

## 7. Dependencies

- **GeoHot profiling results** (`docs/perf-profiling-2026-06.md`): May shift Phase 3 priorities if a different bottleneck is found
- **User decision on T4**: This roadmap cannot enter execution without confirmation of target group count and scenario
- **Tom audit bandwidth**: Code quality review for large refactors

---

## 8. Alternative Direction (if T4 Deferred)

If the user decides **not** to pursue multi-raft in v0.4.0, the recommended fallback scope is:

1. **Performance**: Leader lease read enhancements, coalesced sync, optional zero-copy deserialization
2. **Operations**: Raft state export, emergency recovery CLI, rolling upgrade guide
3. **Observability**: Distributed tracing hooks, structured logging improvements

Estimated effort: 4–5 weeks.

---

## 9. Open Questions for User

1. **Target group count per node?** (10 / 100 / 1,000 — drives design choices)
2. **Target scenario?** (sharded metadata / multi-tenant coordination / other)
3. **Timeline?** (target v0.4.0 date)
4. **Auto-sharding required eventually?** (affects whether we invest in Placement Driver design now)
5. **State machine homogeneity?** (one `StateMachine` type for all groups, or per-group?)

---

## 10. Recommended Next Steps (if approved)

1. **Decision gate**: User confirms T4 scope (1 day)
2. **Spec review**: Team reviews this roadmap, refines Phase 1 boundaries (2 days)
3. **Spike**: Extract `RaftGroup` from `RaftNodeImpl` — compile-time only (3–4 days)
4. **MVP design**: Finalize `RaftStore` API and storage layout (2 days)
5. **Execution**: Begin Phase 1
