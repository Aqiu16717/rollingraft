# Multi-Raft Pre-Research Memo

**Author:** @Jack (Distributed Systems Architect)  
**Date:** 2026-06-03  
**Scope:** Preliminary architectural research — no implementation decisions  
**Target Audience:** RollingRaft core team (Cindy, Alice, Tom, GeoHot)

---

## 1. Executive Summary

Multi-Raft is the pattern of running **many independent Raft consensus groups** on a single physical node (or process). It is the enabling architecture for horizontally-scaled systems like TiKV, CockroachDB, and YugabyteDB. This memo surveys production multi-raft designs, maps them onto RollingRaft's current architecture, and identifies the key engineering decisions we would face if we pursue this direction.

**Bottom line:** RollingRaft's existing abstractions (`NetworkTransport`, `Persister`, `TimerService`, `Protocol`) are already well-factored for sharing. The tight coupling inside `RaftNodeImpl` between **group state** and **node infrastructure** is the primary obstacle. A multi-raft migration is feasible but non-trivial — estimated 4-6 weeks of focused work for an MVP.

---

## 2. Why Multi-Raft?

| Problem | Single Raft Group | Multi-Raft |
|---------|-------------------|------------|
| Throughput ceiling | Bound by leader's single-threaded log | N groups × leader throughput |
| Data size | Entire dataset replicated to every node | Sharded dataset; each node holds subset |
| Recovery time | Full dataset must be restored | Only failed group's shards need recovery |
| Geographic placement | All nodes in same region | Each group can be placed independently |

RollingRaft does not currently need multi-raft for its embedded/consensus-library use case. However, if the product vision includes **a distributed KV store, a sharded metadata service, or a multi-tenant coordination plane**, multi-raft becomes a strategic capability.

---

## 3. Reference Architectures

### 3.1 TiKV / TiDB (PingCAP)

- **Unit of sharding:** `Region` — a contiguous key range (~96MB default)
- **Raft mapping:** 1 Region = 1 Raft group (3-5 replicas)
- **Node role:** `Store` — a TiKV process hosts ~tens of thousands of Regions
- **Key optimizations:**
  - **Raftstore:** Single (or multi-threaded) event loop driving all Raft groups
  - **Batching:** `WriteBatch` for persisting ready entries from multiple groups atomically
  - **Hibernate Region:** Idle regions skip ticks → no heartbeat generation
  - **Shared Transport:** One gRPC connection per peer node, multiplex all Region messages
- **Architecture:** `Store` → `Peer` (per-Region Raft state machine) → `Apply` system

### 3.2 CockroachDB (Cockroach Labs)

- **Unit of sharding:** `Range` — similar to TiKV Region
- **Raft mapping:** 1 Range = 1 Raft group
- **Node role:** `Store` — hosts many Ranges
- **Key optimizations:**
  - **MultiRaft layer:** Coalesces heartbeats across all Ranges sharing the same node pair
  - **Per-node-pair heartbeat:** Instead of N Range heartbeats, one aggregated heartbeat per (node_A, node_B) pair
  - **Raft log separation:** Each Range has its own log, but storage engine (Pebble) is shared
- **Architecture:** `Store` → `Replica` (per-Range) → `Raft` (via etcd-raft library)

### 3.3 etcd-raft Library

- **Design philosophy:** Minimalist — only the core Raft algorithm, no transport/storage
- **Multi-raft support:** The library itself is state-machine style; users create multiple `RawNode` instances
- **Message model:** User polls `Ready` from each `RawNode`, batches writes, sends messages
- **Relevance to RollingRaft:** Our architecture is closer to etcd-raft's minimalism than to TiKV's monolithic raftstore. This is a strength — we can add multi-raft without a ground-up rewrite.

---

## 4. RollingRaft Architecture Assessment

### 4.1 Current Architecture (Single Group)

```
┌─────────────────────────────────────────┐
│           RaftNode (public API)         │
├─────────────────────────────────────────┤
│         RaftNodeImpl (PIMPL)            │
│  ┌─────────┐ ┌──────────┐ ┌─────────┐  │
│  │ Election│ │ Log Repl │ │Snapshot │  │
│  │ Manager │ │(built-in)│ │ Manager │  │
│  └─────────┘ └──────────┘ └─────────┘  │
│  ┌─────────┐ ┌──────────┐ ┌─────────┐  │
│  │  RPC    │ │  Async   │ │Membership│  │
│  │Handlers │ │  Apply   │ │ Manager │  │
│  └─────────┘ └──────────┘ └─────────┘  │
├─────────────────────────────────────────┤
│ NetworkTransport │ Persister │StateMachine│
│   (Asio TCP)     │ (LevelDB) │ (User)    │
└─────────────────────────────────────────┘
```

### 4.2 Multi-Raft Readiness Matrix

| Component | Shareable? | Effort | Notes |
|-----------|-----------|--------|-------|
| `NetworkTransport` | **Yes** — already abstracted | Low | Need `group_id` in wire protocol for routing |
| `TimerService` | **Yes** | Low | One timer wheel driving many groups |
| `Protocol` (JSON) | **Yes** | Low | Need `group_id` field in envelope |
| `Persister` | **Partial** | Medium | Can share LevelDB instance with key prefixing; WAL separation (T3) simplifies this |
| `StateMachine` | **No** | N/A | Per-group by definition |
| `RaftNodeImpl` | **No** | High | Contains group-specific state (term, log, config, leader state) |
| `MetricsHttpServer` | **Yes** | Low | Add group dimension to metrics labels |
| `EventBus` | **Partial** | Low | Per-group event bus, or shared with group_id filtering |

### 4.3 Critical Gap: `RaftNodeImpl` Monolith

`RaftNodeImpl` currently conflates two concerns:

1. **Group-local state** (must be replicated per group):
   - `current_term_`, `voted_for_`, `log_`, `commit_index_`
   - `next_index_`, `match_index_` (leader state)
   - `cluster_config_`, `pending_proposals_`, `pending_reads_`
   - `client_sessions_`, `session_manager_`

2. **Node-local infrastructure** (can be shared):
   - `network_`, `timer_`, `protocol_`
   - `metrics_server_`, `runtime_config_`
   - Thread pools, connection pools

To support multi-raft, we need a **structural decomposition** that separates these two layers.

---

## 5. Design Options for RollingRaft

### 5.1 Option A: "Store" Model (TiKV-style)

**Concept:** Introduce a `RaftStore` top-level entity that owns shared infrastructure and manages a `HashMap<group_id, RaftGroup>`.

```
┌─────────────────────────────────────────────┐
│              RaftStore                        │
│  ┌─────────────────────────────────────┐    │
│  │  Shared Infrastructure              │    │
│  │  - NetworkTransport (1 instance)    │    │
│  │  - TimerService (1 instance)        │    │
│  │  - Protocol (1 instance)            │    │
│  │  - MetricsServer (1 instance)       │    │
│  │  - ConnectionPool (1 instance)      │    │
│  └─────────────────────────────────────┘    │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐       │
│  │Group #1 │ │Group #2 │ │Group #N │       │
│  │RaftGroup│ │RaftGroup│ │RaftGroup│       │
│  └─────────┘ └─────────┘ └─────────┘       │
└─────────────────────────────────────────────┘
```

**Pros:**
- Clean separation of concerns
- Natural fit for sharding (each group = one shard)
- Heartbeat coalescing is straightforward at Store level
- Matches proven production architecture (TiKV)

**Cons:**
- Large refactor of `RaftNode` public API
- Need new `RaftStoreConfig`, `RaftGroupConfig` hierarchies
- Client API changes (must specify `group_id`)

**Estimated effort:** 5-6 weeks

### 5.2 Option B: "Lightweight Group" Model

**Concept:** Keep `RaftNode` as the user-facing API for backward compatibility, but internally extract a `RaftGroupState` struct that is lightweight and cheap to instantiate many times. Share transport/timer/protocol via dependency injection.

```
┌─────────────────────────────────────────────┐
│  User creates N RaftNode instances            │
│  Each RaftNode holds:                         │
│    - RaftGroupState (group-local)             │
│    - shared_ptr<SharedNodeInfra> (shared)     │
├─────────────────────────────────────────────┤
│  SharedNodeInfra:                             │
│    - NetworkTransport, TimerService, Protocol │
│    - Thread pool for async apply              │
│    - Connection pool                          │
└─────────────────────────────────────────────┘
```

**Pros:**
- Minimal public API changes
- Backward compatible (single `RaftNode` still works)
- Incremental adoption

**Cons:**
- Each `RaftNode` still carries some overhead (own thread, own metrics)
- Harder to implement heartbeat coalescing (groups are loosely coupled)
- Risk of resource explosion (N threads for N groups)

**Estimated effort:** 3-4 weeks

### 5.3 Option C: "Library-First" Model (etcd-raft style)

**Concept:** Refactor the core algorithm into a pure state-machine library (`RaftCore`) with no I/O. The user drives it by feeding messages and ticks, and handles the `Ready` output. Multi-raft becomes trivial: create N `RaftCore` instances.

```
// Hypothetical API
auto core = RaftCore::New(config, storage);
core->Tick();           // Advance time
core->Step(msg);        // Process inbound message
Ready ready = core->Ready();  // Get pending writes, messages, commits
```

**Pros:**
- Most testable and deterministic
- Ultimate flexibility for users
- Small memory footprint per group

**Cons:**
- Massive API break — essentially a rewrite of `RaftNodeImpl`
- Users must implement their own transport, apply loop, timer coordination
- RollingRaft's value proposition (batteries included) is weakened

**Estimated effort:** 8-10 weeks

---

## 6. Key Engineering Decisions (if we proceed)

### 6.1 Wire Protocol: Adding `group_id`

All RPC messages (`RaftRequest`, `RaftResponse`) need a `group_id` field for routing. Options:

| Approach | Change Scope | Backward Compatible? |
|----------|-------------|---------------------|
| A. Add `group_id` to base struct | All messages get it | No (serialization break) |
| B. Wrap in envelope: `RaftMessageEnvelope { group_id; payload }` | Protocol layer only | Yes (if envelope is optional for group 0) |
| C. Use separate port per group | No code changes | Yes, but wasteful and unscalable |

**Recommendation:** Option B with envelope. `group_id = 0` means "legacy single-group mode." The `Protocol` layer handles envelope wrapping/unwrapping transparently.

### 6.2 Transport Multiplexing

Current `AsioNetworkTransport` maintains one TCP connection per peer. For multi-raft, this is actually sufficient — we just need to route inbound messages to the correct group based on `group_id`.

**Required change:** The `RpcRequestHandler` callback (currently `void(NodeId, string&, string&)`) needs to become `void(NodeId, group_id, string&, string&)` or the handler itself does demux.

### 6.3 Heartbeat Coalescing

Without coalescing, a node in 1,000 groups with 5 replicas each generates ~1,000 heartbeats per interval. With coalescing, it generates ~4 (one per unique peer).

**CockroachDB approach:** Maintain a per-(node, peer) heartbeat message that aggregates all groups' heartbeat metadata. Send one physical message.

**TiKV approach (Hibernate Region):** Skip ticks for idle regions. No heartbeat generation at all until activity resumes.

**Recommendation:** Start with Hibernate Region (simpler, good enough for moderate group counts). Add CockroachDB-style coalescing only if profiling shows it as a bottleneck.

### 6.4 Storage Isolation

| Option | Isolation Level | Effort | Notes |
|--------|----------------|--------|-------|
| A. Separate LevelDB per group | Strong | Low | N open databases; OS file descriptor limit |
| B. Shared LevelDB with key prefixing | Namespace | Medium | Key format: `group_id + type + key` |
| C. Shared WAL (T3) + separate state | Hybrid | Medium-High | WAL per group; StateMachine state in shared KV |

**Recommendation:** Option B for MVP. Option C after T3 WAL separation is mature.

### 6.5 Timer Sharing

Current `TimerService` uses one Asio `io_context`. For multi-raft:

- **Single shared timer wheel:** One periodic tick (e.g., 100ms) iterates all groups, calling `Tick()` on each. Simple but may cause head-of-line latency if one group is slow.
- **Per-group timer with shared executor:** Each group registers its own timers, but callbacks run on a shared thread pool. Better isolation.

**Recommendation:** Single shared tick with per-group state-machine advance. This is what TiKV does and it works for 10k+ groups.

---

## 7. Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| Memory explosion (N groups × memory) | High | Hibernate Region + limit max groups per node |
| Lock contention on shared transport | Medium | Transport already lock-free (Asio strand per connection) |
| Apply thread explosion (N groups × 1 thread) | High | Share apply thread pool across groups |
| Serialization break | Medium | Envelope approach maintains backward compat |
| Testing matrix explosion | Medium | Deterministic tests can simulate multi-group in one process |
| Scope creep | High | Strictly define MVP: 2 groups on 3 nodes, no coalescing |

---

## 8. Recommended Next Steps (if prioritized)

1. **Decision Gate:** Team vote on whether multi-raft aligns with product vision (1 day)
2. **Spike:** Implement `group_id` envelope in Protocol + transport routing (2-3 days)
3. **Spike:** Extract `RaftGroupState` from `RaftNodeImpl` — compile-time only, no behavior change (3-4 days)
4. **Design Review:** Choose Option A, B, or C based on spike findings (1 day)
5. **MVP Spec:** Define MVP scope: shared transport + 2 groups + no coalescing (2-3 days)

---

## 9. Open Questions

1. What is the target group count per node? (100? 10,000? This drives design choices.)
2. Does the product vision require automatic sharding and rebalancing, or just manual group creation?
3. Should groups share the same `StateMachine` type, or support heterogeneous state machines?
4. How does multi-raft interact with T3 WAL separation? (One WAL per group = natural fit.)
5. Do we need a Placement Driver equivalent for group scheduling?

---

## 10. References

- TiKV Multi-Raft Design: https://www.pingcap.com/blog/design-and-implementation-of-multi-raft/
- CockroachDB Scaling Raft: https://www.cockroachlabs.com/blog/scaling-raft/
- etcd-raft Library: https://github.com/etcd-io/raft
- TiKV Raftstore Performance Tuning: https://tikv.org/blog/tune-with-massive-regions-in-tikv/
- Sergei Turukin — What is MultiRaft: https://sergeiturukin.com/2017/06/09/multiraft.html
