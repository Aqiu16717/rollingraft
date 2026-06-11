# RollingRaft v0.3.0 Release Notes (Draft)

> **⚠️ Version Clarification**: Git tag `v0.2.0` already exists (2026-05-20, commit `72f52eb`).
> The CMakeLists.txt version remains `0.1.0`. This draft covers the changes **since v0.2.0** (33 commits).
> Recommendation: release current HEAD as **v0.3.0** and retroactively document v0.2.0.

---

## Release Overview

RollingRaft v0.3.0 is a major milestone focusing on **production safety**, **performance**, and **operability**. This release introduces enterprise-grade Raft consensus features, a redesigned storage architecture, and comprehensive documentation.

**Test Coverage**: 328 tests (295 unit + 27 integration + 6 deterministic), all passing.
**Supported Platforms**: Linux (GCC 11+, Clang 15+), macOS (Apple Clang 17+).
**Sanitizers**: ASan, TSan, UBSan validated in CI.

---

## 🛡️ Safety & Reliability (P0)

### Pre-vote + CheckQuorum
Pre-vote prevents partitioned nodes from triggering disruptive elections. CheckQuorum ensures a leader must maintain contact with a majority before stepping down. Together they eliminate the classic "flapping leader" problem in unreliable networks.

### Joint Consensus Membership Changes
Safe configuration changes without availability gaps. RollingRaft now supports adding/removing nodes without requiring a snapshot or manual intervention. The implementation follows Diego Ongaro's PhD thesis §4.3.

### TLS/mTLS Encryption
Full node-to-node RPC encryption with X.509 certificate authentication. Supports both one-way TLS and mutual TLS (mTLS). Includes 3-node TLS cluster integration tests.

### Streaming Snapshots
Chunked, incremental snapshot transfer replaces monolithic blob copying. Features:
- Incremental SHA-256 checksum per chunk
- Bounded memory usage during transfer
- Crash-safe cleanup on failure
- Follower-side streaming receiver

### Graceful Shutdown
Configurable shutdown timeout prevents indefinite hangs during node stop. All pending RPCs are drained or forcefully terminated within the timeout window.

---

## ⚡ Performance (P1)

### Async Apply
State machine execution is decoupled from consensus via an independent apply thread and queue. Proposals return immediately after log append; apply happens asynchronously. This is essential for state machines with non-trivial execution costs.

### Pipeline Replication
Replaces the fixed `kMaxPendingAppends=3` window with an ordered inflight window. Under WAN latency (10-50ms), pipeline replication can improve throughput by 3-5× compared to the previous synchronous model.

### Group Commit
Batches multiple log entries into a single `fsync()` call via a dedicated sync thread. Removes the `flushed_index_` blocking that previously serialized all persistence operations.

### Leader Lease Reads
Stale-safe read optimization: the leader can serve read requests without appending to the log or contacting followers, provided the lease hasn't expired. Ideal for read-heavy metadata stores.

### Transport Optimizations
- **Write coalescing**: Small outbound writes are batched into fewer syscalls
- **Heartbeat coalescing**: ReadIndex probe heartbeats are merged with regular heartbeats

---

## 🚀 New Features (P2)

### Dead Node Auto-Removal (T4)
Automatically removes unresponsive nodes from the cluster configuration after a configurable timeout, with quorum safety guarantees. Includes partition-heal integration tests.

### Follower ReadIndex Forwarding (T5)
Clients can send read requests to any node; followers transparently forward to the leader. Eliminates the need for client-side leader discovery for read operations.

### Client Session / Idempotency
LRU+TTL deduplication guarantees exactly-once command execution even under client retries. Essential for production clients that cannot tolerate duplicate side effects.

### Quiesced Mode
When a cluster is idle (no proposals), heartbeat frequency is reduced to minimize CPU and network overhead. Automatically resumes full frequency on new activity.

---

## 💾 Storage Architecture (T3)

### WAL Separation
RollingRaft's storage layer has been re-architected from a monolithic LevelDB design to a hybrid approach:

| Component | Responsibility | Technology |
|-----------|---------------|------------|
| WALPersister | Append-only log entries | Segment-based WAL with CRC32 |
| StatePersister | Term, vote, snapshot metadata | LevelDB |
| HybridPersister | Unified interface | WAL + LevelDB combo |

**Performance validation**:
- Small payload (<1KB) concurrent workloads: **3.5× throughput**, **40× p99 latency improvement** (1.6ms vs 66ms)
- Large payload (≥1KB): equivalent to LevelDB (disk I/O bound)
- **Zero regression** in all 328 tests

---

## 📚 Documentation & Operability

### Public API Guide
Developer-facing guide covering:
- `RaftNode` C++ API with lifecycle examples
- `StateMachine` interface contract
- Client library usage patterns
- Metrics & monitoring (Prometheus endpoints, Grafana dashboard skeleton, 6 alerting rules)

### Operations Guide
SRE-facing manual with:
- Pre-deployment checklist
- P1/P2/P3 alert definitions with PromQL
- 5-scenario troubleshooting runbook
- Maintenance SOPs (add/remove node, leadership transfer, hot config update)

### Architecture Decision Records
Three ADRs documenting key design decisions:
- **ADR-001**: WAL Separation (with benchmark validation appendix)
- **ADR-002**: Client Session (in-memory LRU+TTL vs persistent)
- **ADR-003**: Multi-raft Protocol (`group_id` envelope design)

---

## 🔧 Infrastructure

### Multi-raft Groundwork
`group_id` field added to all RPC message types with backward-compatible default (`0`). Transport dispatch stub routes non-zero `group_id` to a placeholder handler. Full multi-raft implementation deferred to v0.3.0+ based on product decision.

### Code Quality
- Race condition review report with fixes
- Snapshot path audit with fixes
- Documented CI gates (cppcheck exhaustive, TSan unit tests)

---

## ⚠️ Breaking Changes

None in this release. All RPC protocol changes (`group_id` field) use backward-compatible defaults.

---

## 📝 Upgrade Notes

1. **CMakeLists.txt version**: Update `project(VERSION 0.1.0)` to `0.3.0`.
2. **Storage migration**: Existing LevelDB databases are automatically compatible; HybridPersister is opt-in via `RaftNodeConfig`.
3. **Config changes**: New fields in `RaftNodeConfig` have sensible defaults; no manual migration required.

---

## 📊 Metrics Quick Reference

| Metric | Type | Description |
|--------|------|-------------|
| `raft_proposal_latency_seconds` | Histogram | End-to-end proposal latency |
| `raft_readindex_latency_seconds` | Histogram | ReadIndex latency (lease or log) |
| `raft_transport_peer_connected` | Gauge | Per-peer connection state |
| `raft_commit_index` | Gauge | Current commit index |
| `raft_applied_index` | Gauge | Current applied index |
| `raft_leader_lease_active` | Gauge | 1 if leader lease is active |

---

## 🙏 Contributors

This release includes work from the entire RollingRaft team across safety, performance, storage, and documentation tracks.

---

*For detailed changelog, see [CHANGELOG.md](../CHANGELOG.md).*
