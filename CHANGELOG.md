# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.3] — 2026-06-12

### Fixed
- **Empty snapshot cleanup**: `StatePersister::SaveSnapshotStream()` now correctly clears the old snapshot when installing an empty snapshot, restoring behavior consistent with pre-v0.3.2 implementations.

## [0.3.2] — 2026-06-12

### Fixed
- **Atomic snapshot replacement**: `StatePersister::SaveSnapshotStream()` now writes to a temporary file and performs an atomic `rename()` only after successful SHA-256 verification. This eliminates the window where a failed snapshot write leaves the node without a valid snapshot.

## [0.3.1] — 2026-06-12

### Changed
- **Protobuf WAL serialization**: Migrated `WALPersister` log entry serialization from JSON+Base64 to protobuf with raw `bytes` fields. The segment parser automatically falls back to JSON for existing segments, so no manual migration is required.

### Performance
- WAL append throughput: **+28%**
- WAL recovery time: **-80%**
- WAL on-disk size: **-43%**

## [0.3.0] — 2026-06-11

### Safety (P0)
- **Pre-vote + CheckQuorum**: Reduce disruptive elections and prevent partitioned nodes from causing spurious leader changes.
- **Joint Consensus membership changes**: Safe configuration changes without availability gaps.
- **Node-to-node TLS/mTLS**: Encrypted inter-node RPC with certificate-based authentication.
- **Streaming snapshot transfer**: Chunked, incremental snapshot transfer with SHA-256 integrity checks and bounded memory usage.
- **Graceful shutdown**: Configurable timeout with forced cleanup to prevent indefinite hangs.
- **Backpressure in replication**: Limit in-flight AppendEntries per peer to prevent memory explosion.
- **RaftNodeConfig validation**: Comprehensive startup validation for all configuration parameters.
- **Quorum safety fixes**: Use committed cluster config for quorum calculation; fix double-lock deadlocks.

### Performance (P1)
- **Async Apply**: Independent apply thread with queue decouples consensus from state machine execution.
- **Pipeline replication**: Ordered inflight window replaces fixed kMaxPendingAppends=3, improving throughput under latency.
- **Group commit**: Batch fsync with periodic sync thread removes flushed_index_ blocking.
- **Leader lease read optimization**: Stale-safe reads without log replication for read-heavy workloads.
- **Heartbeat coalescing**: Merge ReadIndex heartbeats with regular heartbeats to reduce RPC count.
- **Transport write coalescing**: Batch small writes in AsioNetworkTransport to reduce syscall overhead.
- **Learner role**: Non-voting learner with auto-promote for safe cluster expansion.

### Features (P2)
- **Dead node auto-removal**: Automatic removal of unresponsive nodes with quorum safety (Task T4).
- **Follower ReadIndex forwarding**: Followers forward read requests to leader transparently (Task T5).
- **Client Session / idempotency**: LRU+TTL deduplication for exactly-once semantics.
- **Quiesced Mode**: Idle cluster optimization reducing heartbeat overhead.
- **RuntimeConfig hot reload v2**: Dynamic transport batching and other switches without restart.
- **LevelDB compression**: Expose compression configuration via RaftNodeConfig.

### Storage (T3)
- **WAL Separation architecture**: HybridPersister (WAL for logs + LevelDB for state) replacing monolithic LevelDB.
  - WALPersister: Segment-based append-only log with CRC32 checks.
  - StatePersister: Dedicated LevelDB for persistent state (term, vote, snapshot metadata).
  - Benchmark-validated: 3.5× throughput and 40× p99 latency improvement in small-payload concurrent workloads.

### Infrastructure
- **Multi-raft protocol spike**: group_id envelope added to RPC base class and transport dispatch; zero regression.
- **Architecture Decision Records**: ADR-001 (WAL Separation), ADR-002 (Client Session), ADR-003 (Multi-raft Protocol).
- **Public API Guide**: Developer-facing guide with 8 metric categories and PromQL samples.
- **Operations Guide**: SRE-facing runbook with 5-scenario troubleshooting and maintenance SOPs.
- **Code quality process**: Documented CI gates, cppcheck strategy, and TSan workflow.
- **Audit reports**: Race condition review and snapshot path audit with fixes.

## [0.2.0] — 2026-05-20

### Control Plane & Agent API
- **HTTP Control Plane API** (Task #19): RESTful endpoints for cluster management, metrics, and admin operations.
- **Event Notification System** (Task #20): SSE-based event streaming for real-time cluster state updates.
- **JSON Logging** (Task #21): Structured JSON log output for production observability.
- **Config Hot Reload** (Task #23): Dynamic configuration updates without node restart.
- **Leadership Transfer**: Manual leadership transfer API for maintenance operations.
- **Manual Snapshot Trigger**: On-demand snapshot creation via admin endpoint.
- **Bearer Token Authentication**: Admin endpoint authentication with Bearer tokens.

### Deterministic Testing
- **Deterministic Test Infrastructure** (Task #22): SimulatedNetwork + SimulatedTimerService for reproducible chaos tests.
- **6 Chaos Scenarios**: PartitionRecovery, DelayStorm, DuplicateAndReorder, and more.
- **GetCommitIndex() Public API**: Expose commit index for external observability.

### Concurrency & Safety
- **Fine-grained Locking** (Task #1): 6-phase migration from single global mutex to domain-specific mutexes (election, replication, snapshot, applier).
- **ASIO Network Improvements**: Thread pool, correlation ID matching, full-lifecycle timeout, strand serialization.
- **Connection Pool Fixes**: Prevent deadlocks, use-after-free, and indefinite join hangs.

### Metrics & Observability
- **Prometheus Metrics**: Histograms for proposal/readindex latency, transport peer state gauges.
- **HTTP /metrics Endpoint**: Self-contained Asio-based Prometheus exposition.
- **Metrics Integration**: Full metrics collection wired into RaftNode lifecycle.

### Client & Benchmarks
- **Client Library**: Easy-to-use C++ client for cluster interaction with 80 unit tests.
- **Benchmark Framework**: Pluggable framework with failover, latency curve, and client benchmarks.
- **Log Compaction**: TruncatePrefix policy with async implementation.

### Build & CI
- **GitHub Actions CI**: Build + integration test workflow with artifact capture.
- **TSan Support**: Thread sanitizer validation with suppression file for ASIO false positives.
- **clang-format**: Full codebase formatting with CI gate.
- **Docker Testing**: Multi-node cluster testing infrastructure.

## [0.1.0] — 2025-XX-XX

### Initial Release
- Core Raft consensus implementation (leader election, log replication, safety).
- LevelDB-based log and state persistence.
- Basic snapshot support.
- 3-node cluster example (counter).
- Unit and integration test suite.
