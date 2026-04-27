# RollingRaft TODO

## Immediate (Next Steps)

(none)

## Completed ✅

- [x] **Integration Test Automation**
  - GitHub Actions CI workflow (build + unit tests + integration tests)
  - Docker-based multi-node test setup with working cluster communication
  - Backward-compatible `counter_server` full-address support

- [x] **Performance Benchmarking**
  - benchmark_client: Throughput test (ops/sec under load)
  - benchmark_latency_curve: Latency curve at different throughputs
  - benchmark_failover: Recovery time after leader failure
  - benchmark/README.md: Usage guide and interpretation

## Short-term (Testing & Hardening)

- [x] **Client Library** ✅ COMPLETED
  - C++ client wrapper with automatic leader discovery
  - Connection pooling and retry logic with exponential backoff
  - 80 comprehensive unit tests
  - Thread-safe for concurrent use

- [x] **Unit Test Coverage** ✅ COMPLETED
  - Core Raft: 59 tests (election, log replication, snapshots, membership)
  - Client Library: 80 tests (result, leader tracker, retry policy, connection pool, client)
  - Metrics: 6 tests (counter, gauge, histogram, registry, thread-safety)
  - **Total: 145 tests, all passing**

- [x] **Metrics & Monitoring** ✅ COMPLETED
  - Self-contained Prometheus-style metrics (Counter, Gauge, Histogram, Registry)
  - Minimal Asio-based HTTP /metrics endpoint
  - Optional and zero-cost when disabled
  - Key metrics: role, term, commit rate, election count, propose, RPC, snapshot, ReadIndex
  - Integration tests for HTTP endpoint

- [x] **Log Compaction Policy** ✅ COMPLETED
  - `LogPersister::TruncatePrefix()` with mandatory `FlushSync` drain
  - Configurable `log_retention_entries` safety buffer
  - Wired into leader auto-snapshot and follower InstallSnapshot
  - Metrics: `raft_log_compactions_total`, `raft_log_entries_compacted_total`
  - 3 unit tests

## Medium-term (Features)

- [ ] **TLS Support**
  - Encrypted communication between nodes
  - Certificate management

- [ ] **Dynamic Configuration**
  - Runtime configuration updates
  - Hot-reload of certain parameters

## Completed ✅

- [x] Project foundation and build system
- [x] Asio TCP server framework
- [x] Raft election logic
- [x] Log replication
- [x] Snapshot transfer
- [x] ReadIndex for linearizable reads
- [x] Membership change (add/remove node)
- [x] Log persistence with LevelDB
- [x] Unit test infrastructure with mocks
- [x] Compiler warning cleanup
- [x] Client Library with 80 unit tests
- [x] README documentation updated

---

**Last Updated:** 2026-04-22
**Current Version:** v0.1.0
**Test Status:** 148/148 passing (9 integration tests passing)
