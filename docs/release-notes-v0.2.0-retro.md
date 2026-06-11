# RollingRaft v0.2.0 Release Notes (Retroactive)

> **Tag**: `v0.2.0` (`72f52eb`, 2026-05-20)  
> **Theme**: Control Plane & Deterministic Tests  
> **Commits**: 157 commits since v0.1.0

---

## What's New

### 🎛️ Control Plane & Agent API (Tasks #19–23)
A complete HTTP control plane for cluster management and observability:
- **RESTful Admin API**: Cluster management, metrics, and operational endpoints
- **SSE Event Streaming**: Real-time cluster state events via Server-Sent Events
- **JSON Logging**: Structured log output for production observability
- **Config Hot Reload**: Dynamic configuration updates without node restart
- **Leadership Transfer**: Manual leadership transfer for maintenance windows
- **Manual Snapshot Trigger**: On-demand snapshot creation via API
- **Bearer Token Authentication**: Admin endpoint authentication

### 🧪 Deterministic Testing (Task #22)
Reproducible chaos testing infrastructure:
- **SimulatedNetwork**: Thread-safe simulated network with message delay, drop, and reorder
- **SimulatedTimerService**: Deterministic time advancement for reproducible test scenarios
- **6 Chaos Scenarios**: PartitionRecovery, DelayStorm, DuplicateAndReorder, and more
- **GetCommitIndex() Public API**: External observability of commit progress

### 🔒 Concurrency & Safety
- **Fine-grained Locking** (Task #1): Six-phase migration from single global mutex to domain-specific mutexes (election, replication, snapshot, applier)
- **ASIO Network Hardening**: Thread pool, correlation ID matching, full-lifecycle timeout, strand serialization
- **Connection Pool Fixes**: Deadlock prevention, use-after-free elimination, timeout-based shutdown

### 📊 Metrics & Observability
- **Prometheus Metrics Library**: Self-contained histograms and gauges
- **HTTP /metrics Endpoint**: Asio-based Prometheus exposition
- **Full Integration**: Metrics wired into RaftNode lifecycle (proposals, reads, transport state)

### 🖥️ Client & Benchmarks
- **C++ Client Library**: Easy cluster interaction with 80 unit tests
- **Benchmark Framework**: Pluggable framework with failover, latency curve, and throughput scenarios
- **Log Compaction**: TruncatePrefix policy with async implementation

### 🔧 Build & CI
- **GitHub Actions CI**: Build + integration test workflow
- **TSan Validation**: Thread sanitizer with ASIO false-positive suppression
- **clang-format**: Full codebase formatting with CI gate
- **Docker Testing**: Multi-node cluster testing infrastructure

---

## Upgrade Notes

- No breaking changes from v0.1.0
- Existing LevelDB databases are fully compatible
- New `RaftNodeConfig` fields have sensible defaults

---

*For detailed changelog, see [CHANGELOG.md](../CHANGELOG.md).*
