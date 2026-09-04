# RollingRaft TODO

## Production Readiness Gap Analysis

Current status: **v0.1.0 — suitable for learning and prototyping, NOT production-ready.**

### 🔴 Blockers (data loss or crash risk)

- [x] **WAL Sync Semantics**
  - `Propose()` calls `log_persister_->Append()` asynchronously (fire-and-forget)
  - Leader does not wait for its own log to be persisted before replicating to followers
  - If leader crashes before `DoFlush()` completes, buffered entries are lost
  - Fix: add sync-waiter or `FlushSync()` in leader `Propose()` before replying to client
  - **Implemented:**
    - `LogPersister::Append()` now accepts optional per-entry `FlushCallback`
    - `LogPersister::AppendSync()` blocks until entry is durably flushed
    - Leader `Propose()` delays replication until flush callback fires
    - `ProposeAndWaitLocked()` calls `AppendSync()` before broadcasting
    - `SendAppendEntriesToPeerLocked()` only sends entries up to `flushed_index_`
    - `TryCommitLocked()` only counts leader in quorum if entry is flushed
    - LevelDB persister uses `sync=true` when `sync_on_critical` is enabled

- [x] **Disk-Full Graceful Degradation**
  - `CheckDiskSpace()` now uses `statvfs` on POSIX systems to check available space
  - Configurable `min_disk_space_bytes` threshold (default: 100MB)
  - Auto-recovery when disk space becomes available
  - Recovery attempts in both `Append()` and `DoFlush()` paths

- [x] **Log Corruption Detection**
  - CRC32 checksum appended to each log entry on write
  - Checksum verified on read in `DeserializeEntry()`
  - CRC32 mismatch logs error and returns false, preventing corrupted data application
  - Format: index (4) + term (4) + data_len (4) + data + checksum (4)

- [x] **Node Identity Authentication**
  - mTLS certificates use URI SAN `rollingraft-node:<node_id>`
  - Startup rejects a local certificate that does not match configured `node_id`
  - Handshakes bind inbound/outbound connections to the certificate NodeId
  - Raft RPC sender claims must match the authenticated certificate identity
  - Election and replication still require Raft membership

- [ ] **Client Identity Authentication and Authorization**
  - `client_id` remains a caller-supplied deduplication key, not an identity
  - Define authenticated client credentials and read/write authorization policy
  - Keep client credentials distinct from node certificates

### 🟠 Critical (performance/availability degradation under load)

- [x] **Async TruncatePrefix**
  - Called inside `mtx_` with `FlushSync(1s)` — blocks heartbeat/proposal handling
  - Under high load can trigger unnecessary leader elections
  - Fix: move truncation to a background thread or lock-free queue

- [x] **Batch Propose API**
  - Each command = one `Propose()` → one RPC → one disk write
  - Throughput bounded by RTT, not bandwidth
  - Fix: `ProposeBatch(std::vector<Command>)` with single AppendEntries RPC

- [x] **Snapshot Integrity Check**
  - SHA-256 checksum computed on `SaveSnapshot()` and stored in LevelDB
  - Checksum verified on `LoadSnapshot()` before returning data
  - Corruption detection prevents invalid state machine restoration
  - Backward compatible: warns but doesn't fail if no hash exists (old snapshots)

- [x] **Read Lease**
  - Every `ReadIndex()` heartbeats to majority — excessive RPC at high read load
  - Fix: leader leases (skip heartbeat within lease window)

### 🟡 Normal (operational pain, functionally okay)

- [x] **Graceful Leader Transfer**
  - Restarting leader requires `kill -9` → ~300-600ms unavailability
  - Fix: `TransferLeadership(target_node_id)` API

- [x] **Dynamic Configuration Hot-Reload**
  - Changing `election_timeout_ms` etc. requires node restart
  - Fix: SIGHUP handler or HTTP admin endpoint for config updates

- [ ] **Extended Chaos / Soak Testing**
  - Deterministic partition, delay, duplication, and reordering scenarios exist
  - Disk-failure/slow-disk injection and a 24h soak harness are still missing

### 🔧 Known Workarounds / Technical Debt

- [x] **Replace `SO_SNDTIMEO` RPC workaround**
  - Networking now uses Asio `async_connect` with operation timers

- [x] **Remove synchronous `JoinRpcThreads()` path**
  - The old per-RPC thread model has been replaced by shared Asio workers

- [x] **Cache compiler artifacts without generated CTest state**
  - CI caches ccache directories only; generated build/test files are not cached

---

## Immediate (Next Steps)

Priority order based on gap analysis above:

1. **Client Identity Authentication and Authorization** — close the remaining security blocker
2. **Disk-Failure and Slow-Disk Injection** — validate persistence failure behavior
3. **24h Multi-Raft Soak Harness** — expose lifecycle and election churn defects
4. **Production Operations Hardening** — backup/restore drills and upgrade testing

---

## Completed ✅

- [x] **Project Foundation**
  - C++20 build system, zero compiler warnings, Doxygen API docs

- [x] **Raft Core**
  - Leader election, log replication, snapshot transfer, ReadIndex
  - Membership change (add/remove nodes)

- [x] **Persistence**
  - LevelDB state persistence + `LogPersister` batched async log writes
  - Log compaction (`TruncatePrefix`) with configurable retention

- [x] **Client Library**
  - Auto leader discovery, connection pooling, exponential backoff retry
  - 80 unit tests

- [x] **Metrics & Monitoring**
  - Prometheus-style Counter/Gauge/Histogram + HTTP `/metrics` endpoint
  - 40+ injection points in RaftNode

- [x] **Performance Benchmarks**
  - `benchmark_client` (throughput), `benchmark_latency_curve`, `benchmark_failover`

- [x] **Testing Infrastructure**
  - 396 unit, integration, and deterministic CTest cases
  - GitHub Actions CI + Docker multi-node setup

- [x] **Documentation**
  - README, design docs, API reference, usage guides

---

**Last Updated:** 2026-09-01
**Current Version:** v0.1.0
**Test Status:** Release and TSan suites passing 396/396
