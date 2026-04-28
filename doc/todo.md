# RollingRaft TODO

## Production Readiness Gap Analysis

Current status: **v0.1.0 — suitable for learning and prototyping, NOT production-ready.**

### 🔴 Blockers (data loss or crash risk)

- [ ] **WAL Sync Semantics**
  - `Propose()` calls `log_persister_->Append()` asynchronously (fire-and-forget)
  - Leader does not wait for its own log to be persisted before replicating to followers
  - If leader crashes before `DoFlush()` completes, buffered entries are lost
  - Fix: add sync-waiter or `FlushSync()` in leader `Propose()` before replying to client

- [ ] **Disk-Full Graceful Degradation**
  - `CheckDiskSpace()` is a no-op (returns `Status::OK()`)
  - When disk is full, `WriteBatch()` fails, `healthy_ = false`, no recovery path
  - Node becomes permanently write-dead
  - Fix: implement actual disk space check + retry/recovery when space frees up

- [ ] **Log Corruption Detection**
  - LevelDB-stored log entries have no checksum
  - `Restore()` reads corrupted data silently
  - Fix: append CRC32 per entry on write, verify on read

- [ ] **TLS + Authentication**
  - Node-to-node TCP is plaintext — unusable on public networks
  - No client/node identity verification — anyone can propose
  - Fix: Asio SSL context + certificate management

### 🟠 Critical (performance/availability degradation under load)

- [ ] **TruncatePrefix Blocks Event Loop**
  - Called inside `mtx_` with `FlushSync(1s)` — blocks heartbeat/proposal handling
  - Under high load can trigger unnecessary leader elections
  - Fix: move truncation to a background thread or lock-free queue

- [ ] **No Batch Propose API**
  - Each command = one `Propose()` → one RPC → one disk write
  - Throughput bounded by RTT, not bandwidth
  - Fix: `ProposeBatch(std::vector<Command>)` with single AppendEntries RPC

- [ ] **Snapshot Integrity Check**
  - `HandleInstallSnapshot()` restores data without checksum verification
  - Corrupted snapshot chunks put state machine into invalid state
  - Fix: SHA-256 checksum across all chunks, verify before `Restore()`

- [ ] **No Read Lease**
  - Every `ReadIndex()` heartbeats to majority — excessive RPC at high read load
  - Fix: leader leases (skip heartbeat within lease window)

### 🟡 Normal (operational pain, functionally okay)

- [ ] **No Graceful Leader Transfer**
  - Restarting leader requires `kill -9` → ~300-600ms unavailability
  - Fix: `TransferLeadership(target_node_id)` API

- [ ] **No Dynamic Configuration Hot-Reload**
  - Changing `election_timeout_ms` etc. requires node restart
  - Fix: SIGHUP handler or HTTP admin endpoint for config updates

- [ ] **No Chaos / Soak Testing**
  - No automated network partition / disk failure / slow disk tests
  - Fix: Docker-based chaos tests + 24h soak test harness

### 🔧 Known Workarounds / Technical Debt

- [ ] **`SO_SNDTIMEO` in `SendRpc` is a workaround**
  - Current: `setsockopt(SO_SNDTIMEO, 1s)` caps `connect()` timeout on Linux
  - Problem: affects all send operations, not portable to Windows
  - Proper fix: Asio `async_connect` + `steady_timer` for per-operation timeout

- [ ] **`JoinRpcThreads()` is synchronous**
  - Current: `Stop()` blocks up to 1s per RPC thread (due to `SO_SNDTIMEO`)
  - Problem: if N threads time out, `Stop()` blocks N seconds
  - Proper fix: interruptible threads (`std::jthread` stop_token) or async join with timeout

- [ ] **CI cache may skip `gtest_discover_tests`**
  - Current: `actions/cache@v4` caches entire `build/` directory including CMake generated files
  - Problem: if cache hits, `gtest_discover_tests` may not re-run, new tests invisible to ctest
  - Proper fix: cache only `ccache` / object files, not `CTestTestfile.cmake`

---

## Immediate (Next Steps)

Priority order based on gap analysis above:

1. **WAL Sync Guarantee** — data safety is non-negotiable
2. **Disk-Full Handling + Recovery** — availability under resource pressure
3. **Log Corruption Detection (CRC32)** — data integrity
4. **TLS + Certificate Management** — security baseline
5. **Async TruncatePrefix** — high-load stability
6. **Snapshot Checksum** — data integrity

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
  - 148 unit tests + 9 integration tests
  - GitHub Actions CI + Docker multi-node setup

- [x] **Documentation**
  - README, design docs, API reference, usage guides

---

**Last Updated:** 2026-04-22
**Current Version:** v0.1.0
**Test Status:** 148/148 passing (9 integration tests passing)
