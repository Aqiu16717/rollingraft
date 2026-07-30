# Full-Project Code Review — Progress Tracker

Goal: review the entire codebase in 12 small modules, risk-ordered
(consensus correctness → data safety → infrastructure).
Process per module: read + /code-review (scoped) → report findings by
severity → user decides fix-now vs. follow-up → fix + test + commit.

Legend: ⬜ pending · 🔶 in progress · ✅ done

---

## Phase 1 — Consensus core

### ⬜ #1 Core state & locks
- Files: `src/raft_group.h`, `src/raft_node_impl.h`, `src/raft_node_core.cpp` (~2300 lines)
- Focus: lock-hierarchy adherence, Start/Stop lifecycle, OnStoreTick, manage_network_ dual path
- Findings: —
- Decision: —

### ⬜ #2 Election
- Files: `src/election_manager.cpp` (596)
- Focus: pre-vote edges, CheckQuorum, leader lease, term handling, vote persistence races
- Findings: —

### ⬜ #3 Log replication
- Files: `src/log_replicator.cpp`, `src/raft_log.cpp` (~1100)
- Focus: inflight window, next_/match_index updates, retry backoff, heartbeat coalescing
- Findings: —

### ⬜ #4 Snapshot
- Files: `src/snapshot_manager.cpp` (433)
- Focus: chunked transfer state machine, compaction interplay, InstallSnapshot role transitions
- Findings: —

### ⬜ #5 Membership
- Files: `src/membership_manager.cpp` (255)
- Focus: joint consensus phases, learner promotion, self-removal, quorum math edges
- Findings: —

### ⬜ #6 Apply & ReadIndex
- Files: `src/state_machine_applier.cpp`, `src/client_session_manager.cpp` (~600)
- Focus: commit→apply pipeline, idempotent sessions, linearizable reads, known -Wthread-safety warnings
- Findings: —

## Phase 2 — Data safety

### ⬜ #7 WAL
- Files: `src/wal_persister.cpp` + `include/rollingraft/wal_persister.h` (~1900)
- Focus: write path, checkpoint, corruption recovery, protobuf migration interplay
- Findings: —

### ⬜ #8 Other persisters
- Files: `leveldb_persister.cpp`, `log_persister.cpp`, `hybrid_persister.cpp`, `state_persister.cpp`, `group_commit_controller.cpp` (~2600, may split)
- Focus: flushed_index_ semantics, batched fsync, crash consistency
- Findings: —

### ⬜ #9 Network transport
- Files: `src/asio_network_transport.cpp` (1196)
- Focus: connection pool lifecycle, strands, callback timing (inline-callback bug fixed 2026-07-30), TLS path
- Findings: —

## Phase 3 — Infrastructure

### ⬜ #10 RPC protocol & dispatch
- Files: `src/rpc_handlers.cpp`, `src/json_protocol.cpp`, `include/rollingraft/rpc.h` (~1500)
- Focus: deserialization robustness (malformed input), group_id routing
- Findings: —

### ⬜ #11 Multi-raft
- Files: `src/raft_store.cpp`, `src/multi_raft_persister.cpp`, `src/shared_node_infra.h` (~700)
- Focus: shared-resource boundaries, RemoveGroup (UAF fixed 2026-07-30), tick distribution
- Findings: —

### ⬜ #12 Client + metrics/events
- Files: `src/client.cpp`, `src/client/`, `src/metrics_http_server.cpp`, `src/event.cpp`, `src/sse_connection.cpp` (~1800)
- Focus: retry/backoff, pooling, SSE lifecycle, admin endpoint auth
- Findings: —

---

## Session log

- 2026-07-30: Plan created. Starting with #1.
