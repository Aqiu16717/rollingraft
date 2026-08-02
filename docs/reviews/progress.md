# Full-Project Code Review — Progress Tracker

Goal: review the entire codebase in 12 small modules, risk-ordered
(consensus correctness → data safety → infrastructure).
Process per module: read + /code-review (scoped) → report findings by
severity → user decides fix-now vs. follow-up → fix + test + commit.

Legend: ⬜ pending · 🔶 in progress · ✅ done

---

## Phase 1 — Consensus core

### ✅ #1 Core state & locks (done 2026-07-30)
- Files: `src/raft_group.h`, `src/raft_node_impl.h`, `src/raft_node_core.cpp` (~2300 lines)
- Focus: lock-hierarchy adherence, Start/Stop lifecycle, OnStoreTick, manage_network_ dual path
- Findings:
  - HIGH: Stop() shutdown-timeout path — detached thread held dangling stack ref (`&done`) + raw `this` → UAF/UB
  - MED: Propose/ProposeBatch persist callbacks captured raw `this` (safe only via LogPersister::Stop join), duplicated ×2
  - LOW ×4: RemoveNode misleading comment, ForceShutdown stale detach comment, concurrent Stop() error semantics, executor only wired for ASIO timer
- Decision: fix HIGH + MED + LOW 1-2; leave LOW 3-4 as-is
- Fix: commit `5db0c2e` (keep-alive shutdown thread, shared done flag, OnLogEntryPersisted + weak guards). Tests 356/356.

### ✅ #2 Election (done 2026-07-31)
- Files: `src/election_manager.cpp` (596) + cross-checks in `rpc_handlers.cpp`, `log_replicator.cpp`
- Focus: pre-vote edges, CheckQuorum, leader lease, term handling, vote persistence races
- Findings:
  - HIGH: `voted_for_` reset+persisted at same term in BecomeFollowerLocked → double voting, two leaders in one term possible (CheckQuorum/transfer/disk-failure stepdown paths)
  - MED: vote/pre-vote counting without per-voter dedup → duplicate response could inflate false majority
  - MED: elections/pre-vote/CheckQuorum used new-config majority only during joint consensus (needs old AND new)
  - LOW: synchronous LevelDB SaveState under election_mtx_ (deliberate fail-stop; perf note, not fixed)
- Decision: fix HIGH + both MED; LOW recorded only
- Fix: commit `ef1c100` (sticky voted_for_, votes_received_ sets, ElectionQuorumSatisfiedLocked). Tests 356/356.

### ✅ #3 Log replication (done 2026-08-01)
- Files: `src/log_replicator.cpp`, `src/raft_log.cpp` + cross-checks in `rpc_handlers.cpp`, design-group-commit.md
- Focus: inflight window, next_/match_index updates, retry backoff, heartbeat coalescing
- Findings:
  - HIGH: followers ACK before durable + TryCommit ignores flushed_index_ → entry committable while durable on 0 nodes
  - HIGH: snapshot-boundary term-0 false match (diverged compaction points) + RaftLog::Append self-numbering → silent follower log corruption
  - LOW ×3: Append ignores entry.index_ (no continuity check), no correlation check on inflight responses, stale failure rewinds next_index_
- Decision: fix HIGH #2 + LOW 1&3 (user); HIGH #1 fixed per option A (commit gated on flushed_index_); LOW 2 deferred (needs AppendEntriesResponse protocol field → revisit in #10)
- Fix: commit `f4918d6` (last_snapshot_term_ tracking, boundary term answers, fast-forward reject, continuity guard, flushed_index_ commit gate incl. membership/FINALIZE/no-persister paths, stale-failure floor). Tests 356/356 ×2.

### ✅ #4 Snapshot (done 2026-08-01)
- Files: `src/snapshot_manager.cpp` (433) + receive side in `rpc_handlers.cpp`
- Focus: chunked transfer state machine, compaction interplay, InstallSnapshot role transitions
- Findings:
  - HIGH: receive side accepted any chunk without (index, term, offset) validation → interleaved transfers (leader flap mid-send) could corrupt restored state machine
  - MED: snapshot_sends_ not cleared on step-down (stale resume); temp path without group_id (multi-raft collision); user-code RestoreStream exception rethrown out of RPC thread (std::terminate); snapshot I/O under locks (design follow-up, not fixed)
  - LOW ×3: stale temp files never cleaned; dead offset-vs-index comparison; uint32 offset (theoretical)
- Decision: fix HIGH + MED 1/2/4 + LOW 1/2 (user); lock-held I/O recorded as design follow-up
- Fix: commit `531b6e2` (transfer session validation, group_id temp path + startup cleanup, clear sends on stepdown, no rethrow, dead code removal). Tests 356/356.

### ✅ #5 Membership (done 2026-08-02)
- Files: `src/membership_manager.cpp` (255) + cross-checks in `raft_group.cpp`, persisters
- Focus: joint consensus phases, learner promotion, self-removal, quorum math edges
- Findings:
  - HIGH: cluster_config_ never persisted (only term/vote) — restart after compaction resurrects static seed membership, losing committed add/remove → wrong quorum math
  - MED: leader stepping down mid joint-consensus could strand cluster in joint mode (nobody re-proposes FINALIZE)
  - MED: config command parsing (stoll/json::parse) throws on the apply thread → process terminate
- Decision: fix all three (user)
- Fix: commit `daf7df3` (PersistentState + cluster fields, LevelDB/State persister JSON key, all SaveState paths persist full state, FINALIZE re-proposal on BecomeLeader, parse try/catch). Tests 356/356.
- Test gap: no integration test for membership-survives-restart — worth adding.

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
