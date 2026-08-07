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

### ✅ #6 Apply & ReadIndex (done 2026-08-02)
- Files: `src/state_machine_applier.cpp` (410), `src/client_session_manager.cpp`
- Focus: commit→apply pipeline, idempotent sessions, linearizable reads, known -Wthread-safety warnings
- Findings:
  - MED: ApplyLoop took applier_mtx_ before membership_mtx_ (hierarchy violation, deadlock cycle with ReadIndex path)
  - MED: state_machine_->Apply exception → std::terminate
  - MED: ReadIndex quorum checks used new-config majority only during joint consensus (3 sites)
  - MED: config-change proposal callback invoked synchronously under locks
  - LOW ×3 (recorded): CONFIG_CHANGE: prefix hijack (needs entry type framing), lease-read completion may pass lease expiry, ReadIndex heartbeat next_index_ underflow edge
  - INFO: old-seq returns latest result, session eviction loses dedup
- Decision: fix all MED (user)
- Fix: commit `43e3bf4` (+ `a2b4232` test proxy bypass — local http_proxy caused false MetricsEndpoint failures during verification). Tests 356/356.
- Phase 1 (consensus core) complete.

## Phase 2 — Data safety

### ✅ #7 WAL (done 2026-08-05)
- Files: `src/wal_persister.cpp` + `include/rollingraft/wal_persister.h` (~1900)
- Focus: write path, checkpoint, corruption recovery, protobuf migration interplay
- Findings:
  - HIGH: Open() skipped scanning the checkpoint-covered (active) segment → entries appended after the last checkpoint were durable but invisible after crash recovery (data loss)
  - MED: off-by-one in ShouldCreateCheckpointLocked filename parse → existing checkpoints never recognized, Sync/Close rewrote them constantly (masked the HIGH on clean shutdown)
  - MED: stale pre-GC checkpoint resurrected deleted entries on reopen (first_index moved backwards)
  - MED (recorded, not fixed): single corrupt record fails Open entirely instead of recovering to last valid record — robust-WAL truncation is a bigger semantic change
  - MED perf (recorded): ReadLogEntryAt opens/closes fd per entry; read path rewrites trailer per read
  - LOW: LoadMeta parses meta.json then discards it (dead code)
- Decision: fix HIGH + both correctness MEDs + regression test (user)
- Fix: commit `79c38f0`. Tests 357/357 (incl. new EntriesAfterCheckpointSurviveReopen).

### ✅ #8a Other persisters: leveldb + log_persister (done 2026-08-06)
- Files: `src/leveldb_persister.cpp` (853), `src/log_persister.cpp` (677)
- Focus: flushed_index_ semantics, batched fsync, crash consistency
- Findings:
  - MED: follower conflict truncation never persisted (no LogPersister::TruncateSuffix existed) → divergent entries resurrected from disk on every restart
  - MED: LevelDB TruncateSuffix/Prefix wrote without sync → crash could resurrect truncated entries
  - MED-LOW (recorded): DoFlush failure path fires error callbacks AND requeues entries — retried entries never update flushed_index_ (self-heals on next proposal)
  - LOW (recorded): per-key delete loops in truncations (perf, DeleteRange candidate); hand-rolled SHA-256/CRC32 duplicates (project links OpenSSL); Sync-via-empty-batch idiom
  - INFO: Index is uint32 (4B entry cap); WAL proto uses uint64
- Decision: fix both MED (user)
- Fix: commit `5498335`. Tests 357/357.

### ✅ #8b Other persisters: hybrid + state + group_commit (done 2026-08-06)
- Files: `src/hybrid_persister.cpp` (269), `src/state_persister.cpp` (558), `src/group_commit_controller.cpp` (278)
- Focus: WAL/LevelDB routing, epoch/callback FSM, sync thresholds
- Findings:
  - MED: snapshot saves in StatePersister/LevelDBPersister wrote with sync=false — crash could lose a snapshot after log compaction past it
  - Confirmed safe: group commit FSM (epoch ordering, callback firing outside locks, Stop drain), hybrid WAL buffer ordering for truncations, SetSyncOnWrite routing
- Decision: fix snapshot durability (user)
- Fix: commit `eec7822` (final commit batches sync=true; chunk writes covered via ordered WAL). Tests 357/357.

### ✅ #9 Network transport (done 2026-08-07)
- Files: `src/asio_network_transport.cpp` (1196)
- Focus: connection pool lifecycle, strands, callback timing, TLS path
- Findings:
  - MED: non-batching Send path could interleave bytes (two outstanding async_writes per socket)
  - MED: server responses bypassed the write queue — same interleave risk under pipelined RPCs (default config!)
  - MED: correlation_id fallback completed an arbitrary pending RPC with a mismatched response
  - LOW: std::stoi in Initialize outside try/catch
  - INFO (recorded): inbound raft handlers run synchronously on io threads — slow handler (ReadIndex forward, TruncateSuffix flush) blocks all connections on that thread; PeerConnection ctor stoi unguarded
- Decision: fix all (user)
- Fix: commit `59a1887` (unified write queue for all socket writes, batching toggle = coalescing only; fallback dropped; port parse guarded). Tests 357/357.

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
