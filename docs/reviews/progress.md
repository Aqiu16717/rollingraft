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

### ✅ #10 RPC protocol & dispatch (done 2026-08-07)
- Files: `src/rpc_handlers.cpp`, `src/json_protocol.cpp`, `include/rollingraft/rpc.h` (~1500)
- Focus: deserialization robustness (malformed input), group_id routing
- Findings:
  - Confirmed safe: all deserialization paths fully try/caught, required-field checks, correlation_id/group_id round-trip, lock lifecycle in HandleClientRequest correct
  - LOW (recorded): redundant double-JSON-parse in HandleIncomingRpc; SerializeEntries drops command_/checksum_ fields; IntToMessageType rejects via switch-default rather than explicit validation
- Decision: no fixes needed (user). Tests 357/357.

### ✅ #11 Multi-raft (done 2026-08-07)
- Files: `src/raft_store.cpp` (299), `src/multi_raft_persister.cpp` (125), `src/shared_node_infra.h` (34)
- Focus: shared-resource boundaries, RemoveGroup, tick distribution
- Findings:
  - Confirmed safe: shared tick lock protocol, OnIncomingRpc shared_ptr keep-alive, CreateGroup idempotency, MultiRaftPersister forward-delegate + cached-settings pattern, group-isolated persist subdirs
  - LOW (recorded): MultiRaftPersister delegate methods lack inner_ null guard — safe only because call order guarantees Open before use
- Decision: no fixes needed (user).

### ✅ #12 Client + metrics/events (done 2026-08-07)
- Files: `src/client.cpp` (372), `src/client/`, `src/metrics_http_server.cpp`, `src/event.cpp`, `src/sse_connection.cpp`
- Focus: retry/backoff, pooling, SSE lifecycle, admin endpoint auth
- Findings:
  - MED: redirect hint from NotLeader response discarded immediately (line 217 ClearLeader nuked the UpdateLeader at line 270) — leader discovery degraded to full round-robin every retry
  - Confirmed safe: SSE write serialization, EventBus handler-copy-outside-lock, worker thread drain, timing-safe admin token comparison
  - LOW (recorded): stoi unguarded in MetricsHttpServer::Start; FormatPrometheus histogram label slicing
- Decision: fix MED (user)
- Fix: commit `6bf20ed`. Tests 357/357.

---

## Review complete — all 12 modules ✅ (2026-08-07)

---

## Phase 4 — Delta review (changes since 2026-08-07)

Re-review the codebase surface that changed since the 12-module review
closed: the SSE store workstream, the lock-I/O refactor, and WAL perf work.
Same process per module: read + /code-review (scoped) → findings by severity
→ user decides fix-now vs. follow-up → fix + test + commit.

### ✅ #13 Multi-raft store + SSE/events delta (done 2026-08-19)
- Files: `src/raft_store.cpp/h` (+202), `src/sse_connection.cpp`, `src/event.cpp`,
  `src/metrics_http_server.cpp/h`, store endpoints in `src/rpc_handlers.cpp`
- Focus: SSE broadcast lifecycle, group event routing, admin endpoint auth
- Findings (8 parallel review angles, verified):
  - HIGH: unguarded `stoi` on DELETE node_id → unauthenticated remote crash
  - HIGH: `?group_id=N` parse always threw → store DELETE permanently dead
  - HIGH: `/readyz` ready-path returned 404 (status_line never set) → probes never passed
  - HIGH: admin handlers used raw `GetGroup` pointer after `groups_mtx_` release → UAF vs RemoveGroup
  - MED: store SSE lambda races Stop() (metrics_server_ UAF); `BroadcastEvent` reads io_ctx_ unsynchronized
  - MED: `RaftStore::Start` leaves store half-started on MetricsHttpServer::Start throw
  - MED: SSE strong-refs retain silently-vanished clients indefinitely; `/v1/events` accepts any method
  - LOW: `/v1/status` unlocked leader-addr read races the leader-change writer
- Decision: fix now = crash + group_id parse + readyz + UAF (user); rest logged as follow-ups
- Fix: commit `0e00e60` (guarded parse with 400, documented query form parsed, readyz 200, GetGroupShared shared-ownership lookups) + 3 unit regression tests. Tests 375/375 (incl. TSan).
- Follow-up (2026-08-22): `SseConnection` now watches for passive peer
  disconnects and notifies `MetricsHttpServer`, which immediately releases its
  strong reference. Added a loopback regression test for disconnect detection
  and single close notification. Release and TSan tests 381/381.

### ✅ #14 Lock-I/O refactor delta (done 2026-08-19)
- Files: `src/snapshot_manager.cpp` (+295), `src/raft_node_core.cpp` (+210),
  `src/log_replicator.cpp`, `src/election_manager.cpp`
- Focus: two-phase snapshot creation/receive, peer snapshot prep outside
  manager locks, lock-hierarchy adherence after the refactor
- Findings (verified):
  - CRITICAL S1: unlocked creation window + `SetStartIndex` clearing ALL entries → committed entries wiped, index reuse with new term (Raft safety violation). Verified in code + deterministic RED test.
  - CRITICAL S2: concurrent `CreateSnapshot()` on the user SM (peer-prep path bypassed `snapshot_in_progress_`; contract only covers Apply-vs-reads)
  - HIGH S3: unbounded peer-snapshot queueing on the shared timer strand (create-before-dedup; multi-group tick stalls → elections churn)
  - HIGH S4: follower-side unlocked-restore races (same-path truncation mid-restore, stale restore clobber, commit/last_applied rewind double-apply + dropped client callbacks)
  - HIGH S5: leader-side vs receive-side persister stream ordering → durable snapshot regression behind truncated log (chunk-interleave mechanism refuted; ordering hazard confirmed)
  - HIGH #9: heartbeat-rejection rewind — no contact-time refresh (live voter auto-removed), no match+1 floor, quiesced-mode stall, unbounded conflict_index_ (0xFFFFFFFF permanent stall), follower-ahead oscillation freezing commits
  - MED: TriggerSnapshot OK-on-skip + manual metrics missing; idle snapshot loop via uint32 underflow; chained stale snapshot transfers; dead locked wrappers (DoSnapshotLocked/MaybeTriggerAutoSnapshotLocked)
  - MED: mixed clocks (deadlines on timer_->Now(), lease/quorum/contact on steady_clock) — deterministic tests freeze lease/quorum paths
- Decision: approach B (finish the two-phase design) + #9 rewind fix now (user); mixed clocks + metric cleanups logged as follow-ups
- Fix: commits `d2e202e` (staleness guards on apply/persist, single-flight creation token incl. peer-prep, per-peer pending flag at schedule time, restore-window transfer guard, commit guard on receive apply, dead wrappers deleted, snapshot_io_mtx_ stream serialization + deterministic regression test `SnapshotTwoPhaseTest`) and `3c3d08a` (rewind clamped to leader last+1, snapshot branch for ahead-followers, contact refresh, budget-free prompt resend). Tests 375/375 (incl. TSan ×2).
- Noted: the exact 3-node "follower ahead via snapshot" oscillation scenario from the sweep is not constructible in a 3-node cluster (majority arithmetic); the snapshot branch is defensive but correct by construction.
- Follow-up (2026-08-21): lease, quorum-ack, leader-contact, dead-peer-contact,
  and heartbeat-coalescing timestamps now use `TimerService::Now()`. Added
  deterministic CheckQuorum step-down and lease-expiry scenarios. Enabling the
  former exposed and fixed a self-deadlock in the quorum-loss follower
  transition (replication mutex was already held). Release tests 380/380;
  TSan reported no races, with two unrelated election-timeout flakes passing
  isolated reruns and a combined 10-iteration stress run (20/20).
- Follow-up (2026-08-22): manual and automatic snapshot creation now increment
  `raft_snapshots_created_total` only after successful creation/persistence,
  labeled by trigger. A snapshot skipped during the two-phase apply window now
  returns its real error instead of `Status::OK()`, and the single-group admin
  endpoint preserves generic-vs-not-leader error classification. Release tests
  381/381; the snapshot TSan coverage passed, while an unrelated one-off
  `PeerConnection` reconnect/close race was logged for transport follow-up
  after five isolated reruns passed.

### ✅ #15 WAL perf + transport/protocol delta (done 2026-08-20)
- Files: `src/wal_persister.cpp/h` (+164 fd cache), `src/asio_network_transport.cpp`,
  `src/json_protocol.cpp`
- Focus: cached-fd lifetime and serialization, RPC timer arm/cancel ordering,
  protocol parse/serialization compatibility
- Findings (verified):
  - Confirmed safe: WAL cached fd and file offsets are serialized by `mtx_`;
    cache invalidation precedes segment deletion and runs on close
  - Confirmed safe: RPC timer mutation is serialized on the connection strand;
    callbacks remain exactly-once through the pending-callback map
  - Confirmed safe: JSON protocol changes preserve legacy `group_id == 0` and
    reject unknown message types without dispatch
  - LOW (recorded): WAL cached-open failure is collapsed to a generic error,
    losing the more specific `OpenSegment` status
- Decision: no #15 blocker; retain the low-severity diagnostic issue as a follow-up.
  Tests 378/378, including TSan.
- Focus: segment fd caching correctness (restart/truncate paths), the timer
  strand fix (fresh, already TSan-verified), protocol changes
- Follow-up (2026-08-22): the initial `PeerConnection::StartConnecting()` call
  now posts to the connection strand when invoked from an arbitrary
  `io_context` worker, serializing `conn_` and timer access with reconnect and
  `Close()`. The triggering membership-restart test passed 20/20 under TSan and
  the full TSan suite passed 381/381. Two unrelated Release timing flakes
  (`MetricsShowHeartbeatCoalescing` and `RecoversAfterLeaderCrash`) each passed
  10/10 in isolated reruns.

---

## Session log

- 2026-07-30: Plan created. Starting with #1.
