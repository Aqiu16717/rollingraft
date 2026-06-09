# ADR-002: Client Session for Idempotent Propose

**Status:** Accepted  
**Date:** 2026-06-09  
**Deciders:** @Jack (Architecture), @Cindy (Product)  

---

## Context & Problem Statement

Raft replicates commands, but it does not guarantee exactly-once execution. If a client sends a command, the network times out before the response arrives, and the client retries, the command may execute twice. This is a classic problem in distributed consensus systems.

The question: how should RollingRaft provide idempotent `Propose()` so that clients can safely retry without double-execution?

---

## Options Considered

### Option A: Persistent Session Store (in LevelDB)

- **Pros**: Sessions survive node restart; leader change does not lose session state.
- **Cons**: Every `CheckDuplicate()` and `RecordResponse()` becomes a disk I/O operation on the critical Propose path, adding ~0.1-1ms latency; complex transaction logic needed to keep session state consistent with log apply.
- **Verdict**: Rejected. The latency and complexity cost on the hot path is too high.

### Option B: No Built-in Deduplication (client-side only)

- **Pros**: Zero server-side complexity; clients can implement their own dedup if needed.
- **Cons**: Every client must implement correct deduplication logic; inconsistent implementations across language bindings; poor out-of-box experience.
- **Verdict**: Rejected. A Raft library should provide this primitive.

### Option C: In-Memory Session Manager with LRU + TTL

- **Pros**:
  - O(1) `CheckDuplicate()` lookup on the Propose path.
  - Simple implementation using `unordered_map` + `list` + shared_mutex.
  - TTL-based expiration provides automatic cleanup without operator intervention.
- **Cons**:
  - Sessions are lost on leader change (memory state is node-local).
  - Memory usage grows with `max_sessions` and TTL.
  - Sticky leader assumption: clients must typically retry to the same leader.
- **Verdict**: **Accepted**.

### Option D: Hybrid (memory hot cache + async persistence)

- **Pros**: Faster than fully persistent; can recover some session state after restart.
- **Cons**: Adds complexity without solving the leader-change problem (new leader still won't have the persisted session unless log-replayed); inconsistent guarantees.
- **Verdict**: Rejected. The complexity is not justified for the marginal gain.

---

## Decision

Adopt **Option C**: `ClientSessionManager` is an in-memory, per-node component with the following properties:

- **API**: `CheckDuplicate(session_id, seq_num)` returns `kNew`, `kDuplicate`, or `kOldSeq`.
- **Cache policy**: LRU eviction when `max_sessions` is reached; TTL expiration on write operations.
- **Response caching**: `RecordResponse(session_id, seq_num, response)` stores the result after state machine apply.
- **Integration**: `RaftNode::Propose(command, callback, session_id, seq_num)` checks dedup under `replication_mtx_` before log append; `StateMachineApplier` calls `RecordResponse()` after successful apply.
- **Backward compatibility**: `session_id = 0` disables deduplication entirely.

---

## Consequences

### Positive

- **Zero latency overhead** on duplicate detection (O(1) hash lookup).
- **Exactly-once semantics** within a single leader's tenure for well-behaved clients.
- **Backward compatible**: existing code using `Propose(command, callback)` continues to work.
- **Bounded memory**: `max_sessions` and `ttl_ms` are configurable.

### Negative / Known Limitations

- **Leader change loses sessions**: if the leader steps down or crashes, the new leader has no session memory. Clients must be prepared to receive `NOT_LEADER` or `OLD_SEQUENCE` errors and re-establish their session window.
- **Sticky leader assumption**: clients should cache the leader address and retry to the same leader for best results.
- **Memory vs. correctness trade-off**: very large `ttl_ms` or `max_sessions` can increase memory pressure; very small values increase the risk of duplicate execution.
- **Not a cross-leader solution**: for true cross-leader exactly-once, clients would need a persistent session log replicated through Raft itself (significant complexity, future work).

### Operational Recommendation

- Use `ttl_ms` slightly larger than expected client retry window (default 60s is reasonable).
- Use `max_sessions` based on expected concurrent client count (default 10,000).
- Client libraries should: (1) persist `session_id` across restarts, (2) increment `seq_num` monotonically, (3) retry with the same `(session_id, seq_num)` on timeout.
