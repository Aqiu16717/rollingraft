# ADR-003: Multi-Raft Protocol — `group_id` Placement

**Status:** Accepted (Spike validated)  
**Date:** 2026-06-09  
**Deciders:** @Jack (Architecture), @Cindy (Product)  

---

## Context & Problem Statement

RollingRaft currently supports exactly one Raft consensus group per `RaftNode`. To support sharded workloads (e.g., sharded KV, multi-tenant coordination), we need to run multiple independent Raft groups on the same physical node. A prerequisite for multi-raft is the ability to route inbound RPCs to the correct group.

The question: how should the wire protocol identify which Raft group a message belongs to, while maintaining backward compatibility with single-group deployments?

---

## Options Considered

### Option A: Add `group_id` to Every Derived Message Struct

- **Pros**: Explicit per-message typing; no ambiguity about which messages need routing.
- **Cons**: Touches every RPC struct (`RequestVoteRequest`, `AppendEntriesRequest`, `InstallSnapshotRequest`, etc.); repetitive; easy to forget on new message types.
- **Verdict**: Rejected. Too much boilerplate and maintenance burden.

### Option B: Envelope Pattern — Wrap Payload in `RaftMessageEnvelope { group_id; payload }`

- **Pros**: Clean separation between routing metadata and message payload; transport layer can route without understanding message types.
- **Cons**: Requires double serialization/deserialization (envelope + inner message); breaks existing protocol tests that inspect raw JSON; adds nesting to wire format.
- **Verdict**: Rejected. Adds complexity without clear benefit over a flat field.

### Option C: Add `group_id` to Base `RaftRequest` / `RaftResponse`

- **Pros**:
  - All messages automatically carry `group_id` via inheritance.
  - Zero changes to derived structs.
  - Flat JSON: `{"group_id": 0, "type": 2, ...}` — easy to extract at transport layer.
  - `group_id = 0` means "default/single-group mode", preserving backward compatibility.
- **Cons**:
  - Base struct change affects all messages; semantically, some messages (e.g., `ClientRequest`) might not seem group-specific at first glance.
  - Transport must parse enough JSON to extract `group_id`.
- **Verdict**: **Accepted**.

### Option D: Separate Port per Group

- **Pros**: Zero protocol changes; routing happens at TCP layer via port numbers.
- **Cons**: Wasteful (one listening port per group); complicates firewall and service discovery; does not scale to thousands of groups.
- **Verdict**: Rejected. Does not meet multi-raft scaling goals.

---

## Decision

Adopt **Option C**: add `uint64_t group_id = 0` to `RaftRequest` and `RaftResponse` base structs. The JSON protocol serializes `group_id` as a top-level field alongside `correlation_id` and `type`.

### Routing Behavior

| `group_id` | Action |
|------------|--------|
| `0` | Existing single-group path (backward compatible). |
| `!= 0` | Extracted by `AsioNetworkTransport` and dispatched to a group-specific handler. Until full multi-raft is implemented, non-zero `group_id` returns `MULTI_RAFT_NOT_ENABLED`. |

### Key Implementation Details

- `JsonProtocol` uses `j.value("group_id", 0)` on deserialize, so old messages without the field default to 0.
- `AsioNetworkTransport` uses a lightweight `nlohmann::json::parse()` to extract `group_id` from inbound raw messages before dispatch.
- A `SetGroupRequestHandler()` API is exposed for future multi-raft code to register a real dispatcher without further transport changes.

---

## Consequences

### Positive

- **Minimal code change**: only base structs + protocol + transport stub modified.
- **Zero regression**: spike validated 328/328 existing tests pass with no modifications.
- **Backward compatible**: old peers ignore unknown `group_id` field; new peers default missing field to 0.
- **Forward compatible**: transport stub can be filled in later without touching the protocol again.

### Negative / Trade-offs

- **Transport parses JSON**: `AsioNetworkTransport` now depends on JSON parsing for routing. In a future protobuf or flatbuffers protocol, this extraction logic would need to be updated or moved behind a `Protocol::ExtractGroupId()` abstraction.
- **Semantically broader**: `ClientRequest` carries `group_id` even though clients usually target the cluster, not a specific group. This is acceptable because a client-aware-of-shards can use it, and clients unaware of multi-raft simply leave it at 0.
- **No message-type filtering**: even control-plane messages like `RequestVote` carry `group_id`. This is correct (every message belongs to some group), but it means all RPC handlers must be group-aware in a full multi-raft implementation.

### Future Work

- Replace inline JSON parsing in transport with `Protocol::ExtractGroupId()` when a second protocol implementation (e.g., protobuf) is added.
- Implement `GroupRequestHandler` that maps `group_id` to a `RaftGroup` registry.
- Evaluate heartbeat coalescing (CockroachDB-style) once group counts exceed 100 per node.
