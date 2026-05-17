# RollingRaft Agent-Friendly Event Notification Design

> Version: 0.1.0-draft
> Status: Design Phase
> Target: Task #20 Event Notification Mechanism

## Overview

Extend the existing callback system to provide a unified, type-safe event notification interface for AI agents. Events are delivered asynchronously to avoid blocking the Raft hot path.

## Existing Callbacks

```cpp
// Current ( raft_node.h )
void SetRoleChangeCallback(std::function<void(RaftNodeRole, Term)>);
void SetLeaderChangeCallback(std::function<void(NodeId, NodeAddr)>);
```

**Limitations:**
- Only 2 event types covered
- No event metadata (timestamp, sequence number)
- No batching or backpressure control
- C++ `std::function` only — not friendly to language-agnostic agents

## Proposed Design

### Event Types

```cpp
enum class EventType {
  kRoleChange,        // Follower → Candidate → Leader transitions
  kLeaderChange,      // New leader elected or detected
  kMembershipChange,  // Node added/removed, config committed
  kSnapshotComplete,  // Local snapshot creation finished
  kSnapshotInstall,   // Snapshot installed from leader
  kLogCompaction,     // TruncatePrefix executed
  kStateMachineApply, // Command applied to state machine
  kConnectionEvent,   // Peer connect/disconnect
};

struct Event {
  EventType type;
  uint64_t sequence;           // Monotonic sequence number
  uint64_t timestamp_ms;       // Unix timestamp (ms)
  std::string json_payload;    // JSON-encoded event data
};
```

### C++ Interface (for embedded agents)

```cpp
class EventListener {
 public:
  virtual ~EventListener() = default;
  virtual void OnEvent(const Event& event) = 0;
};

// In RaftNode:
void AddEventListener(std::shared_ptr<EventListener> listener);
void RemoveEventListener(std::shared_ptr<EventListener> listener);
```

### HTTP SSE Stream (for external agents)

```
GET /v1/events
Content-Type: text/event-stream
```

```
id: 42
event: role_change
data: {"old_role":"Follower","new_role":"Leader","term":43}

event: membership_change
data: {"change_type":"add","node_id":4,"status":"committed","config_version":3}

event: snapshot_complete
data: {"snapshot_index":1500,"snapshot_size":1048576,"duration_ms":23}

event: log_compaction
data: {"truncated_index":1200,"retained_index":1201,"entries_removed":500}
```

## Implementation Plan

### Phase 1: Internal EventBus

Add an `EventBus` inside `RaftNodeImpl`:

```cpp
class EventBus {
 public:
  void Publish(Event event);
  void Subscribe(std::shared_ptr<EventListener> listener);
  void Unsubscribe(std::shared_ptr<EventListener> listener);

 private:
  std::mutex mtx_;
  uint64_t next_sequence_ = 1;
  std::vector<std::weak_ptr<EventListener>> listeners_;
};
```

**Threading model:**
- `Publish()` is called from various locked contexts inside RaftNodeImpl
- `Publish()` copies the event into an internal ring buffer / queue
- A dedicated ASIO strand (or the existing io_context) drains the queue and delivers to listeners **outside all locks** (Pattern C)
- SSE connections are managed by the HTTP server with backpressure (drop old events if client is slow)

### Phase 2: Hook Into Existing State Transitions

| Existing Code Location | Event to Publish |
|------------------------|------------------|
| `BecomeFollowerLocked()` / `BecomeCandidateLocked()` / `BecomeLeaderLocked()` | `kRoleChange` |
| `HandleAppendEntriesResponse()` (leader discovery) | `kLeaderChange` |
| `ApplyConfigChangeLocked()` | `kMembershipChange` |
| `MaybeTriggerAutoSnapshotLocked()` completion | `kSnapshotComplete` |
| `HandleInstallSnapshotResponse()` (follower) | `kSnapshotInstall` |
| `ApplyCommittedLocked()` (after state machine apply) | `kStateMachineApply` |
| `TruncatePrefix()` completion | `kLogCompaction` |

### Phase 3: SSE Endpoint

Extend `MetricsHttpServer` (or create `AgentHttpServer`) to support SSE:

```cpp
class AgentHttpServer {
 public:
  void Start();
  void Stop();
  void BroadcastEvent(const Event& event);

 private:
  void HandleSseRequest(asio::ip::tcp::socket socket);
  std::vector<std::weak_ptr<SseConnection>> sse_connections_;
};
```

**SSE backpressure strategy:**
- Each SSE connection has a bounded queue (e.g., 100 events)
- If queue is full, drop oldest events (agents can detect gaps via `sequence` number)
- If write fails (client disconnected), remove connection

## JSON Payload Schemas

### Role Change

```json
{
  "node_id": 1,
  "old_role": "Follower",
  "new_role": "Leader",
  "term": 43
}
```

### Leader Change

```json
{
  "node_id": 1,
  "leader_id": 2,
  "leader_addr": "127.0.0.1:8002",
  "term": 43
}
```

### Membership Change

```json
{
  "change_type": "add",
  "node_id": 4,
  "node_addr": "127.0.0.1:8004",
  "status": "committed",
  "config_version": 3,
  "old_nodes": [1, 2, 3],
  "new_nodes": [1, 2, 3, 4]
}
```

### Snapshot Complete

```json
{
  "snapshot_index": 1500,
  "snapshot_term": 42,
  "snapshot_size": 1048576,
  "duration_ms": 23,
  "trigger": "auto"
}
```

### Log Compaction

```json
{
  "truncated_index": 1200,
  "retained_index": 1201,
  "entries_removed": 500,
  "snapshot_index": 1500
}
```

## Open Questions

1. **Event ordering guarantee**: Should events be strictly ordered per-type or globally? (Recommend: global ordering via `sequence`)
2. **Event persistence**: Should events survive node restart? (Recommend: no, agents should query `/v1/status` on reconnect)
3. **Event filtering**: Should agents subscribe to specific event types only? (Recommend: v1 delivers all; filter client-side)
4. **Performance impact**: Event serialization on hot path — measure with benchmark after implementation
