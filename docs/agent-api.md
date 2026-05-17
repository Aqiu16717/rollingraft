# RollingRaft Agent-Friendly API Specification

> Version: 0.1.0-draft
> Status: Design Phase
> Target: Task #19 Control Plane API

## Design Goals

Make RollingRaft programmatically controllable and observable by AI agents:

1. **Agent can query full cluster state** without parsing logs
2. **Agent can trigger administrative operations** via HTTP/JSON
3. **Agent can subscribe to events** (leadership changes, membership changes)
4. **Agent can parse all output** (JSON logs, Prometheus metrics, structured status)

---

## API Endpoints

All endpoints return JSON. Base path: `/v1/`.

### 1. Cluster Status

```
GET /v1/status
```

**Response 200:**
```json
{
  "node_id": 1,
  "role": "Leader",
  "term": 42,
  "leader_id": 1,
  "leader_addr": "127.0.0.1:8001",
  "commit_index": 1523,
  "last_applied": 1523,
  "last_log_index": 1523,
  "last_log_term": 42,
  "cluster_config": {
    "version": 3,
    "nodes": [
      {"id": 1, "addr": "127.0.0.1:8001"},
      {"id": 2, "addr": "127.0.0.1:8002"},
      {"id": 3, "addr": "127.0.0.1:8003"}
    ]
  },
  "uptime_ms": 3600000
}
```

### 2. Health Probes

```
GET /v1/healthz    # Node is running
GET /v1/readyz     # Node is ready to serve requests
GET /v1/livez      # Node is alive (not deadlocked)
```

**Response 200 /v1/readyz (leader):**
```json
{
  "status": "ready",
  "role": "Leader",
  "checks": {
    "network": "ok",
    "storage": "ok",
    "leader_heartbeat": "ok"
  }
}
```

**Response 503 /v1/readyz (follower, not caught up):**
```json
{
  "status": "not_ready",
  "role": "Follower",
  "checks": {
    "network": "ok",
    "storage": "ok",
    "log_catchup": "in_progress"
  }
}
```

### 3. Membership Management

```
POST /v1/members
```

**Request:**
```json
{
  "node_id": 4,
  "addr": "127.0.0.1:8004"
}
```

**Response 202:**
```json
{
  "status": "accepted",
  "message": "Configuration change proposed",
  "change_id": "cfg-12345"
}
```

```
DELETE /v1/members/{node_id}
```

**Response 202:**
```json
{
  "status": "accepted",
  "message": "Node removal proposed",
  "change_id": "cfg-12346"
}
```

### 4. Snapshot Operations

```
POST /v1/snapshot/trigger
```

**Response 202:**
```json
{
  "status": "triggered",
  "message": "Snapshot creation initiated"
}
```

### 5. Leadership Transfer

```
POST /v1/leadership/transfer
```

**Request (optional):**
```json
{
  "target_node_id": 2
}
```

**Response 202:**
```json
{
  "status": "initiated",
  "message": "Leadership transfer started"
}
```

### 6. Metrics (Prometheus)

```
GET /metrics
```

Already exists. Returns Prometheus text format.

### 7. Events (SSE or WebSocket — TBD)

```
GET /v1/events
```

Server-Sent Events stream:

```
event: role_change
data: {"node_id":1,"old_role":"Follower","new_role":"Leader","term":43}

event: membership_change
data: {"change_id":"cfg-12345","type":"add","node_id":4,"status":"committed"}

event: snapshot_complete
data: {"snapshot_index":1500,"snapshot_size":1048576}

event: compaction
data: {"truncated_index":1200,"retained_index":1201}
```

---

## Error Response Format

All errors return JSON:

```json
{
  "error": "NOT_LEADER",
  "message": "Only the leader can modify cluster membership",
  "leader_hint": "127.0.0.1:8001"
}
```

**Error Codes:**

| HTTP Status | Error Code | Meaning |
|-------------|------------|---------|
| 400 | BAD_REQUEST | Invalid request body or parameters |
| 403 | NOT_LEADER | Node is not leader; use `leader_hint` to redirect |
| 404 | NODE_NOT_FOUND | Target node not in cluster |
| 409 | CONFIG_CONFLICT | Another membership change in progress |
| 422 | ALREADY_MEMBER | Node already in cluster |
| 500 | INTERNAL_ERROR | Unexpected server error |
| 503 | NOT_READY | Node is not ready to serve |

---

## Thread Safety & Implementation Notes

- All HTTP handlers run on the existing ASIO `io_context` or a dedicated strand
- Read operations (`GET /v1/status`, `/v1/healthz`) can be served from any thread with appropriate locks
- Write operations (`POST /v1/members`, `/v1/snapshot/trigger`) must be dispatched to the Raft node's strand
- Event SSE stream requires long-lived connection management (consider using ASIO's `async_write` with backpressure)

## Open Questions

1. **Event transport**: SSE (simpler) vs WebSocket (bidirectional)?
2. **Authentication**: Do we need API key / TLS for agent control plane?
3. **Rate limiting**: Should membership changes be rate-limited?
4. **Change ID tracking**: How does the agent poll for `change_id` completion?
