# T4 Multi-Raft Spike: `group_id` Envelope

**Author:** @Jack  
**Date:** 2026-06-09  
**Scope:** Technical spike — validate that adding `group_id` to the wire protocol causes zero regression in single-group mode.

---

## 1. Goal

Verify that the existing JSON protocol can carry a `group_id` field without breaking backward compatibility or existing tests.

Success criteria:
1. Old peers (no `group_id` in JSON) are correctly handled by new code.
2. New peers (with `group_id=0`) are correctly handled by old code.
3. All existing single-group tests pass with zero modifications.

---

## 2. Changes Made

### 2.1 RPC Base Types (`include/rollingraft/rpc.h`)

Added `uint64_t group_id = 0` to both base structs:

```cpp
struct RaftRequest {
  RaftMessageType type_;
  uint64_t correlation_id_ = 0;
  uint64_t group_id = 0;  // NEW
  // ...
};

struct RaftResponse {
  RaftMessageType type_ = RaftMessageType::KInvalid;
  uint64_t correlation_id_ = 0;
  uint64_t group_id = 0;  // NEW
  // ...
};
```

**Rationale:** All RPC messages naturally belong to a Raft group. Placing `group_id` in the base type is the cleanest architectural choice and avoids touching every derived message struct.

### 2.2 JSON Protocol (`src/json_protocol.cpp`)

Minimal, symmetric changes to all four serialize/deserialize paths:

- `SerializeRequest`: `j["group_id"] = req.group_id;`
- `DeserializeRequest`: `req.group_id = j.value("group_id", 0);`
- `SerializeResponse`: `j["group_id"] = res.group_id;`
- `DeserializeResponse`: `res.group_id = j.value("group_id", 0);`

**Backward compatibility:** `j.value("group_id", 0)` means old messages without the field default to 0 (single-group mode).

### 2.3 Transport Dispatch Stub (`src/asio_network_transport.cpp`)

Added lightweight group_id extraction and dispatch logic:

```cpp
uint64_t ExtractGroupId(const std::string& body) {
  try {
    auto j = nlohmann::json::parse(body);
    return j.value("group_id", 0);
  } catch (const std::exception& e) {
    return 0;
  }
}
```

In `TcpConnection::HandleMessage`:

```cpp
uint64_t group_id = ExtractGroupId(body_buffer_);

if (group_id == 0) {
  // Existing single-group path (backward compatible)
  request_handler_(peer_id_, body_buffer_, response);
} else {
  // Multi-raft group dispatch stub
  if (group_request_handler_) {
    group_request_handler_(peer_id_, group_id, body_buffer_, response);
  } else {
    // Return clear error until multi-raft is fully implemented
    nlohmann::json err;
    err["error"] = "MULTI_RAFT_NOT_ENABLED";
    err["group_id"] = group_id;
    response = err.dump();
  }
}
```

Also added `SetGroupRequestHandler()` API on `AsioNetworkTransport` so future multi-raft code can register a real dispatcher without further transport changes.

---

## 3. Verification Results

### 3.1 Build

```bash
cmake --build . --target rollingraft -j4
```

Result: **Clean build** (only pre-existing warnings unrelated to this change).

### 3.2 Unit Tests

```bash
ctest -R "unit|Unit" -j4
```

Result: **295/295 passed**.

### 3.3 Integration + Deterministic Tests

```bash
ctest -R "integration|Integration|deterministic|Deterministic" -j2
```

Result: **33/33 passed**.

### 3.4 Total

**328/328 tests passed — zero regression.**

### 3.5 Backward Compatibility Check

| Scenario | Outcome |
|----------|---------|
| New code receives old JSON (no `group_id`) | `j.value("group_id", 0)` → defaults to 0 ✅ |
| Old code receives new JSON (with `group_id=0`) | nlohmann/json ignores unknown fields → no error ✅ |
| Transport sees `group_id=0` | Falls through to existing `request_handler_` ✅ |
| Transport sees `group_id!=0` (spike test) | Returns `MULTI_RAFT_NOT_ENABLED` error ✅ |

---

## 4. Conclusion

**Adding `group_id` to the wire protocol is safe and non-breaking.**

The spike validates that:
1. The JSON protocol can carry an extra top-level field without breaking existing peers.
2. The transport layer can extract and dispatch on `group_id` without disrupting the single-group code path.
3. All 328 existing tests pass with zero modifications.

This means **Option B (轻量 Group model)** is feasible from a protocol/transport perspective. The remaining work for multi-raft MVP would be:
- Multiple `RaftNode` / `RaftGroup` instances sharing one `NetworkTransport`
- A real `GroupRequestHandler` that routes messages to the correct group instance
- Per-group storage isolation (Phase 2 HybridPersister/WAL work provides the foundation)

---

## 5. Open Questions

1. Should the transport continue to parse JSON directly for group_id extraction, or should we add a `Protocol::ExtractGroupId()` abstraction? For the spike, direct JSON parsing is acceptable; for production, using the Protocol abstraction would be cleaner.
2. What should the error response format be for `MULTI_RAFT_NOT_ENABLED`? Currently returns a JSON error object, but this may need to be a proper Raft response type once multi-raft is enabled.
