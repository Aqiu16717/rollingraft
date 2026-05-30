# P0: RPC Snapshot Receiver Streaming Fix — Design Doc

## Problem

`HandleInstallSnapshotRequest` in `rpc_handlers.cpp` still accumulates all snapshot chunks into `snapshot_temp_data_` (`std::string`) before restoring the state machine and persisting. This undermines the streaming Persister API (`SaveSnapshotStream`/`LoadSnapshotStream`) that was introduced to prevent OOM on large snapshots.

For a 1GB snapshot, the current path allocates:
1. `snapshot_temp_data_` — 1GB (string)
2. `snapshot_bytes` — 1GB (vector copy for Restore)
3. `SaveSnapshot` internal copy — 1GB (LevelDB Put)

Total: ~3GB peak memory, guaranteed OOM on memory-constrained nodes.

## Root Cause

The `StateMachine::Restore` interface requires the **entire** snapshot in memory:

```cpp
virtual bool Restore(const std::vector<uint8_t>& snapshot) = 0;
```

This forces the RPC handler to buffer all chunks before calling Restore. Even if we stream chunks into `SaveSnapshotStream`, we still need the full buffer for Restore.

## Proposed Fix (Two-Phase)

### Phase 1: Incremental Persister Write (Immediate)

Replace `Persister::SaveSnapshot` call with `SaveSnapshotStream` in `HandleInstallSnapshotRequest`. This eliminates the Persister-side copy (item 3 above). Peak memory drops from ~3GB to ~2GB.

**Implementation:**
- In `HandleInstallSnapshotRequest`, on `req.done_`:
  - Create a `chunk_provider` lambda that yields chunks from `snapshot_temp_data_`
  - Call `persister_->SaveSnapshotStream(chunk_provider, last_index, last_term)`

This is a ~10-line change with zero interface impact.

### Phase 2: Streaming StateMachine Restore (Follow-up)

Add a streaming restore interface to `StateMachine`:

```cpp
class StateMachine {
 public:
  // Existing (keep for backward compat)
  virtual bool Restore(const std::vector<uint8_t>& snapshot) = 0;

  // New: incremental restore from chunks
  virtual bool RestoreStream(
      const std::function<bool(std::string& chunk)>& chunk_provider) {
    // Default: collect all chunks and call Restore
    std::vector<uint8_t> data;
    std::string chunk;
    while (chunk_provider(chunk)) {
      data.insert(data.end(), chunk.begin(), chunk.end());
    }
    return Restore(data);
  }
};
```

Then in `HandleInstallSnapshotRequest`:
1. On `req.offset_ == 0`: open a temp file, write chunks to disk as they arrive
2. On `req.done_`:
   a. Create a `chunk_provider` that reads from the temp file
   b. Call `state_machine_->RestoreStream(chunk_provider)`
   c. Call `persister_->SaveSnapshotStream(chunk_provider, ...)`
   d. Delete temp file

Peak memory: ~0GB (only one chunk in flight at a time).

## Trade-offs

| Approach | Peak Memory | Complexity | Risk |
|----------|------------|------------|------|
| Current | ~3GB | Low | OOM |
| Phase 1 only | ~2GB | Very Low | Safe, partial fix |
| Phase 1+2 | ~0GB | Medium | Requires StateMachine interface change |

## Recommendation

Implement **Phase 1 immediately** (1 commit, ~10 lines, zero risk). Schedule Phase 2 for next sprint because it touches the `StateMachine` interface and all implementations (`MockStateMachine`, `CounterStateMachine`, etc.).
