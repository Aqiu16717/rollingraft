# RollingRaft Public API Guide

**Version:** v0.1.0  
**Target Audience:** First-time RollingRaft developers  
**Prerequisites:** C++20 or later, basic understanding of Raft consensus algorithm

---

## Table of Contents

1. [Getting Started](#1-getting-started)
2. [RaftNode Core API](#2-raftnode-core-api)
3. [StateMachine Interface](#3-statemachine-interface)
4. [RaftNodeConfig Reference](#4-raftnodeconfig-reference)
5. [Client API](#5-client-api)
6. [Event System](#6-event-system)
7. [Runtime Configuration](#7-runtime-configuration)
8. [Common Patterns](#8-common-patterns)
9. [Metrics & Monitoring](#9-metrics--monitoring)

---

## 1. Getting Started

RollingRaft is a C++ implementation of the Raft consensus algorithm. It provides strong consistency guarantees through leader-based log replication, automatic failover, and membership changes.

### Minimal Example: Counter Server

```cpp
#include "rollingraft/raft_node.h"
#include "rollingraft/state_machine.h"
#include "rollingraft/persister.h"

// 1. Implement your state machine
class CounterMachine : public rollingraft::StateMachine {
  // ... see Section 3 for full implementation
};

// 2. Configure the node
rollingraft::RaftNodeConfig config;
config.node_id = 1;
config.listen_addr = "0.0.0.0:8001";
config.peers = {"127.0.0.1:8002", "127.0.0.1:8003"};
config.data_dir = "./data/node1";

// 3. Create and start
auto sm = std::make_shared<CounterMachine>();
rollingraft::RaftNode node(config, sm);
node.Start();

// 4. Propose commands (only on leader)
if (node.IsLeader()) {
  node.Propose("inc", [](const rollingraft::ApplyResult& result) {
    if (result.success) {
      std::cout << "New value: " << result.response << "\n";
    }
  });
}
```

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Node** | A single server participating in Raft consensus |
| **Cluster** | A set of nodes (typically 3 or 5) forming one consensus group |
| **Leader** | The single node that accepts client writes and replicates them |
| **Follower** | Passive node that receives replicated log entries |
| **Candidate** | Transitional state when a follower tries to become leader |
| **Log Entry** | A single command replicated across the cluster |
| **Commit** | A log entry is committed when replicated to a majority |
| **Apply** | A committed entry is applied to the state machine |

---

## 2. RaftNode Core API

### Lifecycle

```cpp
// Construction
RaftNode(const RaftNodeConfig& config, std::shared_ptr<StateMachine> sm);

// Lifecycle
Status Start();   // Initialize network, load state, begin participation
Status Stop();    // Graceful shutdown with configurable timeout
```

**Thread-safety:** All public methods are thread-safe. `Start()` must be called before any other operation.

### Proposing Commands

```cpp
// Single command proposal (with optional idempotency)
Status Propose(const std::string& command,
               std::function<void(const ApplyResult&)> callback,
               uint64_t session_id = 0,
               uint64_t seq_num = 0);

// Batch proposal (atomic group of commands)
Status ProposeBatch(
    const std::vector<std::string>& commands,
    std::function<void(const std::vector<ApplyResult>& results)> callback);
```

| Parameter | Description |
|-----------|-------------|
| `command` | Opaque data passed to `StateMachine::Apply()` |
| `callback` | Invoked asynchronously when command is applied or fails |
| `session_id` | Client session ID for idempotency (0 = disabled) |
| `seq_num` | Monotonically increasing sequence within session |

**Important:**
- Only the leader can propose. Returns error on followers/candidates.
- Callback is invoked from a different thread (the apply thread).
- Propose is non-blocking; the callback indicates eventual success/failure.

### Linearizable Reads

```cpp
Status ReadIndex(std::function<void()> callback);
```

`ReadIndex` ensures the node is still leader by exchanging heartbeats with a majority, then invokes the callback when it's safe to read from the state machine.

**Usage pattern:**
```cpp
node.ReadIndex([sm]() {
  auto result = sm->Query("get counter");
  std::cout << result.response << "\n";
});
```

### Membership Management

```cpp
Status AddNode(NodeId id, const NodeAddr& addr);       // Add voting member
Status AddLearner(NodeId id, const NodeAddr& addr);    // Add non-voting replica
Status PromoteLearner(NodeId id);                      // Learner → voter
Status RemoveNode(NodeId id);                          // Remove member
```

**Safety constraints:**
- Only the leader can modify membership
- Changes are applied via Raft log (one node at a time for safety)
- Cannot remove the leader itself

### Queries

```cpp
bool IsLeader() const;
RaftNodeRole GetRole() const;
Term CurrentTerm() const;
NodeAddr GetLeaderAddr() const;
Index GetCommitIndex() const;
ClusterConfig GetConfig() const;
```

### Snapshots

```cpp
Status TriggerSnapshot();              // Manual snapshot trigger (leader only)
Status TransferLeadershipTo(NodeId target_id);  // Graceful leadership transfer
```

### Event Bus

```cpp
EventBus& GetEventBus();
```

Subscribe to cluster events (see [Section 6: Event System](#6-event-system)).

---

## 3. StateMachine Interface

The `StateMachine` is the **only component you must implement** to use RollingRaft. It defines how commands modify your application state.

### Required Methods

```cpp
class StateMachine {
 public:
  // Core: Apply a committed log entry
  virtual ApplyResult Apply(std::span<const uint8_t> data, uint64_t index) = 0;

  // Tracking: Last applied index (used for reads and snapshots)
  virtual uint64_t GetLastAppliedIndex() const = 0;

  // Snapshot: Create a point-in-time snapshot
  virtual std::shared_ptr<Snapshot> CreateSnapshot() = 0;

  // Snapshot: Restore from snapshot data
  virtual bool Restore(const std::vector<uint8_t>& snapshot) = 0;

  // Reads: Wait for specific index to be applied
  virtual void WaitIndex(uint64_t index, std::function<void()> cb) = 0;

  // Reads: Execute read-only query
  virtual ApplyResult Query(std::span<const uint8_t> data) = 0;
};
```

### Critical Requirements

1. **Determinism:** `Apply()` must produce the **same result** for the same `(data, index)` on all nodes. Do not use random numbers, timestamps, or node-local state in `Apply()`.

2. **Thread-safety:** `Apply()` may be called concurrently with `Query()`. Use appropriate synchronization.

3. **Index tracking:** `GetLastAppliedIndex()` must return the highest index passed to `Apply()`.

### Snapshot Implementation

For large state machines, implement streaming snapshots to avoid loading everything into memory:

```cpp
class MySnapshot : public Snapshot {
 public:
  const SnapshotMeta& GetMeta() const override { return meta_; }

  size_t Read(uint64_t offset, uint8_t* dest, size_t length) override {
    // Read up to `length` bytes starting at `offset`
    // Return actual bytes read (0 at EOF)
  }

  std::string GetPath() const override { return file_path_; }

 private:
  SnapshotMeta meta_;
  // ... your snapshot data
};
```

For streaming restore, override `RestoreStream()`:

```cpp
bool RestoreStream(const std::function<bool(std::string& chunk)>& chunk_provider) override {
  std::string chunk;
  while (chunk_provider(chunk)) {
    // Process chunk incrementally
  }
  return true;
}
```

### Complete Example: CounterMachine

```cpp
class CounterMachine : public rollingraft::StateMachine {
 public:
  ApplyResult Apply(std::span<const uint8_t> data, uint64_t index) override {
    std::lock_guard<std::mutex> lock(mtx_);
    std::string cmd(data.begin(), data.end());

    if (cmd == "inc") ++value_;
    else if (cmd == "dec") --value_;

    last_applied_index_ = index;
    NotifyWaiters(index);

    ApplyResult result;
    result.success = true;
    result.response = std::to_string(value_);
    result.applied_index = index;
    return result;
  }

  uint64_t GetLastAppliedIndex() const override {
    return last_applied_index_.load();
  }

  std::shared_ptr<Snapshot> CreateSnapshot() override {
    std::lock_guard<std::mutex> lock(mtx_);
    return std::make_shared<CounterSnapshot>(value_, last_applied_index_, 0);
  }

  bool Restore(const std::vector<uint8_t>& snapshot) override {
    std::lock_guard<std::mutex> lock(mtx_);
    if (snapshot.size() < sizeof(int64_t)) return false;
    std::memcpy(&value_, snapshot.data(), sizeof(int64_t));
    return true;
  }

  void WaitIndex(uint64_t index, std::function<void()> cb) override {
    std::lock_guard<std::mutex> lock(mtx_);
    if (last_applied_index_.load() >= index) {
      cb();
    } else {
      waiters_.emplace(index, std::move(cb));
    }
  }

  ApplyResult Query(std::span<const uint8_t> data) override {
    std::lock_guard<std::mutex> lock(mtx_);
    ApplyResult result;
    result.success = true;
    result.response = std::to_string(value_);
    return result;
  }

 private:
  void NotifyWaiters(uint64_t index) {
    auto it = waiters_.begin();
    while (it != waiters_.end() && it->first <= index) {
      it->second();
      it = waiters_.erase(it);
    }
  }

  mutable std::mutex mtx_;
  int64_t value_ = 0;
  std::atomic<uint64_t> last_applied_index_{0};
  std::multimap<uint64_t, std::function<void()>> waiters_;
};
```

---

## 4. RaftNodeConfig Reference

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `node_id` | `NodeId` | Unique identifier for this node (must be unique in cluster) |
| `listen_addr` | `string` | Address to listen on, e.g., `"0.0.0.0:8001"` |
| `peers` | `vector<string>` | Addresses of peer nodes |
| `data_dir` | `string` | Directory for persistent storage |

### Timing Parameters

| Field | Default | Description |
|-------|---------|-------------|
| `election_timeout_ms` | 300 | Base election timeout (actual: randomized [1x, 2x]) |
| `heartbeat_interval_ms` | 50 | Leader heartbeat interval |
| `rpc_timeout_ms` | 500 | RPC call timeout |
| `propose_timeout_ms` | 5000 | Max wait for proposal to commit |
| `shutdown_timeout_ms` | 30000 | Graceful shutdown timeout (0 = indefinite) |

### Replication Parameters

| Field | Default | Description |
|-------|---------|-------------|
| `max_entries_per_append` | 100 | Max log entries per AppendEntries RPC |
| `max_retry_attempts` | 5 | Max retries for AppendEntries |
| `base_retry_delay_ms` | 10 | Base delay for exponential backoff |
| `max_retry_delay_ms` | 500 | Max retry delay |
| `max_pipeline_window` | 128 | Max in-flight AppendEntries per peer |

### Snapshot Parameters

| Field | Default | Description |
|-------|---------|-------------|
| `snapshot_threshold_entries` | 10000 | Entries since last snapshot to trigger |
| `snapshot_threshold_bytes` | 10MB | Bytes since last snapshot to trigger |
| `snapshot_check_interval_ms` | 5000 | How often to check (leader only) |
| `max_snapshot_size_bytes` | 100MB | Max snapshot size (0 = unlimited) |
| `log_retention_entries` | 0 | Entries to keep before snapshot (0 = delete all) |

### Feature Flags

| Field | Default | Description |
|-------|---------|-------------|
| `leader_lease_enabled` | true | Enable leader lease for local reads |
| `check_quorum_enabled` | true | Leader steps down without quorum acks |
| `pre_vote_enabled` | true | Pre-vote to prevent term inflation |
| `auto_remove_dead_nodes` | false | Auto-remove unresponsive followers |
| `transport_batching_enabled` | true | Batch outbound messages |
| `quiesced_mode_enabled` | false | Reduce heartbeat when idle |
| `compression_type` | 1 (Snappy) | LevelDB compression (0=none, 1=Snappy) |

### Security

| Field | Default | Description |
|-------|---------|-------------|
| `tls_enabled` | false | Enable TLS for node-to-node and metrics |
| `tls_cert_file` | "" | Server certificate path |
| `tls_key_file` | "" | Private key path |
| `tls_ca_file` | "" | CA certificate path |
| `tls_mutual_auth` | false | Require CA-verified peer certificates with a node URI SAN |
| `tls_allowed_peer_identities` | empty | Optional exact URI SAN allowlist |
| `admin_token` | "" | Bearer token for admin API endpoints |

With `tls_mutual_auth = true`, every node certificate must contain exactly one
URI SAN in the form `rollingraft-node:<node_id>`. The local certificate is
checked against `RaftNodeConfig::node_id` during startup. After each handshake,
the authenticated identity is also checked against the expected outbound peer
and the sender ID claimed by RequestVote, PreVote, AppendEntries, and
InstallSnapshot RPCs.

The current high-level `Client` does not present a node certificate. A strict
mTLS Raft endpoint therefore accepts authenticated node traffic only; expose a
separate application gateway or keep client access disabled until client
credentials and authorization are configured by a future release.

```cpp
config.tls_enabled = true;
config.tls_mutual_auth = true;
config.tls_cert_file = "/run/secrets/node-1.crt";
config.tls_key_file = "/run/secrets/node-1.key";
config.tls_ca_file = "/run/secrets/cluster-ca.crt";
config.tls_allowed_peer_identities = {
    "rollingraft-node:2",
    "rollingraft-node:3",
};
```

The allowlist is static configuration. Update it alongside planned membership
changes. Leaving it empty accepts any correctly formed node identity signed by
the configured CA; Raft membership checks still prevent unknown identities
from participating in election and replication.

### Factory Functions (for testing/customization)

```cpp
std::function<std::unique_ptr<NetworkTransport>()> network_factory;
std::function<std::unique_ptr<TimerService>()> timer_factory;
std::function<std::unique_ptr<Persister>()> persister_factory;
std::function<std::unique_ptr<Protocol>()> protocol_factory;
```

### Best Practice Configurations

**Local Development (3 nodes, same machine):**
```cpp
config.election_timeout_ms = 150;
config.heartbeat_interval_ms = 50;
config.rpc_timeout_ms = 200;
config.check_quorum_enabled = false;  // Faster for testing
config.pre_vote_enabled = false;
```

**Production (3-5 nodes, LAN):**
```cpp
config.election_timeout_ms = 300;
config.heartbeat_interval_ms = 50;
config.rpc_timeout_ms = 500;
config.leader_lease_enabled = true;
config.check_quorum_enabled = true;
config.pre_vote_enabled = true;
config.tls_enabled = true;
config.tls_mutual_auth = true;
config.admin_token = "your-secure-token";
```

**WAN / Multi-DC:**
```cpp
config.election_timeout_ms = 1000;
config.heartbeat_interval_ms = 200;
config.rpc_timeout_ms = 2000;
config.max_pipeline_window = 256;  // Higher for high latency
```

---

## 5. Client API

The `Client` class provides automatic leader discovery, retry, and connection management.

```cpp
#include "rollingraft/client.h"

// Create client
rollingraft::ClientOptions options;
options.max_retries = 5;
options.request_timeout = std::chrono::seconds(10);

rollingraft::Client client(
    {"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"},
    options);

// Synchronous write
auto result = client.Execute("set mykey myvalue");
if (result.ok()) {
  std::cout << "Response: " << result.value() << "\n";
} else {
  std::cerr << "Error: " << result.error_message() << "\n";
}

// Synchronous read
auto query_result = client.Query("get mykey");

// Async write
client.ExecuteAsync("inc", [](rollingraft::ClientResult result) {
  if (result.ok()) {
    std::cout << "Async result: " << result.value() << "\n";
  }
});
```

### ClientOptions

| Field | Default | Description |
|-------|---------|-------------|
| `max_retries` | 3 | Max retry attempts on failure |
| `initial_retry_delay` | 100ms | Initial delay between retries |
| `max_retry_delay` | 1000ms | Max delay between retries |
| `retry_backoff_multiplier` | 2.0 | Exponential backoff multiplier |
| `request_timeout` | 5000ms | Per-request timeout |
| `connect_timeout` | 2000ms | Connection timeout |
| `leader_cache_ttl` | 30000ms | How long to cache leader address |
| `client_id` | 0 | Client ID for deduplication (0 = auto-generate) |
| `max_async_queue_size` | 10000 | Max async task queue (0 = unlimited) |

---

## 6. Event System

RollingRaft provides an `EventBus` for subscribing to cluster lifecycle events without polling.

```cpp
auto& bus = node.GetEventBus();

// Subscribe to role changes
auto sub1 = bus.Subscribe<NodeRoleChangedEvent>([](const auto& e) {
  std::cout << "Node " << e.node_id << " became "
            << RaftNodeRoleToString(e.new_role) << "\n";
});

// Subscribe to leader changes
auto sub2 = bus.Subscribe<LeaderChangedEvent>([](const auto& e) {
  std::cout << "New leader: " << e.new_leader_addr << "\n";
});

// Subscribe to all events
auto sub3 = bus.SubscribeAll([](const RaftEvent& event) {
  std::cout << "Event: " << event.Name() << "\n";
});

// Unsubscribe when done
bus.Unsubscribe(sub1);
```

### Available Events

| Event | Triggered When |
|-------|---------------|
| `NodeRoleChangedEvent` | Node transitions between follower/candidate/leader |
| `LeaderChangedEvent` | A new leader is elected (observed on any node) |
| `MembershipChangedEvent` | Node added or removed from cluster |
| `LogCompactedEvent` | Log compaction (snapshot) completed |
| `SnapshotInstalledEvent` | Follower receives snapshot from leader |
| `ProposalCommittedEvent` | Proposal reaches majority (committed) |
| `ProposalAppliedEvent` | Proposal applied to state machine |
| `ElectionTimeoutEvent` | Election timeout fires |
| `NodeLifecycleEvent` | Node starts or stops |

**Important:** Events are dispatched synchronously on the publisher's thread. Keep handlers fast or offload work.

---

## 7. Runtime Configuration

RollingRaft supports hot-reloading of tuning parameters via the `RuntimeConfig` system.

### HTTP API

```bash
# Get current runtime config
curl http://localhost:9001/v1/config

# Multi-Raft: select one group
curl 'http://localhost:9001/v1/config?group_id=42'

# Update parameters (requires admin token if configured)
curl -X PATCH http://localhost:9001/v1/config \
  -H "Authorization: Bearer <token>" \
  -d '{"heartbeat_interval_ms": 100, "transport_batching_enabled": false}'

# Multi-Raft: group_id is required in the PATCH body
curl -X PATCH http://localhost:9001/v1/config \
  -H "Authorization: Bearer <token>" \
  -d '{"group_id":42,"heartbeat_interval_ms":100}'
```

Consensus tuning is group-local in a `RaftStore`; updating one group does not
change another group's election, replication, snapshot, or retry settings.
`transport_batching_enabled` is node-level because all groups share one
transport, so changing it through any group updates every hosted group.

### Programmatic Access

```cpp
// RuntimeConfig is internal; parameters are consumed automatically
// by RaftNode on each timer reset or config read.
```

### Runtime-Configurable Parameters

| Parameter | Min | Max | Description |
|-----------|-----|-----|-------------|
| `election_timeout_ms` | 50 | 5000 | Election timeout |
| `heartbeat_interval_ms` | 10 | 1000 | Heartbeat interval |
| `max_entries_per_append` | 1 | 10000 | Entries per AppendEntries |
| `rpc_timeout_ms` | 100 | 10000 | RPC timeout |
| `snapshot_threshold_entries` | 100 | 1000000 | Snapshot trigger (entries) |
| `snapshot_threshold_bytes` | 1MB | 1GB | Snapshot trigger (bytes) |
| `max_retry_attempts` | 1 | 100 | Max retries |
| `max_pipeline_window` | 1 | 10000 | Pipeline window size |
| `leader_lease_enabled` | — | — | Enable leader lease |
| `transport_batching_enabled` | — | — | Enable write batching |

Updates are validated atomically — either all requested changes apply, or none do.

---

## 8. Common Patterns

### 8.1 Leader Election

Raft automatically handles leader election. Your application typically only needs to check `IsLeader()` before proposing:

```cpp
if (node.IsLeader()) {
  node.Propose(command, callback);
} else {
  // Redirect to leader or queue for later
  auto leader = node.GetLeaderAddr();
  RedirectToLeader(leader, command);
}
```

**Detection via events:**
```cpp
bus.Subscribe<NodeRoleChangedEvent>([](const auto& e) {
  if (e.new_role == LEADER) {
    OnBecameLeader();
  } else if (e.old_role == LEADER) {
    OnSteppedDown();
  }
});
```

### 8.2 Membership Change

Add nodes one at a time using joint consensus for safety:

```cpp
// Step 1: Add as learner (replicates but doesn't vote)
node.AddLearner(4, "192.168.1.4:8001");

// Step 2: Wait for learner to catch up, then promote
// (Auto-promotion happens when match_index >= commit_index)
node.PromoteLearner(4);

// To remove a node
node.RemoveNode(4);
```

**Important:**
- Changes are propagated through the Raft log
- Only one membership change at a time
- The leader cannot remove itself

### 8.3 Snapshot Management

Snapshots are triggered automatically based on `snapshot_threshold_entries` and `snapshot_threshold_bytes`. You can also trigger manually:

```cpp
// Manual trigger (leader only)
node.TriggerSnapshot();
```

**When to tune thresholds:**
- **Lower thresholds:** Faster recovery, more frequent snapshot I/O
- **Higher thresholds:** Less snapshot I/O, longer recovery time

### 8.4 Client Session (Idempotent Propose) ⭐

**This is a P3 feature that enables exactly-once semantics for client commands.**

Use case: A client sends a command, but the network times out before the response arrives. Did the command execute? With client sessions, the client can safely retry without double-execution.

```cpp
// Client side: maintain session_id + monotonic seq_num
uint64_t session_id = GenerateUniqueId();  // Persistent across retries
uint64_t seq_num = 1;  // Increment for each command

// First attempt
auto status = node.Propose("transfer $100", callback, session_id, seq_num);

// If network error or timeout, retry with SAME session_id + seq_num
// RollingRaft will return the cached result instead of re-executing
status = node.Propose("transfer $100", callback, session_id, seq_num);
```

**Server-side configuration:**
```cpp
// ClientSessionManager is internal; configure via RaftNodeConfig
// (No explicit config needed — defaults are reasonable)
```

**Key behaviors:**
- `session_id = 0` disables deduplication (backward compatible)
- `seq_num <= last_executed` → returns cached response immediately
- `seq_num < last_executed - window` → error (OLD_SEQUENCE)
- Responses are cached until session expires (TTL-based cleanup)

**Client best practices:**
1. Generate `session_id` once per client instance (persist across restarts)
2. Increment `seq_num` monotonically for each command
3. On timeout, retry with the **same** `(session_id, seq_num)` pair
4. Only increment `seq_num` after receiving success or a definitive error

### 8.5 Linearizable Read

Two approaches for consistent reads:

**Approach A: ReadIndex (recommended)**
```cpp
node.ReadIndex([sm]() {
  // Safe to read — leader confirmed, index applied
  auto result = sm->Query("get counter");
});
```

**Approach B: Leader Lease (faster, slightly relaxed)**
```cpp
// Enabled by default via config.leader_lease_enabled
// Leader skips heartbeat broadcast if lease is valid
// ~5-10x faster than ReadIndex for read-heavy workloads
```

**Trade-offs:**
- `ReadIndex`: Strongest guarantee, but requires RTT to majority
- `Leader Lease`: Faster, but may serve slightly stale data during network partitions

---

## 9. Metrics & Monitoring

RollingRaft exposes Prometheus-compatible metrics via an HTTP server. Enable it by setting `metrics_enabled = true` and `metrics_addr` in `RaftNodeConfig`.

### 9.1 HTTP Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/metrics` | GET | Public | Prometheus metrics in text format |
| `/healthz` | GET | Public | Liveness probe: `{"status":"alive"}` |
| `/livez` | GET | Public | Same as `/healthz` |
| `/readyz` | GET | Public | Readiness probe: ready if leader is known |
| `/v1/status` | GET | Public | JSON status: node_id, role, term, leader_id, commit_index |
| `/v1/members` | POST | Admin | Add a new cluster member |
| `/v1/members/:id` | DELETE | Admin | Remove a cluster member |
| `/v1/snapshot/trigger` | POST | Admin | Trigger manual snapshot |
| `/v1/leadership/transfer` | POST | Admin | Transfer leadership |
| `/v1/config` | GET | Admin | Get runtime config (`?group_id=N` for multi-raft) |
| `/v1/config` | PATCH | Admin | Update runtime config (`group_id` in multi-raft body) |

**Admin Authentication:** If `admin_token` is configured, admin endpoints require:
```
Authorization: Bearer <admin_token>
```

### 9.2 Core Metrics

#### Leadership & Elections

| Metric | Type | Description |
|--------|------|-------------|
| `raft_role` | Gauge | Current role: 0=follower, 1=candidate, 2=leader |
| `raft_current_term` | Gauge | Current Raft term |
| `raft_elections_total` | Counter | Total elections started |
| `raft_leader_elected_total` | Counter | Total successful leader elections |
| `raft_election_timeouts_total` | Counter | Total election timeouts |

#### Proposals & Commits

| Metric | Type | Description |
|--------|------|-------------|
| `raft_propose_total` | Counter | Total proposals received |
| `raft_commits_total` | Counter | Total log entries committed |
| `raft_commit_index` | Gauge | Current commit index |
| `raft_applied_index` | Gauge | Last index applied to state machine |
| `raft_proposal_latency_seconds` | Histogram | End-to-end proposal latency |

#### Log Replication

| Metric | Type | Description |
|--------|------|-------------|
| `raft_appendentries_sent_total` | Counter | AE requests sent |
| `raft_appendentries_retries_total` | Counter | AE retries |
| `raft_appendentries_success_total` | Counter | Successful AE responses |
| `raft_appendentries_failure_total` | Counter | Failed AE responses |
| `raft_appendentries_received_total` | Counter | AE requests received |

#### ReadIndex

| Metric | Type | Description |
|--------|------|-------------|
| `raft_readindex_total` | Counter | Total ReadIndex requests |
| `raft_readindex_lease_total` | Counter | ReadIndex served from leader lease |
| `raft_readindex_heartbeats_sent_total` | Counter | Heartbeats sent for ReadIndex quorum |
| `raft_readindex_acks_received_total` | Counter | Acknowledgments received for ReadIndex |
| `raft_readindex_completed_total` | Counter | Completed ReadIndex operations |
| `raft_readindex_latency_seconds` | Histogram | ReadIndex latency |

#### Snapshots

| Metric | Type | Description |
|--------|------|-------------|
| `raft_snapshot_sends_started_total` | Counter | Snapshot transfers started |
| `raft_snapshot_chunks_sent_total` | Counter | Snapshot chunks sent |
| `raft_snapshot_sends_completed_total` | Counter | Successful snapshot transfers |
| `raft_snapshots_received_total` | Counter | Snapshots received |
| `raft_snapshots_created_total` | Counter | Snapshots created, labeled by `trigger` (`auto` or `manual`) |
| `raft_log_compactions_total` | Counter | Log compaction events |
| `raft_log_entries_compacted_total` | Counter | Log entries removed by compaction |

#### Voting

| Metric | Type | Description |
|--------|------|-------------|
| `raft_requestvote_sent_total` | Counter | RequestVote requests sent |
| `raft_requestvote_received_total` | Counter | RequestVote requests received |
| `raft_votes_granted_total` | Counter | Votes granted |
| `raft_prevote_sent_total` | Counter | PreVote requests sent |
| `raft_prevote_received_total` | Counter | PreVote requests received |

#### Transport & Cluster Health

| Metric | Type | Description |
|--------|------|-------------|
| `transport_peer_state` | Gauge | Peer connection state: 0=disconnected, 1=connecting, 2=connected, 3=failed |
| `raft_transport_peer_connected` | Gauge | 1 if peer is connected, 0 otherwise |
| `raft_transport_connections_total` | Counter | Total successful connections established |
| `raft_dead_nodes_detected_total` | Counter | Dead node detections by leader |
| `raft_checkquorum_stepdown_total` | Counter | Leader step-downs due to CheckQuorum |
| `raft_heartbeat_coalesced_total` | Counter | Heartbeats skipped due to coalescing |

#### Quiesced Mode

| Metric | Type | Description |
|--------|------|-------------|
| `raft_quiesced_mode_entered_total` | Counter | Times quiesced mode was entered |
| `raft_quiesced_mode_exited_total` | Counter | Times quiesced mode was exited |
| `raft_quiesced_mode_active` | Gauge | 1 if currently quiesced, 0 otherwise |

### 9.3 Histogram Buckets

RollingRaft uses the following fixed buckets for latency histograms:

```
0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
```

Units: **seconds**.

### 9.4 Prometheus Configuration Example

```yaml
scrape_configs:
  - job_name: 'rollingraft'
    static_configs:
      - targets: ['node1:9001', 'node2:9001', 'node3:9001']
    metrics_path: '/metrics'
    scrape_interval: 10s
```

### 9.5 Grafana Dashboard JSON (Snippet)

```json
{
  "panels": [
    {
      "title": "Raft Role",
      "targets": [
        {
          "expr": "raft_role",
          "legendFormat": "node {{node_id}}"
        }
      ],
      "type": "timeseries"
    },
    {
      "title": "Proposal Latency P99",
      "targets": [
        {
          "expr": "histogram_quantile(0.99, sum(rate(raft_proposal_latency_seconds_bucket[5m])) by (le))",
          "legendFormat": "p99"
        }
      ],
      "type": "timeseries"
    },
    {
      "title": "Commit Index",
      "targets": [
        {
          "expr": "raft_commit_index",
          "legendFormat": "node {{node_id}}"
        }
      ],
      "type": "timeseries"
    }
  ]
}
```

### 9.6 Recommended Alerts

| Alert | PromQL | Threshold |
|-------|--------|-----------|
| No Leader | `max(raft_role) < 2` | > 30s |
| High Proposal Latency | `histogram_quantile(0.99, raft_proposal_latency_seconds_bucket) > 1` | > 5m |
| Follower Lag | `max(raft_commit_index) - min(raft_commit_index) > 1000` | > 5m |
| Frequent Elections | `rate(raft_leader_elected_total[5m]) > 0.1` | > 5m |
| Snapshot Transfer Failing | `rate(raft_snapshot_sends_started_total[5m]) > rate(raft_snapshot_sends_completed_total[5m])` | > 10m |

---

## Appendix: Quick Reference

### Status Codes

| Code | Meaning |
|------|---------|
| `OK()` | Success |
| `Error("NOT_LEADER", ...)` | Not the leader — redirect to leader |
| `Error("OLD_SEQUENCE", ...)` | seq_num too old for session |
| `Error("TIMEOUT", ...)` | Operation timed out |
| `Error("CONFIG_CHANGE_PENDING", ...)` | Another membership change in progress |

### Type Aliases

```cpp
using NodeId = int32_t;    // Unique node identifier
using NodeAddr = std::string;  // "host:port"
using Term = uint32_t;     // Raft epoch
using Index = uint32_t;    // Log position (1-based)
```

### Include Paths

```cpp
#include "rollingraft/raft_node.h"      // RaftNode, RaftNodeConfig
#include "rollingraft/state_machine.h"  // StateMachine, Snapshot
#include "rollingraft/client.h"         // Client, ClientOptions
#include "rollingraft/event.h"          // EventBus, events
#include "rollingraft/persister.h"      // Persister, PersistentState
#include "rollingraft/status.h"         // Status
#include "rollingraft/types.h"          // NodeId, Term, Index
```
