# RollingRaft

![](./assets/rolling.gif)

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/Aqiu16717/rollingraft/actions/workflows/ci.yml/badge.svg)](https://github.com/Aqiu16717/rollingraft/actions)

A modern C++ implementation of the [Raft consensus algorithm](https://raft.github.io/) for learning and experimentation.

> ⚠️ **Development Status**: This project is currently in development/testing phase. It is **NOT production-ready** and should not be used for critical systems without extensive testing and validation.

## Features

- **Modern C++20** - Clean, type-safe implementation with modern C++ idioms
- **Easy Integration** - Header-only public interface, just link and use
- **Pluggable Architecture** - Customize network transport, persistent storage, timers, and protocol
- **Built-in Components** - TCP transport, LevelDB persistence, ASIO timers, JSON protocol
- **High-Level Client Library** - Built-in client with automatic leader discovery, retry logic, and connection pooling
- **Raft Features**:
  - Leader election with randomized timeouts
  - Log replication with batching (`ProposeBatch` API)
  - Snapshot support with automatic triggering
  - Log compaction with configurable retention (`TruncatePrefix`)
  - Membership changes (add/remove nodes dynamically)
  - ReadIndex for linearizable reads
  - Fine-grained locking per manager (election / log / snapshot / membership / applier)
- **Observability**: Built-in Prometheus-style metrics with HTTP `/metrics` endpoint
- **Transport Security**: TLS encryption and optional node-identity mTLS using URI SANs

## Current Status

| Feature | Status | Notes |
|---------|--------|-------|
| Leader Election | ✅ Implemented | Passes unit tests |
| Log Replication | ✅ Implemented | With persistence |
| Snapshot Transfer | ✅ Implemented | Auto-trigger by entry count / size |
| Membership Change | ✅ Implemented | Add/remove nodes |
| ReadIndex | ✅ Implemented | Heartbeat confirmation to majority |
| Auto Snapshot | ✅ Implemented | Entry/byte threshold with log truncation |
| Log Compaction | ✅ Implemented | `TruncatePrefix` with retention buffer |
| Performance Tests | ✅ Implemented | Throughput / latency / failover benchmarks |
| Metrics | ✅ Implemented | Prometheus HTTP `/metrics` endpoint |

**Known Limitations:**
- Client requests are not yet bound to an authenticated application identity
- Disk-failure/slow-disk chaos injection and long-running soak coverage are incomplete
- Not battle-tested in production environments

**Use Cases:**
- ✅ Learning Raft consensus algorithm
- ✅ Educational projects
- ✅ Prototyping distributed systems
- ❌ Production financial systems
- ❌ Critical infrastructure
- ❌ Large-scale deployment without extensive testing

## Quick Start

### Prerequisites

- C++20 compatible compiler (GCC 10+, Clang 12+, MSVC 2019+)
- CMake 3.11+
- Git (for submodules)

### Build

RollingRaft uses a **Makefile** for convenient multi-config builds:

```bash
# Clone with submodules
git clone --recursive https://github.com/Aqiu16717/rollingraft.git
cd rollingraft

# Release build
make release

# Debug build
make debug

# With sanitizers
make asan
make tsan
make ubsan

# Run tests
make test

# Format check
make format
```

### CI Pipeline

Every push to `main` runs the full verification matrix (see `.github/workflows/ci.yml`):

| Dimension | Configurations |
|-----------|---------------|
| **Compilers** | GCC, Clang |
| **Platforms** | Ubuntu 22.04, macOS latest |
| **Build types** | Release, Debug |
| **Sanitizers** | ASan, TSan, UBSan |
| **Additional checks** | Werror, format-check (clang-format-18), Docker test |

All 10 jobs must pass before merge. 🔥

Or use CMake directly:

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Run Example

Start a 3-node counter cluster:

```bash
# Terminal 1 - Node 1 (becomes leader)
./example/counter/counter_server 1 8001 8002 8003

# Terminal 2 - Node 2
./example/counter/counter_server 2 8002 8001 8003

# Terminal 3 - Node 3
./example/counter/counter_server 3 8003 8001 8002
```

Interact with the cluster using the client:

```bash
./example/counter/counter_client 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003
> inc
[Client] Success. Response: 1
> add 10
[Client] Success. Response: 11
> dec
[Client] Success. Response: 10
```

## Usage Guide

### 1. Implement Your State Machine

```cpp
#include "rollingraft/state_machine.h"

class MyStateMachine : public rollingraft::StateMachine {
 public:
  rollingraft::ApplyResult Apply(std::span<const uint8_t> data, uint64_t index) override {
    std::string cmd(data.begin(), data.end());

    // Apply command to your state machine
    ProcessCommand(cmd);

    return {
      .success_ = true,
      .response_ = GetResult(),
      .applied_index_ = index
    };
  }

  uint64_t GetLastAppliedIndex() const override {
    return last_applied_index_.load();
  }

  std::shared_ptr<rollingraft::Snapshot> CreateSnapshot() override {
    // Create a snapshot of current state
    return std::make_shared<MySnapshot>(state_, last_applied_index_);
  }

  bool Restore(const std::vector<uint8_t>& snapshot) override {
    // Restore state from snapshot
    return Deserialize(snapshot);
  }

  void WaitIndex(uint64_t index, std::function<void()> cb) override {
    // For linearizable reads: callback when index is applied
    if (last_applied_index_.load() >= index) {
      cb();
    } else {
      waiters_.emplace(index, std::move(cb));
    }
  }

 private:
  std::atomic<uint64_t> last_applied_index_{0};
  std::multimap<uint64_t, std::function<void()>> waiters_;
};
```

### 2. Configure and Start Node

```cpp
#include "rollingraft/raft_node.h"

int main() {
  rollingraft::RaftNodeConfig config;
  config.node_id = 1;
  config.listen_addr = "127.0.0.1:8001";
  config.peers = {"127.0.0.1:8002", "127.0.0.1:8003"};
  config.data_dir = "./data/node1";

  // Optional: tune timeouts
  config.election_timeout_ms = 300;    // Randomized between 300-600ms
  config.heartbeat_interval_ms = 100;  // Leader heartbeat every 100ms
  config.snapshot_threshold = 10000;   // Snapshot every 10000 entries

  auto sm = std::make_shared<MyStateMachine>();
  rollingraft::RaftNode node(config, sm);

  // Optional: set callbacks
  node.SetRoleChangeCallback([](rollingraft::RaftNodeRole role, uint64_t term) {
    std::cout << "Role changed to " << rollingraft::RaftNodeRoleToString(role)
              << " in term " << term << std::endl;
  });

  node.SetLeaderChangeCallback([](rollingraft::NodeId id, const std::string& addr) {
    std::cout << "Leader changed to " << id << " at " << addr << std::endl;
  });

  // Start the node
  auto status = node.Start();
  if (!status.ok()) {
    std::cerr << "Failed to start: " << status.ToString() << std::endl;
    return 1;
  }

  // Run until interrupted
  // ...

  node.Stop();
  return 0;
}
```

### 3. Propose Commands (Leader Only)

```cpp
// Only the leader can propose commands
if (node.IsLeader()) {
  std::string command = "your_command_data";

  auto status = node.Propose(command, [](const rollingraft::ApplyResult& result) {
    if (result.success_) {
      std::cout << "Applied at index " << result.applied_index_
                << ", response: " << result.response_ << std::endl;
    } else {
      std::cerr << "Failed: " << result.error_message_ << std::endl;
    }
  });

  if (!status.ok()) {
    std::cerr << "Propose failed: " << status.ToString() << std::endl;
  }
}
```

### 4. Linearizable Reads (Leader Only)

Use `ReadIndex` for linearizable reads that won't return stale data:

```cpp
// Only the leader can perform linearizable reads
if (node.IsLeader()) {
  // ReadIndex confirms the leader is still valid by heartbeating to majority
  auto status = node.ReadIndex([]() {
    // Safe to read from state machine - we have confirmation from majority
    int value = state_machine.GetCurrentValue();
    std::cout << "Current value: " << value << std::endl;
  });

  if (!status.ok()) {
    std::cerr << "ReadIndex failed: " << status.ToString() << std::endl;
  }
}
```

### 5. Client Library (Recommended)

The easiest way to interact with a Raft cluster:

```cpp
#include "rollingraft/client.h"

// Create client with cluster addresses
rollingraft::Client client({
  "127.0.0.1:8001",
  "127.0.0.1:8002",
  "127.0.0.1:8003"
});

// Execute command - automatically finds leader and retries
auto result = client.Execute("inc");
if (result.ok()) {
  std::cout << "Response: " << result.value() << std::endl;
} else {
  std::cerr << "Error: " << result.error_message() << std::endl;
}

// Query for read-only operations
auto query_result = client.Query("get counter");
if (query_result.ok()) {
  std::cout << "Value: " << query_result.value() << std::endl;
}

// Async execution
client.ExecuteAsync("add 10", [](rollingraft::ClientResult result) {
  if (result.ok()) {
    std::cout << "Async response: " << result.value() << std::endl;
  }
});
```

**Client Features:**
- Automatic leader discovery and failover
- Exponential backoff with jitter for retries
- Connection pooling and reuse
- Configurable timeouts and retry policies
- Thread-safe for concurrent use

### 6. Low-Level Client RPC

For direct RPC without the client library:

```cpp
#include "rollingraft/rpc.h"

rollingraft::ClientRequest req;
req.command = "inc";
req.client_id = 12345;  // Unique client ID
req.seq = seq_num++;    // Monotonic sequence number

rollingraft::ClientResponse resp;
auto status = rollingraft::RpcCall("127.0.0.1:8001", req, resp);

if (status.ok() && resp.success) {
  std::cout << "Response: " << resp.response << std::endl;
} else if (!resp.success) {
  // Not leader, redirect to leader
  std::cout << "Redirect to leader at " << resp.leader_addr << std::endl;
}
```

## Multi-Raft (Multiple Groups per Node)

RollingRaft can host many independent Raft groups in one process, sharing a
single network transport, timer service, and protocol stack:

- **`SharedNodeInfra`** — per-node shared infrastructure: network transport,
  timer service, protocol codec, metrics registry.
- **`RaftGroup`** — all per-group state: term, log, leader state, membership.
  Groups on the same node elect leaders and replicate independently.
- **`RaftStore`** — owns the infra plus the group table. Inbound RPCs carry a
  `group_id` and are routed to the matching group (`group_id == 0` keeps the
  legacy single-group path).

```
                 ┌─────────────────────────────────────┐
  node 1         │  RaftStore                          │
  (127.0.0.1)    │  ┌────────────┐  ┌────────────┐     │
                 │  │ group 1    │  │ group 2    │     │
                 │  │ term/log/  │  │ term/log/  │     │
                 │  │ leader     │  │ leader     │     │
                 │  └────────────┘  └────────────┘     │
                 │  SharedNodeInfra: transport/timers/ │
                 │  protocol/metrics (one per node)    │
                 └─────────────────────────────────────┘
        replicated across node 1, node 2, node 3 (each runs a RaftStore)
```

### Quick start (single binary, 3 nodes in one process)

```bash
make release
./build/release/example/example_multi_raft_3node
```

The demo starts 3 nodes on `127.0.0.1:9101-9103`, creates 2 groups per node,
waits for a leader per group, proposes 5 increments per group, and prints the
converged counters — every node × group cell must read `5`:

```
-------------------------------------------
 Multi-Raft 3-node demo started
 nodes: 127.0.0.1:9101-9103
 groups per node: 2
 data: /tmp/rollingraft_3node_demo
-------------------------------------------
group 1: leader elected, proposing 5 increments
group 2: leader elected, proposing 5 increments

Final counter state (all cells must read 5):
              group 1   group 2
  node 1        5         5
  node 2        5         5
  node 3        5         5

Demo complete: all groups converged, stores stopped, data removed.
```

### Running nodes in separate processes (multi-host)

For a multi-host deployment, run one `RaftStore` per process:

```bash
./build/release/example/example_multi_raft_server 1 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003
./build/release/example/example_multi_raft_server 2 127.0.0.1:8002 127.0.0.1:8001 127.0.0.1:8003
./build/release/example/example_multi_raft_server 3 127.0.0.1:8003 127.0.0.1:8001 127.0.0.1:8002
```

Each process hosts groups 1 and 2; see `example/multi_raft/multi_raft_client.cpp`
for the matching client. Integration coverage lives in
`tests/integration/test_multi_raft_2groups.cpp`.

## Configuration Reference

| Option | Default | Description |
|--------|---------|-------------|
| `node_id` | *required* | Unique identifier for this node |
| `listen_addr` | *required* | Address to listen on (e.g., "127.0.0.1:8001") |
| `peers` | *required* | List of peer addresses |
| `data_dir` | *required* | Directory for persistent storage |
| `election_timeout_ms` | 300 | Base election timeout (randomized 1x-2x) |
| `heartbeat_interval_ms` | 100 | Leader heartbeat interval |
| `max_entries_per_append` | 100 | Max entries per AppendEntries RPC |
| `snapshot_threshold` | 10000 | Entries before triggering auto-snapshot |
| `snapshot_max_size` | 1048576 | Max bytes before triggering auto-snapshot |
| `log_retention_entries` | 0 | Entries to keep before snapshot index |
| `metrics_enabled` | false | Enable Prometheus metrics endpoint |
| `metrics_addr` | `""` | Metrics HTTP listen address (e.g. "0.0.0.0:9090") |
| `rpc_timeout_ms` | 1000 | RPC call timeout |

## API Reference

### RaftNode

| Method | Description |
|--------|-------------|
| `Start()` | Start the node, begin participating in consensus |
| `Stop()` | Gracefully stop the node |
| `IsLeader()` | Check if this node is the current leader |
| `GetRole()` | Get current role (Follower/Candidate/Leader) |
| `CurrentTerm()` | Get current Raft term |
| `GetLeaderAddr()` | Get current leader's address |
| `Propose(cmd, callback)` | Propose a command (leader only) |
| `ReadIndex(callback)` | Linearizable read - callback when safe to read |
| `AddNode(id, addr)` | Add a new node to the cluster (leader only) |
| `RemoveNode(id)` | Remove a node from the cluster (leader only) |
| `GetConfig()` | Get current cluster configuration |
| `SetRoleChangeCallback(cb)` | Set callback for role changes |
| `SetLeaderChangeCallback(cb)` | Set callback for leader changes |

### Client

| Method | Description |
|--------|-------------|
| `Execute(command)` | Execute command with automatic leader discovery |
| `Execute(command, timeout)` | Execute with custom timeout |
| `Query(command)` | Read-only query |
| `ExecuteAsync(cmd, cb)` | Async execution with callback |
| `RefreshLeader()` | Force refresh leader cache |
| `GetLeaderAddr()` | Get current cached leader address |
| `IsHealthy()` | Check if cluster is reachable |
| `GetClientId()` | Get unique client ID |

### Status

```cpp
rollingraft::Status status = node.Start();

if (status.ok()) {
  // Success
} else if (status.IsNotLeader()) {
  // Not the leader
} else {
  std::cerr << status.ToString() << std::endl;
}
```

## Project Structure

```
rollingraft/
├── include/rollingraft/    # Public headers
│   ├── raft_node.h        # Main Raft node interface
│   ├── client.h           # High-level client library
│   ├── state_machine.h    # State machine interface
│   ├── metrics.h          # Prometheus-style metrics
│   ├── rpc.h              # RPC message definitions
│   ├── status.h           # Status codes
│   └── types.h            # Type definitions
├── src/                   # Implementation
│   ├── raft_node.cpp      # Core Raft logic
│   ├── raft_log.cpp       # Log management
│   ├── log_persister.cpp  # Batched async log persistence
│   ├── asio_network_transport.cpp
│   ├── leveldb_persister.cpp
│   ├── client/            # Client library internals
│   └── ...
├── example/               # Examples
│   └── counter/           # Counter service demo
├── tests/                 # Unit & integration tests
├── benchmark/             # Performance benchmarks
├── doc/                   # Design docs
└── third_party/           # Dependencies
    ├── asio/              # Networking
    ├── nlohmann_json/     # JSON
    ├── spdlog/            # Logging
    ├── leveldb/           # Persistence
    └── googletest/        # Testing
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Application                          │
│              (Your State Machine)                        │
└─────────────────────┬───────────────────────────────────┘
                      │ Apply / Propose
┌─────────────────────▼───────────────────────────────────┐
│                    RaftNode                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │   Election  │  │ Log Replica │  │   Snapshot      │  │
│  │   Manager   │  │   Manager   │  │   Manager       │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
│  ┌─────────────┐  ┌─────────────┐                       │
│  │  Membership │  │   Applier   │                       │
│  │   Manager   │  │   Manager   │                       │
│  └─────────────┘  └─────────────┘                       │
└─────────────────────┬───────────────────────────────────┘
                      │
    ┌─────────────────┼─────────────────┐
    │                 │                 │
┌───▼────┐     ┌─────▼──────┐   ┌──────▼──────┐
│Network │     │ Persister  │   │    Timer    │
│ (ASIO) │     │ (LevelDB)  │   │   (ASIO)    │
└────────┘     └────────────┘   └─────────────┘
```

### Lock Hierarchy

Fine-grained locks follow a strict hierarchy to prevent deadlocks:

```
election_mtx_ → replication_mtx_ → snapshot_mtx_ → membership_mtx_ → applier_mtx_
```

Cross-manager calls use two-phase or bridge patterns to maintain safety.

## Advanced Usage

### Custom Components

You can provide custom implementations for any component:

```cpp
// Custom network transport
config.network_factory = []() {
  return std::make_unique<MyNetworkTransport>();
};

// Custom persistent storage
config.persister_factory = []() {
  return std::make_unique<MyPersister>("./data");
};

// Custom timer service
config.timer_factory = []() {
  return std::make_unique<MyTimerService>();
};

// Custom protocol
config.protocol_factory = []() {
  return std::make_unique<MyProtocol>();
};
```

## Testing

RollingRaft includes comprehensive unit tests covering all major components:

```bash
# Build tests
cmake .. -DBUILD_TESTING=ON
make

# Run all tests
ctest --output-on-failure

# Run specific test suites
./build/tests/unit_tests --gtest_filter="Client*"
./build/tests/unit_tests --gtest_filter="RaftElection*"
```

**Test Coverage:** 170 unit tests + 9 integration tests
- Raft core: 81 tests (election, log replication, snapshots, membership, batch propose)
- Client library: 80 tests (result handling, leader tracking, retry policy, connection pool, client)
- Metrics: 6 tests (counter, gauge, histogram, registry)
- Log compaction: 3 tests (truncate prefix, buffer safety, retention math)

### Benchmarks

```bash
# Throughput benchmark
./build/benchmark/benchmark_client -t write -d 30 -c 4 127.0.0.1:8001

# Latency vs throughput curve
./build/benchmark/benchmark_latency_curve -d 10 127.0.0.1:8001

# Failover recovery time
./build/benchmark/benchmark_failover \
  -k 'pkill -f "counter_server 1"' \
  127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003
```

## Recent Improvements

### Phase 2 — Network Layer & Core Refactoring

| Improvement | Details |
|-------------|---------|
| **Fine-grained Locks** | Replaced monolithic `mtx_` with 5 manager-specific mutexes following strict hierarchy |
| **ASIO Thread Pool** | Replaced per-RPC `std::thread` with ASIO thread pool + strand serialization |
| **Correlation ID** | RPC matching via `correlation_id` instead of FIFO callback queue |
| **Batch Propose** | `ProposeBatch()` API with atomic append + shared result collection |
| **CI Stability** | 10/10 jobs green including TSan, ASan, UBSan across GCC/Clang × Linux/macOS |
| **Test Coverage** | 170 unit tests + 9 integration tests, all passing under TSan |

See [`doc/`](doc/) for design documents and [`doc/commit-message-convention.md`](doc/commit-message-convention.md) for commit standards.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Raft Consensus Paper](https://raft.github.io/raft.pdf) - Diego Ongaro and John Ousterhout
- [raft.github.io](https://raft.github.io/) - Raft resources and implementations
