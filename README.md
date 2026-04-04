# RollingRaft

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A modern, production-ready C++ implementation of the [Raft consensus algorithm](https://raft.github.io/).

## Features

- **Modern C++20** - Clean, type-safe implementation with modern C++ idioms
- **Easy Integration** - Header-only public interface, just link and use
- **Pluggable Architecture** - Customize network transport, persistent storage, timers, and protocol
- **Production Defaults** - Built-in TCP transport, LevelDB persistence, ASIO timers, JSON protocol
- **Complete Raft Features**:
  - Leader election with randomized timeouts
  - Log replication with batching
  - Snapshot support for log compaction
  - Membership changes (add/remove nodes dynamically)
  - Linearizable reads (read index)

## Quick Start

### Prerequisites

- C++20 compatible compiler (GCC 10+, Clang 12+, MSVC 2019+)
- CMake 3.11+
- Git (for submodules)

### Build

```bash
# Clone with submodules
git clone --recursive https://github.com/Aqiu16717/rollingraft.git
cd rollingraft

# Build
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

### 5. Client RPC

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
| `snapshot_threshold` | 10000 | Entries before triggering snapshot |
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
│   ├── state_machine.h    # State machine interface
│   ├── rpc.h              # RPC message definitions
│   ├── status.h           # Status codes
│   └── types.h            # Type definitions
├── src/                   # Implementation
│   ├── raft_node.cpp      # Core Raft logic
│   ├── raft_log.cpp       # Log management
│   ├── asio_network_transport.cpp
│   ├── leveldb_persister.cpp
│   ├── raft_timer.cpp     # Timer service
│   └── ...
├── example/               # Examples
│   └── counter/           # Counter service demo
├── tests/                 # Unit tests
└── third_party/           # Dependencies
    ├── asio/              # Networking
    ├── nlohmann_json/     # JSON
    ├── spdlog/            # Logging
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
│  │   (5.2)     │  │   (5.3)     │  │   (7)           │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
└─────────────────────┬───────────────────────────────────┘
                      │
    ┌─────────────────┼─────────────────┐
    │                 │                 │
┌───▼────┐     ┌─────▼──────┐   ┌──────▼──────┐
│Network │     │ Persister  │   │    Timer    │
│ (TCP)  │     │ (LevelDB)  │   │   (ASIO)    │
└────────┘     └────────────┘   └─────────────┘
```

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

```bash
# Build tests
cmake .. -DBUILD_TESTING=ON
make

# Run tests
ctest --output-on-failure
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Raft Consensus Paper](https://raft.github.io/raft.pdf) - Diego Ongaro and John Ousterhout
- [raft.github.io](https://raft.github.io/) - Raft resources and implementations
