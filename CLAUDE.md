# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RollingRaft is a C++20 Raft consensus library built on Asio. It provides an integrated network layer, storage layer, and consensus logic - users only need to implement the `StateMachine` interface for their business logic.

## Build Commands

```bash
# Configure the build (generates compile_commands.json for LSP)
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -B build

# Build the library and examples
cmake --build build

# Build specific targets
cmake --build build --target rollingraft
cmake --build build --target example_counter
cmake --build build --target example_server_thread

# Format code (uses .clang-format with Google style)
clang-format -i --style=file src/file.cpp
find . -iname '*.cpp' -o -iname '*.h' | xargs clang-format -i --style=file
```

## Running Examples

```bash
# Build examples
cmake --build build

# Run counter example (3-node cluster)
mkdir -p data/node1 data/node2 data/node3
./build/example/example_counter 1 8001 8002 8003  # Terminal 1
./build/example/example_counter 2 8002 8001 8003  # Terminal 2
./build/example/example_counter 3 8003 8001 8002  # Terminal 3
```

## Code Architecture

### Core Components

**RaftNode** (`include/rollingraft/raft_node.h`, `src/raft_node.cpp`)
- PIMPL pattern: `RaftNode` is the public API, `RaftNodeImpl` contains implementation
- Manages the three Raft states: Follower, Candidate, Leader
- Handles RequestVote and AppendEntries RPCs
- Configuration via `RaftNodeConfig` struct

**StateMachine** (`include/rollingraft/state_machine.h`)
- Abstract interface users must implement
- Key methods:
  - `Apply()` - Apply committed commands (write operations)
  - `CreateSnapshot()` - Create lightweight snapshot handle
  - `Restore()` - Restore from snapshot data
  - `WaitIndex()` - For linearizable reads

**Server** (`include/rollingraft/server.h`, `src/server.cpp`)
- Asio-based TCP server for node-to-node communication
- Handles RPC message receiving and dispatching

### RPC Protocol

Uses JSON serialization (`src/json_protocol.cpp`). Core message types in `include/rollingraft/rpc.h`:
- `RequestVoteRequest/Response` - Leader election
- `AppendEntriesRequest/Response` - Log replication and heartbeats
- `InstallSnapshotRequest/Response` - Snapshot transfer

### Project Structure

```
include/rollingraft/     # Public API headers
  raft_node.h            # Main API: RaftNode class
  state_machine.h        # User-implemented interface
  rpc.h                  # RPC message structs
  server.h               # TCP server
  status.h               # Error handling (leveldb-style)
  raft_log.h             # Log entry structures
  persister.h            # Storage interface
  protocol.h             # Serialization interface
  logger.h               # Logging interface

src/                     # Implementation
  raft_node.cpp          # Core Raft logic
  server.cpp             # Asio TCP server
  json_protocol.cpp      # JSON serialization
  raft_command_handler.cpp  # Command processing
  status.cpp             # Status implementation
  logger*.cpp            # spdlog integration

example/                 # Examples
  counter/               # Distributed counter demo
  server_thread/         # Multi-threading example
```

## Code Style

- **Standard**: C++20
- **Style**: Google C++ Style Guide (enforced via `.clang-format`)
- **Line length**: 80 characters
- **Indentation**: 2 spaces
- **Pointer alignment**: Left-aligned (`Type* ptr`)

### Naming Conventions

- Classes/Structs: PascalCase (`RaftNode`, `Server`)
- Functions: PascalCase for public methods (`Start`, `Stop`)
- Variables: snake_case (`current_term`, `is_leader`)
- Member variables: trailing underscore (`term_`, `state_`)
- Constants: `k_` prefix (`k_default_port`)

## Third-Party Dependencies

All vendored in `third_party/`:
- **nonboost_asio** - Networking library (standalone, no Boost)
- **spdlog** - Logging
- **nlohmann_json** - JSON serialization
- **googletest** - Testing framework (currently unused)

## Key Design Principles

From `AGENTS.md`:
1. **KISS** - Prefer simple solutions; avoid premature abstraction
2. **No "AI Smell"** - Avoid over-commenting obvious logic; don't over-design
3. **Dependencies are acceptable** - Integrated solution over zero-dependency

## Common Tasks

### Implementing a New State Machine

1. Create a class inheriting from `rollingraft::StateMachine`
2. Implement `Apply()` for write operations
3. Implement `CreateSnapshot()` and `Restore()` for persistence
4. Implement `WaitIndex()` for linearizable reads

See `example/counter/counter_server.cpp` for a complete example.

### Adding New RPC Messages

1. Add struct definitions to `include/rollingraft/rpc.h`
2. Add serialization/deserialization in `src/json_protocol.cpp`
3. Add handling logic in `RaftNodeImpl`

### Running Tests

Currently the test suite is empty (`tests/` directory exists but contains no tests). Tests should be added to `tests/` and enabled in `tests/CMakeLists.txt`.

## Current Implementation State

As noted in `DESIGN.md` and `AGENTS.md`:
- Project structure: ✅ Complete
- RPC message definitions: ✅ Complete
- Server framework: ✅ Complete
- Raft election logic: ⚠️ Needs completion
- Log replication: ⚠️ Needs completion
- Client protocol: ⚠️ Not yet implemented
