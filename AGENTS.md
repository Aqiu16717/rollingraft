# RollingRaft — Agent Guide

This file contains project-specific information intended for AI coding agents. The reader is expected to know nothing about the project.

---

## Project Overview

RollingRaft is a modern C++20 implementation of the [Raft consensus algorithm](https://raft.github.io/). It provides a pluggable distributed consensus library with a high-level client library, built-in TCP networking, LevelDB persistence, and Prometheus-style metrics.

- **Version**: 0.1.0
- **Language**: C++20
- **License**: MIT
- **Status**: Development/testing phase — **NOT production-ready**
- **Repository**: https://github.com/Aqiu16717/rollingraft

### What This Project Does

- Implements leader election, log replication, snapshot transfer, and dynamic membership changes
- Provides a `RaftNode` class that applications embed alongside a custom `StateMachine`
- Ships built-in components: ASIO-based TCP transport, LevelDB persister, ASIO timers, JSON protocol
- Includes a high-level `Client` library with automatic leader discovery, retry logic, and connection pooling
- Exposes Prometheus-style metrics via an HTTP `/metrics` endpoint

### Known Limitations

- No TLS encryption for node-to-node communication
- No runtime configuration hot-reload
- Not battle-tested in production environments
- See `doc/todo.md` for a detailed gap analysis (WAL sync semantics, disk-full handling, log corruption detection, etc.)

---

## Technology Stack

| Component | Technology | Notes |
|-----------|-----------|-------|
| Build System | CMake 3.14+ | Root `CMakeLists.txt` drives everything |
| C++ Standard | C++20 | Required; extensions disabled |
| Networking | ASIO (standalone) | Header-only, bundled in `third_party/nonboost_asio/` |
| Serialization | nlohmann/json | Header-only, bundled in `third_party/nlohmann_json/` |
| Logging | spdlog | Header-only, bundled in `third_party/spdlog/` |
| Persistence | LevelDB | Bundled submodule; patched for macOS/C++20 compatibility |
| Testing | GoogleTest | Bundled submodule; used via `gtest_discover_tests` |
| Metrics | Custom | Prometheus-style Counter/Gauge/Histogram + HTTP server |

### Supported Compilers

- GCC 10+
- Clang 12+
- MSVC 2019+

### System Dependencies

- `libsnappy-dev` (for LevelDB)
- Standard build tools: `build-essential`, `cmake`, `git`

---

## Project Structure

```
rollingraft/
├── include/rollingraft/          # Public API headers
│   ├── raft_node.h               # Main RaftNode class
│   ├── client.h                  # High-level client library
│   ├── state_machine.h           # StateMachine interface
│   ├── network_transport.h       # NetworkTransport interface
│   ├── persister.h               # Persister interface
│   ├── protocol.h                # Protocol interface
│   ├── timer_service.h           # TimerService interface
│   ├── rpc.h                     # RPC message definitions
│   ├── status.h                  # Status codes
│   ├── types.h                   # Core type aliases (NodeId, Term, Index, ...)
│   ├── metrics.h                 # Metrics types
│   ├── log_persister.h           # Batched async log persistence
│   └── logger.h                  # Logging interface
├── src/                          # Implementation
│   ├── raft_node.cpp             # Core Raft logic (PIMPL: RaftNodeImpl)
│   ├── raft_log.cpp              # In-memory log management
│   ├── asio_network_transport.cpp # TCP transport
│   ├── asio_timer_service.cpp/h   # ASIO timer service
│   ├── leveldb_persister.cpp     # LevelDB state persistence
│   ├── log_persister.cpp         # Async batched log writes
│   ├── json_protocol.cpp/h       # JSON serialization
│   ├── client.cpp                # Client library implementation
│   ├── client/                   # Client internals
│   │   ├── connection_pool.cpp   # TCP connection pooling
│   │   ├── leader_tracker.cpp    # Leader discovery/caching
│   │   └── retry_policy.cpp      # Exponential backoff retry
│   ├── metrics.cpp               # Metrics collection
│   ├── metrics_http_server.cpp   # HTTP /metrics endpoint
│   ├── logger.cpp                # Logger implementation
│   ├── logger_spdlog_adapter.cpp # spdlog adapter
│   ├── rpc_client.cpp            # Low-level RPC client
│   └── status.cpp                # Status helpers
├── tests/                        # Test suite
│   ├── unit/                     # 148 unit tests
│   ├── integration/              # 9 integration tests
│   ├── mock/                     # Test mocks (network, timer, persister, state machine)
│   └── CMakeLists.txt            # Test targets: unit_tests, integration_tests
├── example/                      # Examples and demos
│   ├── counter/                  # 3-node distributed counter
│   └── client_example.cpp        # Standalone client example
├── benchmark/                    # Performance benchmarks
│   ├── benchmark.cpp/h           # Benchmark framework
│   ├── client_benchmark.cpp      # Throughput benchmark
│   ├── latency_curve_benchmark.cpp
│   └── failover_benchmark.cpp
├── third_party/                  # Git submodules
│   ├── googletest/
│   ├── leveldb/
│   ├── nlohmann_json/
│   ├── nonboost_asio/
│   └── spdlog/
├── cmake/patches/                # Build patches (leveldb-macos-cxx20.patch)
├── scripts/                      # Helper scripts
│   ├── build.sh                  # Build script (debug/test/benchmark/install)
│   ├── docker-test.sh            # Docker-based integration testing
│   └── diagnose.sh               # Local 3-node diagnostic script
├── .github/workflows/ci.yml      # GitHub Actions CI
├── docker-compose.yml            # 3-node cluster + integration tests
├── Dockerfile                    # Multi-stage build
├── .clang-format                 # Google style formatting rules
├── CONTRIBUTING.md               # Contribution guidelines
└── doc/todo.md                   # Development roadmap and known issues
```

---

## Build and Test Commands

### Quick Build (Release)

```bash
# Clone with submodules
git clone --recursive https://github.com/Aqiu16717/rollingraft.git
cd rollingraft

# Configure and build
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Using the Build Script

```bash
./build.sh              # Release build
./build.sh debug        # Debug build
./build.sh test         # Build and run all tests
./build.sh benchmark    # Build benchmarks
./build.sh install      # Build and install
./build.sh clean        # Remove build directory
```

### CMake Options

| Option | Default | Description |
|--------|---------|-------------|
| `BUILD_TESTING` | `ON` | Build unit and integration tests |
| `BUILD_EXAMPLES` | `ON` | Build example executables |
| `BUILD_BENCHMARK` | `ON` | Build benchmark executables |
| `BUILD_SHARED_LIBS` | `OFF` | Build shared instead of static library |

### Running Tests

```bash
# All tests (unit + integration)
ctest --output-on-failure

# Unit tests only
./build/tests/unit_tests
./build/tests/unit_tests --gtest_filter="RaftElection*"

# Integration tests only
./build/tests/integration_tests

# Custom CMake targets
make run_unit_tests
make run_integration_tests
```

### Running the Counter Example

```bash
# Terminal 1
./build/example/example_counter_server 1 8001 8002 8003

# Terminal 2
./build/example/example_counter_server 2 8002 8001 8003

# Terminal 3
./build/example/example_counter_server 3 8003 8001 8002

# Terminal 4 — interactive client
./build/example/example_counter_client 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003
> inc
> add 10
> dec
```

### Docker-Based Testing

```bash
# Full Docker test suite (build + cluster + integration tests + cleanup)
./scripts/docker-test.sh full

# Start a 3-node cluster locally
./scripts/docker-test.sh up

# Run integration tests against running cluster
./scripts/docker-test.sh test

# Interactive client against Docker cluster
./scripts/docker-test.sh client

# Stop and cleanup
./scripts/docker-test.sh down
```

---

## Code Style Guidelines

We follow the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) with project-specific conventions.

### Formatting

- Use `.clang-format` in the project root (BasedOnStyle: Google)
- Format before committing:
  ```bash
  clang-format -i src/your_file.cpp
  # Or all modified files:
  git diff --name-only | grep -E '\.(cpp|h)$' | xargs clang-format -i
  ```

### Naming Conventions

| Type | Style | Example |
|------|-------|---------|
| Classes / Structs | PascalCase | `RaftNode`, `LogEntry` |
| Public functions | PascalCase | `Start()`, `Propose()` |
| Variables | snake_case | `current_term`, `is_leader` |
| Member variables | trailing underscore | `term_`, `state_` |
| Constants | `k_` prefix | `k_default_port` |
| Enums | PascalCase type, UPPER_SNAKE_CASE values | `FOLLOWER`, `LEADER` |
| Template parameters | PascalCase | `typename T` |

### Code Organization

- Public interface first, then private implementation
- Use PIMPL for large public classes (e.g., `RaftNode` → `RaftNodeImpl`)
- All comments in **English**
- Public APIs use `/** */` Doxygen style
- Inline comments use `//` and explain **why**, not **what**

### Includes

The `.clang-format` defines include priorities:
1. `(benchmarks|db|helpers)/`
2. `"rollingraft/..."` (public headers)
3. `(issues|port|third_party|util)/`
4. Everything else

---

## Testing Instructions

### Test Structure

- **Unit tests**: `tests/unit/` — isolated component tests using mocks
- **Integration tests**: `tests/integration/` — multi-node cluster scenarios
- **Mocks**: `tests/mock/` — `MockNetworkTransport`, `MockTimerService`, `MockPersister`, `MockStateMachine`

### Test Naming Convention

```cpp
// Format: <Class>Test.<Scenario>_<ExpectedBehavior>
TEST_F(RaftNodeTest, Propose_AsFollower_ReturnsNotLeader) { ... }
TEST_F(LogReplicationTest, Follower_RejectPropose) { ... }
```

### Mock Usage Example

```cpp
#include "mock/mock_network.h"
#include "mock/mock_state_machine.h"
#include "mock/mock_persister.h"
#include "mock/mock_timer.h"

MockNetworkTransport network;
network.SetAutoResponse("{\"success\": true}", true);
```

### Required Practices

- All new features must have unit tests
- Complex scenarios need integration tests
- Code must build with zero warnings (`-Wall -Wextra -Wpedantic` on GCC/Clang, `/W4` on MSVC)
- All tests must pass before submitting changes

---

## Architecture Notes

### Pluggable Components

`RaftNodeConfig` accepts factory functions for dependency injection:

```cpp
config.network_factory   = []() { return std::make_unique<MyNetworkTransport>(); };
config.persister_factory = []() { return std::make_unique<MyPersister>(); };
config.timer_factory     = []() { return std::make_unique<MyTimerService>(); };
config.protocol_factory  = []() { return std::make_unique<MyProtocol>(); };
```

Default implementations:
- **Network**: ASIO TCP transport (`asio_network_transport.cpp`)
- **Persister**: LevelDB (`leveldb_persister.cpp`)
- **Timer**: ASIO steady_timer (`asio_timer_service.cpp`)
- **Protocol**: JSON over TCP (`json_protocol.cpp`)

### Key Internal Data Structures (in `raft_node.cpp`)

- `PendingProposal` — proposals awaiting commit
- `ClientSession` — idempotency tracking per client
- `SnapshotSendState` — leader-side snapshot streaming state
- `PendingReadIndex` — linearizable read coordination

### Threading Model

- `RaftNode` public methods are thread-safe
- Core logic runs on an ASIO io_context
- `Propose()` and `ReadIndex()` callbacks are invoked asynchronously from internal threads
- `LogPersister` batches and flushes log entries asynchronously

---

## CI / CD

### GitHub Actions (`.github/workflows/ci.yml`)

Triggered on `push`/`pull_request` to `main` or `master`:

1. **build-and-test** (ubuntu-22.04)
   - Installs dependencies (`build-essential`, `cmake`, `libsnappy-dev`)
   - Caches `build/` directory via `actions/cache@v4`
   - Configures with `-DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DBUILD_EXAMPLES=ON`
   - Builds with `cmake --build build --verbose -j$(nproc)`
   - Runs `ctest --test-dir build --output-on-failure`
   - Runs `./build/tests/integration_tests` separately for verbose output

2. **docker-test**
   - Runs `./scripts/docker-test.sh full`

### Known CI Issue

Caching the full `build/` directory may skip `gtest_discover_tests` on cache hits, making new tests invisible to `ctest`. The proper fix is to cache only compiler artifacts (e.g., ccache), not generated test files.

---

## Security Considerations

- **Node-to-node TCP is plaintext** — no TLS or authentication
- **No client/node identity verification** — anyone who can reach a node can propose commands
- **No checksums on log entries or snapshots** — corrupted data may be applied silently
- **Disk-full handling is a no-op** — `CheckDiskSpace()` returns `Status::OK()` unconditionally
- Do **not** deploy on public networks or for critical systems without addressing the above

---

## Commit Message Guidelines

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

<body>

<footer>
```

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation changes |
| `style` | Formatting, no logic change |
| `refactor` | Code refactoring |
| `perf` | Performance improvements |
| `test` | Adding or updating tests |
| `chore` | Build, dependencies, tooling |

Rules:
- Imperative mood: "add" not "added"
- No capitalization on subject line, no trailing period
- Subject max 50 characters, body wrapped at 72 characters
- Use `*` for bullet points in body

---

## Useful References

- `README.md` — User-facing documentation, API reference, usage examples
- `CONTRIBUTING.md` — Full contribution workflow, PR checklist, release process
- `doc/todo.md` — Development roadmap, production readiness gaps, known technical debt
- `example/counter/DESIGN.md` — Design of the counter example (mixed Chinese/English)
- `example/counter/README.md` — Counter example usage instructions
- `benchmark/README.md` — Benchmark usage and interpretation
