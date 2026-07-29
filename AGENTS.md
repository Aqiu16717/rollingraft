# RollingRaft — Agent Guide

Project-specific information for AI coding agents. The reader is expected to know nothing about the project.

---

## Project Overview

RollingRaft is a modern C++20 implementation of the [Raft consensus algorithm](https://raft.github.io/) — a pluggable distributed consensus library with a high-level client library, built-in TCP networking, multiple persistence backends, and Prometheus-style metrics.

- **License**: MIT
- **Status**: Development/testing phase — **NOT production-ready**
- **Repository**: https://github.com/Aqiu16717/rollingraft

Features: leader election (pre-vote, CheckQuorum, leader lease), log replication (pipelined, batched), snapshot transfer + log compaction, dynamic membership (joint consensus, learners), ReadIndex linearizable reads, and an in-progress **multi-raft** layer (many Raft groups per process).

### Known Limitations

- No client/node identity verification — anyone who can reach a node can propose commands
- No checksums on log entries or snapshots — corrupted data may be applied silently
- Disk-full handling is a no-op — `CheckDiskSpace()` returns `Status::OK()` unconditionally
- See `doc/todo.md` for the full gap analysis

---

## Build and Test

The **Makefile** is the primary interface. Each config builds into its own directory under `build/`, so configs coexist and `compile_commands.json` is always exported (ccache is used if installed).

```bash
make release          # or just `make` — build into build/release
make debug / asan / tsan / ubsan / werror
make test             # build release + run all tests via ctest
make unit-test        # unit tests only
make int-test         # integration tests only
make test-tsan        # tests under TSan (uses tsan_suppressions.txt)
make format / format-check
```

Run a single test against the built binaries:

```bash
./build/release/tests/unit_tests --gtest_filter="RaftElection*"
./build/release/tests/integration_tests --gtest_filter="MultiRaft*"
```

System dependency: `libsnappy-dev` (for LevelDB). Third-party deps are bundled as git submodules under `third_party/` (ASIO, nlohmann/json, spdlog, LevelDB — patched for macOS/C++20 via `cmake/patches/` — GoogleTest); clone with `--recursive`.

---

## Architecture

Public API lives in `include/rollingraft/`; all implementation in `src/`. Key public types: `RaftNode` (single-group node), `StateMachine` (application interface), `Client` (high-level client with leader discovery/retry/pooling).

### Core split

`RaftNode` is a PIMPL wrapper over `RaftNodeImpl` (`src/raft_node_impl.h`), which delegates to per-concern components, one file each:

- `election_manager.cpp` — elections, pre-vote, CheckQuorum, leader lease
- `log_replicator.cpp` — AppendEntries, pipelined inflight windows, heartbeat coalescing
- `snapshot_manager.cpp` — snapshot creation/transfer/compaction
- `membership_manager.cpp` — joint-consensus add/remove node, learners
- `state_machine_applier.cpp` — async apply thread, commit → apply pipeline
- `rpc_handlers.cpp` — inbound RPC dispatch
- `raft_log.cpp` / `raft_node_core.cpp` — in-memory log, shared core

### Multi-raft (active development)

A process can host many Raft groups over shared infrastructure:

- `SharedNodeInfra` (`src/shared_node_infra.h`) — one network transport, timer service, protocol, metrics registry, runtime config per **node**, injected into every group.
- `RaftGroup` (`src/raft_group.h`) — all **per-group** state (term, log, indices, leader state, membership, locks). Members are deliberately public for incremental migration.
- `RaftStore` (`src/raft_store.h`) — owns the infra + group table. Inbound RPCs carry a `group_id` routed to the matching group; **group_id 0 is the legacy single-group path**. A single shared tick timer drives all groups' deadlines via `OnStoreTick()` — do not add per-group timers.
- Example: `example/multi_raft/`; integration test: `tests/integration/test_multi_raft_2groups.cpp`.

### Persistence

Pluggable via `RaftNodeConfig` factories (`network_factory`, `persister_factory`, `timer_factory`, `protocol_factory`). Implementations: `leveldb_persister.cpp`, `wal_persister.cpp` (write-ahead log), `hybrid_persister.cpp`, `log_persister.cpp` (batched async flush; `flushed_index_` tracks durability). `group_commit_controller.*` batches fsyncs across proposals.

### Threading & locks

- Core logic runs on an ASIO io_context; `RaftNode` public methods are thread-safe; `Propose()`/`ReadIndex()` callbacks fire from internal threads.
- Strict lock hierarchy (never acquire out of order, documented in `raft_group.h`):
  `election_mtx_ → replication_mtx_ → snapshot_mtx_ → membership_mtx_ → applier_mtx_`
- Cross-manager calls use two-phase / bridge patterns to respect the hierarchy.

---

## Code Style

[Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) via `.clang-format` (BasedOnStyle: Google). Code must build warning-free (`-Wall -Wextra -Wpedantic`; CI has a `-Werror` job). Run `make format` before committing.

| Type | Style | Example |
|------|-------|---------|
| Classes / Structs | PascalCase | `RaftNode`, `LogEntry` |
| Public functions | PascalCase | `Start()`, `Propose()` |
| Variables | snake_case | `current_term` |
| Member variables | trailing underscore | `term_`, `state_` |
| Constants | `k_` prefix | `k_default_port` |
| Enums | PascalCase type, UPPER_SNAKE_CASE values | `FOLLOWER`, `LEADER` |

- `Status` return codes for fallible ops
- Public APIs use `/** */` Doxygen; `//` comments explain **why**, not **what**; all comments in English
- PIMPL for large public classes

---

## Testing

- **Unit tests**: `tests/unit/` — isolated component tests using mocks from `tests/mock/` (`MockNetworkTransport`, `MockTimerService`, `MockPersister`, `MockStateMachine`)
- **Integration tests**: `tests/integration/` — multi-node cluster scenarios (3-node cluster, metrics endpoint, multi-raft 2-group)
- Naming: `TEST_F(<Class>Test, <Scenario>_<ExpectedBehavior>)`, e.g. `Propose_AsFollower_ReturnsNotLeader`
- All new features need unit tests; complex scenarios need integration tests
- All tests must pass before submitting changes

---

## CI

GitHub Actions (`.github/workflows/ci.yml`) on push/PR: GCC+Clang × Ubuntu+macOS × Release/Debug, ASan/TSan/UBSan, `-Werror`, format-check, and a Docker suite (`./scripts/docker-test.sh full`). All jobs must pass before merge.

**Known CI issue**: caching the full `build/` directory may skip `gtest_discover_tests` on cache hits, making new tests invisible to `ctest`. Cache compiler artifacts (ccache), not generated test files.

---

## Commit Messages

[Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <subject>` — types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`. Imperative mood, no capitalization, no trailing period, subject ≤ 50 chars, body wrapped at 72. Recent scopes include `multi-raft`.

---

## Where to Look

- `README.md` — user-facing docs, API reference, usage examples
- `CONTRIBUTING.md` — PR workflow and checklist
- `CLAUDE.md` — Claude Code quick reference (commands + current architecture)
- `doc/todo.md` — roadmap and production-readiness gaps
- `doc/design_fine_grained_locks.md` — lock hierarchy design
- `docs/design-multi-raft-spike.md`, `docs/multi-raft-spike.md` — multi-raft design
- `docs/design-group-commit.md`, `docs/design-wal-checkpoint.md`, `docs/wal-separation-design.md` — persistence design
- `docs/operations-guide.md`, `docs/public-api-guide.md` — operational and API guidance
