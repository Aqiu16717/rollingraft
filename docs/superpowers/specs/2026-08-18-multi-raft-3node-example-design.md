# Multi-Raft 3-Node Example + README Guide — Design

- Date: 2026-08-18
- Issue: [#18](https://github.com/Aqiu16717/rollingraft/issues/18)
- Status: approved (user review pending on this document)

## Goal

Provide a runnable single-binary demo of multi-raft and document it in the
README, so users can start 3 nodes hosting 2 Raft groups each with one
command and see the whole cluster lifecycle: election → proposals → isolated
per-group state → graceful shutdown.

## Scope

1. `example/multi_raft/multi_raft_3node.cpp` — self-contained demo binary.
   (Issue text says `examples/`; the repo convention is `example/` singular —
   this spec follows the repo.)
2. README "Multi-Raft" section — architecture description, quick start,
   build/run instructions.
3. CMake target registration in `example/CMakeLists.txt` (build + install).

Out of scope: CLI interaction, persistence-restart demo, membership changes,
TLS, metrics. The existing 3-process `multi_raft_server.cpp` /
`multi_raft_client.cpp` examples stay as-is and remain the multi-host
deployment reference.

## Component design

### The demo binary

**Location:** `example/multi_raft/multi_raft_3node.cpp` (~300 lines),
self-contained — no shared headers with the other examples.

**State machine:** a minimal `CounterMachine` in an anonymous namespace,
implementing the `StateMachine` interface: `Apply` (parses `inc`/`dec`,
updates an `int64_t`, notifies waiters), `Query`, `GetLastAppliedIndex`,
`CreateSnapshot`/`Restore` (functional stubs for a demo; snapshot triggering
is off), `WaitIndex`. The counter model matches existing example conventions
(`multi_raft_server.cpp`, benchmarks).

**Cluster setup:**
- 3 `RaftStore` instances in one process; node IDs 1–3.
- Listen addresses `127.0.0.1:9101`–`9103`. Chosen to avoid the
  docker-compose cluster range (8001–8003) that the README references.
- Temp data dirs under `/tmp/rollingraft_3node_demo_<pid>/` (per node), path
  printed at startup, removed on exit.
- 2 groups per store: group IDs 1 and 2, each with its own `CounterMachine`.
  Election/heartbeat timeouts from `RaftGroupOptions` (300 ms / 50 ms —
  same as the integration test).

**Lifecycle sequence:**
1. Build all three stores; `Initialize()` each — check every `Status`.
2. `CreateGroup(1, ...)` and `CreateGroup(2, ...)` on every store.
3. `Start()` each store.
4. Wait up to 10 s for one leader **per group** (poll `GetGroup(gid)->IsLeader()`
   across the three stores).
5. Propose 5 `inc` commands per group through that group's leader, waiting on
   each apply callback with a 5 s deadline.
6. Print a table: for each node × group, the counter value. All six cells
   must read 5 — demonstrating per-group state isolation over shared
   infrastructure.
7. `Stop()` every store, remove temp dirs, exit 0.

**Error handling:** any failed `Status` or leader-wait timeout prints a
diagnostic and exits non-zero, stopping stores and cleaning up dirs on the
failure path. No signal handler — the demo runs to completion on its own;
graceful shutdown is the `Stop()` sequence.

### Build integration

`example/CMakeLists.txt`: add `example_multi_raft_3node` executable linked
against `rollingraft`, with `src/` on the include path (for `raft_store.h`),
and add it to the `install(...)` list. Examples are built by default
(`make release`), so CI compile coverage comes for free — no CI workflow
edits (denylisted anyway).

### README section

New top-level section "Multi-Raft (Multiple Groups per Node)", placed after
the existing single-group usage docs:

- **Architecture paragraph** (~10 lines): `SharedNodeInfra` = per-node
  network transport, timer service, protocol, metrics; `RaftGroup` = all
  per-group state (term, log, leader, membership); `RaftStore` = infra +
  group table, routes inbound RPCs by `group_id` (`group_id == 0` keeps the
  legacy single-group path).
- **ASCII diagram**: one node box containing N group boxes over shared
  infra, replicated across 3 nodes.
- **Quick start**: `make release`, then run
  `./build/release/example/example_multi_raft_3node`, with a short sample
  output snippet (leaders elected per group, counter table).
- **Multi-host pointer**: the existing
  `example/multi_raft/multi_raft_server.cpp` + client for running nodes in
  separate processes/hosts.
- **Test pointer**: `tests/integration/test_multi_raft_2groups.cpp`.

## Verification

- `make format-check` passes.
- Build warning-free (`-Wall -Wextra -Wpedantic`).
- Run the binary end-to-end: both groups elect leaders, all six counters
  converge to 5, exit 0, temp dirs removed.
- `make test` — full suite still green (example is compile-only in CI).
- CI: the example compiles in the default build matrix.

## Acceptance criteria mapping (issue #18)

| Criterion | Covered by |
|---|---|
| `multi_raft_3node.cpp` compiles and runs locally | demo binary + verification |
| README has a multi-raft section | README section |
| Format check passes | verification |
| CI build passes | default-build target registration |
