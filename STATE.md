# Loop State — rollingraft

Last run: 2026-08-17 (interactive: docker-test segfault fix)

## Project context (static, keep updated)

- C++20 Raft consensus library; Makefile is the build interface (`make release`, `make test`).
- Verification ladder (cheap → expensive, use the cheapest that covers the change):
  1. `make format-check` — clang-format, Google style
  2. `make unit-test` — ctest -R "unit\." (GoogleTest, mocks in tests/mock/)
  3. `make test` — full release build + all ctest (unit + integration)
  4. `make test-tsan` / asan / ubsan — sanitizers, slow, only for concurrency/unsafe changes
- Code must build warning-free (`-Wall -Wextra -Wpedantic`; CI has a `-Werror` job).
- CI matrix: GCC+Clang × Linux+macOS × Release/Debug + ASan/TSan/UBSan + format-check + Docker suite.
- Active development: multi-raft (`SharedNodeInfra` / `RaftGroup` / `RaftStore`); review modules #1–#12 complete (as of 2026-08-07).
- Active workstream (2026-08-09→10): **lock-I/O refactor** — snapshot two-phase creation/receive (`bed8ccb`), prepare peer snapshots outside manager locks (`890de54`), phase 1b logged 08-10. Touches the documented lock hierarchy — high-value but risky area.
- Conventions: Conventional Commits (≤50 char imperative subject), no AI attribution trailers, braces on all `if` bodies.

## High Priority (loop is acting or waiting on human)

- **docker-test segfault on main — root-caused and fixed locally, awaiting push approval (08-17)**.
  `Cluster3NodesTest.FollowerCatchesUpAfterRestart` (added 08-16 in `4cc2ae6`) segfaulted in docker-test (exit 139) — the only CI job that runs the integration suite. Root cause is a **test bug, not a product bug**: `GetLeader()` in `test_cluster_3nodes.cpp` called `IsLeader()` on a reset (null) `nodes_` slot. Fixed with a null-slot guard (matches existing idiom). Verified: 10/10 bare runs + full `make test` (370/370) green locally. CI on main stays red until the fix commit is pushed.
  Awaiting: human approval to push.

- **Intermittent TSan race — fix proposal delivered, awaiting human go/no-go (08-15)**.
  Root cause identified: NOT the `pending_callbacks_` map (that's mutex-protected) — it's the `asio::steady_timer` object itself. `async_wait`/`expires_after` initiated on Send's caller thread vs `cancel()` on io thread / Close's thread, without strand serialization (ASIO "shared objects: unsafe"). Sites: `asio_network_transport.cpp:175,184,494,116,140`.
  Proposed fix (~20 lines): post all timer initiations through the strand + cancel helper. TSan suppression is forbidden by constraints.
  Awaiting: human decision to implement on a branch/worktree.
  Loop-pause-all: not active.

## Watch List

- ~~Intermittent TSan race~~ → in High Priority (escalated 08-13).
- `MetricsEndpointTest.TriggerSnapshotOnLeader` flake (port conflict, 2026-08-02) — 14 days no recurrence; test-stability fixes landed 08-09. Presumed fixed — drop if CI stays green through end of August.
- `third_party/leveldb` submodule still has uncommitted local modifications — never touch it (see loop-constraints.md). Docker builds vanilla LevelDB (patch fails to apply: `CMakeLists.txt:264`, `env_posix.cc:837`) — pre-existing on green runs too (checked 08-17), so not the segfault cause, but a human decision is eventually needed.
- Issue #18 (open, enhancement): multi-raft 3-node example + README usage guide — unassigned, no recent activity.
- Multi-raft under active development; expect churn in `src/raft_store.*`, `src/raft_group.*`, `src/shared_node_infra.*`.

## Recent Noise (ignored this run)

- 08-17: docker-test segfault resolved (see High Priority) — root cause was the test's `GetLeader()`, not library code; library catch-up path stays green. LevelDB patch drift ruled out as cause.
- Docs session logs (lock-I/O, correctness cleanup, flaky-test session) — expected cadence of an active workstream, no action.
- `6f67085 fix(wal): recover from corrupt records by truncating the segment` — pushed, CI green.
- Last 8 CI runs green (08-09 → 08-10), no failures to chase.
- 08-13 CI red (format-check gate) resolved by 08-14 — SSE commits passed full matrix.
- New workstream 08-15: `perf(wal): cache the segment read fd` (`3276997`) — CI green. Loop state commit `e8778f6` pushed and verified green.

---
Run log: loop-run-log.md
