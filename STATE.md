# Loop State — rollingraft

Last run: 2026-08-15 (scheduled fire)

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

- **Intermittent TSan race unresolved** — last movement 08-10 (`a02fcba`, lock-I/O refactor phase 1b). Still open as of 08-15; human active on store/SSE workstreams, race area untouched for 5 days.
  Loop-pause-all: not active.

## Watch List

- ~~Intermittent TSan race~~ → in High Priority (escalated 08-13).
- `MetricsEndpointTest.TriggerSnapshotOnLeader` flake (port conflict, 2026-08-02) — 13 days no recurrence; test-stability fixes landed 08-09. Presumed fixed — drop if CI stays green through end of August.
- `third_party/leveldb` submodule still has uncommitted local modifications — never touch it (see loop-constraints.md); flag if build/CI fallout correlates.
- Issue #18 (open, enhancement): multi-raft 3-node example + README usage guide — unassigned, no recent activity.
- Multi-raft under active development; expect churn in `src/raft_store.*`, `src/raft_group.*`, `src/shared_node_infra.*`.

## Recent Noise (ignored this run)

- Docs session logs (lock-I/O, correctness cleanup, flaky-test session) — expected cadence of an active workstream, no action.
- `6f67085 fix(wal): recover from corrupt records by truncating the segment` — pushed, CI green.
- Last 8 CI runs green (08-09 → 08-10), no failures to chase.
- 08-13 CI red (format-check gate) resolved by 08-14 — SSE commits (`a394c77` shared_ptr keepalive, `d666312` broadcast group events) passed full matrix; new `feat(store)` SSE workstream confirmed green.

---
Run log: loop-run-log.md
