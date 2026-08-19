# Loop State — rollingraft

Last run: 2026-08-19 (interactive: delta review #13/#14 fixes — snapshot safety, rewind liveness, store admin)

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

- ~~Delta review #13/#14 fixes~~ → pushed 08-19, **CI fully green** (run 32246368423, all 12 jobs — one prior run hung on runner apt install and was retriggered). Closed out.

- ~~TSan RPC-timer race~~ → fixed (`ca8c39a`), pushed 08-17, **Linux CI TSan green** (run 32041263920, all 12 jobs). Closed out.
- ~~docker-test segfault on main~~ → pushed 08-17, CI green (run 32039682301). Closed out.

## Watch List

- ~~Intermittent TSan race~~ → in High Priority (escalated 08-13).
- `MetricsEndpointTest.TriggerSnapshotOnLeader` flake (port conflict, 2026-08-02) — 14 days no recurrence; test-stability fixes landed 08-09. Presumed fixed — drop if CI stays green through end of August.
- `third_party/leveldb` submodule still has uncommitted local modifications — never touch it (see loop-constraints.md). Docker builds vanilla LevelDB (patch fails to apply: `CMakeLists.txt:264`, `env_posix.cc:837`) — pre-existing on green runs too (checked 08-17), so not the segfault cause, but a human decision is eventually needed.
- macOS TSan: raw binary runs (no TSAN_OPTIONS) report kqueue_reactor races — these are KNOWN and already suppressed in `tsan_suppressions.txt` (`race:asio::detail::kqueue_reactor::run` etc.). Always run TSan via `make test-tsan`; do not re-escalate this as new.
- Issue #18: implemented 08-18 on `feature/multi-raft-pr-g-example-readme`, merged to main locally (30a1ccb), branch deleted. Spec/plan in `docs/superpowers/{specs,plans}/`. NOT pushed, NOT closed — awaiting human approval for both.
- Multi-raft under active development; expect churn in `src/raft_store.*`, `src/raft_group.*`, `src/shared_node_infra.*`.
- **Delta review follow-ups (logged 08-19, not fixed)**: store SSE lambda races Stop() (metrics_server_ UAF), `BroadcastEvent` io_ctx_ race, `RaftStore::Start` half-start on metrics bind failure, SSE strong-refs retain vanished clients, `/v1/events` accepts any method, mixed clocks (lease/quorum/contact still steady_clock — deterministic tests freeze those paths), TriggerSnapshot metric gaps, `GetLogStats` O(n) under locks. See `docs/reviews/progress.md` #13/#14.
- **Delta review #15 (WAL perf fd cache + transport/protocol) — not yet reviewed** (pending).

## Recent Noise (ignored this run)

- 08-17: docker-test segfault resolved (see High Priority) — root cause was the test's `GetLeader()`, not library code; library catch-up path stays green. LevelDB patch drift ruled out as cause.
- Docs session logs (lock-I/O, correctness cleanup, flaky-test session) — expected cadence of an active workstream, no action.
- `6f67085 fix(wal): recover from corrupt records by truncating the segment` — pushed, CI green.
- Last 8 CI runs green (08-09 → 08-10), no failures to chase.
- 08-13 CI red (format-check gate) resolved by 08-14 — SSE commits passed full matrix.
- New workstream 08-15: `perf(wal): cache the segment read fd` (`3276997`) — CI green. Loop state commit `e8778f6` pushed and verified green.

---
Run log: loop-run-log.md
