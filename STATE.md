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

- **TSan RPC-timer race — fixed and verified locally, awaiting push approval (08-17)**.
  Root cause confirmed in code: `Send()` armed the per-RPC `steady_timer` on the PeerConnection strand thread while `cancel()` ran on the TcpConnection strand / Close threads (ASIO shared-object violation). Fix: all timer ops (expires_after/async_wait/cancel) serialized on the connection strand via `asio::post` + `CancelTimer` helper. Regression test added: `tests/integration/test_transport_timer_race.cpp` (3-node Propose hammer under TSan). Verified: `make test` 371/371, `make test-tsan` 371/371, stress test 8/8 TSan-clean.
  Note: the timer race never reproduced locally on macOS TSan (Linux-only manifestation) — Linux CI TSan is the definitive gate post-push.
  Awaiting: human approval to push.

- ~~docker-test segfault on main~~ → pushed 08-17, CI green (run 32039682301). Closed out.

## Watch List

- ~~Intermittent TSan race~~ → in High Priority (escalated 08-13).
- `MetricsEndpointTest.TriggerSnapshotOnLeader` flake (port conflict, 2026-08-02) — 14 days no recurrence; test-stability fixes landed 08-09. Presumed fixed — drop if CI stays green through end of August.
- `third_party/leveldb` submodule still has uncommitted local modifications — never touch it (see loop-constraints.md). Docker builds vanilla LevelDB (patch fails to apply: `CMakeLists.txt:264`, `env_posix.cc:837`) — pre-existing on green runs too (checked 08-17), so not the segfault cause, but a human decision is eventually needed.
- macOS TSan: raw binary runs (no TSAN_OPTIONS) report kqueue_reactor races — these are KNOWN and already suppressed in `tsan_suppressions.txt` (`race:asio::detail::kqueue_reactor::run` etc.). Always run TSan via `make test-tsan`; do not re-escalate this as new.
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
