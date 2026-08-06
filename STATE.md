# Loop State — rollingraft

Last run: 2026-08-06 (manual, report-only)

## Project context (static, keep updated)

- C++20 Raft consensus library; Makefile is the build interface (`make release`, `make test`).
- Verification ladder (cheap → expensive, use the cheapest that covers the change):
  1. `make format-check` — clang-format, Google style
  2. `make unit-test` — ctest -R "unit\." (GoogleTest, mocks in tests/mock/)
  3. `make test` — full release build + all ctest (unit + integration)
  4. `make test-tsan` / asan / ubsan — sanitizers, slow, only for concurrency/unsafe changes
- Code must build warning-free (`-Wall -Wextra -Wpedantic`; CI has a `-Werror` job).
- CI matrix: GCC+Clang × Linux+macOS × Release/Debug + ASan/TSan/UBSan + format-check + Docker suite.
- Active development: multi-raft (`SharedNodeInfra` / `RaftGroup` / `RaftStore`); review modules #1–#7, #8a done; #8b split out and pending (as of 2026-08-06).
- Conventions: Conventional Commits (≤50 char imperative subject), no AI attribution trailers, braces on all `if` bodies.

## High Priority (loop is acting or waiting on human)

- **Local main is 2 commits ahead of origin, unpushed** — includes `5498335 fix(persister): persist follower conflict truncation`, a correctness fix with no CI coverage yet.
  Why: persistence correctness fix sitting unverified by the CI matrix (incl. sanitizers + Docker suite).
  Next action: human pushes (constraint: loop never pushes). Effort: 1 min.
  Loop-pause-all: not active.

## Watch List

- `MetricsEndpointTest.TriggerSnapshotOnLeader` failed in docker-test on 2026-08-02 ("Address already in use" starting node 3) — next run passed, looks like port-conflict flake. Re-triage if it recurs; consider port randomization if it becomes frequent.
- Same failed run showed leveldb patch "does not apply" errors in docker-test; subsequent run green, so likely tied to the dirty submodule state below. Watch only.
- `third_party/leveldb` submodule has uncommitted local modifications — never touch it (see loop-constraints.md); flag if build/CI fallout correlates.
- Issue #18 (open, enhancement): multi-raft 3-node example + README usage guide — unassigned, no recent activity.
- Review module #8b pending (split out from #8a on 2026-08-06).
- Multi-raft under active development; expect churn in `src/raft_store.*`, `src/raft_group.*`, `src/shared_node_infra.*`.

## Recent Noise (ignored this run)

- Docs-only review-module tracking commits (#1–#8a) — expected cadence, no action.
- Loop-engineering scaffold files (STATE.md, LOOP.md, etc.) untracked pending first commit — known, handled by human this session.

---
Run log: loop-run-log.md
