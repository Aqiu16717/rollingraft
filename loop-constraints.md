# Loop Constraints

> Add rules below with `/constraints <rule>` in your agent.
> The `loop-constraints` skill reads this file at the start of every run.
> Constraints here are **binding** — the agent MUST follow them.

## Push & Merge
- Don't push before telling me
- Never auto-merge to main without human approval
- Always create a draft PR first; let me review before marking ready
- Conventional Commits, imperative subject ≤ 50 chars; no AI attribution trailers

## Paths
- Never edit anything under `third_party/` (submodules; LevelDB has local patches in `cmake/patches/`)
- Never edit `cmake/patches/`, CI workflows, or Docker scripts without human approval
- Never edit .env, .env.*, secrets, credentials, or any file containing tokens/keys
- Never modify `tsan_suppressions.txt` to silence a race — a suppression change is a human decision

## Code
- Always run `make unit-test` before proposing a fix; `make test` before calling anything ready
- Never disable tests to make CI green
- Respect the lock hierarchy documented in `src/raft_group.h`:
  `election_mtx_ → replication_mtx_ → snapshot_mtx_ → membership_mtx_ → applier_mtx_` —
  never introduce a lock acquisition that violates this order
- No per-group timers in multi-raft — the single shared tick drives all groups (`OnStoreTick()`)
- Braces on all `if` bodies; build must stay warning-free (`-Wall -Wextra -Wpedantic`)
- Never refactor unrelated code — one fix per run
- Max 3 fix attempts per item; escalate after
- Enforce the attempt limit mechanically: log each try to `loop-ledger.json` and run `loop-context --check` before retrying (see the `loop-guard` skill)

## Communication
- Always tell me what you're about to do before doing it
- Never close an issue or PR without my approval

## Budget
- If token spend hits 80% of daily cap, switch to report-only
- If loop-pause-all is active, exit immediately

---
<!-- Add your own rules below. Use plain English. The loop reads this verbatim. -->
