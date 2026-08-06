# Loop Configuration — rollingraft (Claude Code)

## Active Loops

| Pattern | Cadence | Status | Command |
|---------|---------|--------|---------|
| Daily Triage | 1d | L1 report-only | `/loop 1d Run $loop-triage` |

## Human Gates

- **No auto-fix until L2 checklist complete.** Week one: triage writes STATE.md only.
- `src/` and `include/`: loop may never commit directly. Even at L2, changes ship as a
  draft PR on a branch; human reviews and merges. Consensus code (election, log
  replication, membership, lock hierarchy) always requires human review.
- `third_party/` is fully off-limits — LevelDB carries local patches under
  `cmake/patches/`; any submodule bump is a human decision.
- All merges to `main` are human-only, regardless of autonomy level.

## Verification (what "done" means for any proposed change)

- Cheap first: `make format-check`, then `make unit-test`.
- Before proposing anything as ready: `make test` green.
- Concurrency/locking changes additionally need `make test-tsan` before human review.
- Never disable or skip a test to get green.

## Worktrees

- Use `isolation: worktree` when spawning implementer sub-agents (L2+).
- One worktree per fix attempt; discard after verifier REJECT.
- L1 triage never builds in a worktree — it reads and reports only.

## Connectors (MCP)

- MCP optional for L1 report-only loops.
- For L2+: GitHub MCP to read CI/issues; scope connectors to read + comment only until trusted.

## Budget

- Max sub-agent spawns per run: 0 (L1) / 2 (L2)
- Review STATE.md daily; see loop-budget.md for caps and kill switch.

## Links

- Pattern: daily-triage (cobusgreyling/loop-engineering)
- Project guide: CLAUDE.md (commands + architecture), AGENTS.md (canonical long-form)
