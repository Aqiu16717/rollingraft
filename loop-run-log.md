# Loop Run Log — rollingraft

Append one entry per run. Prune entries older than 30 days.

## Format

```json
{
  "run_id": "2026-06-09T08:15:00Z",
  "pattern": "daily-triage",
  "duration_s": 45,
  "items_found": 4,
  "actions_taken": 1,
  "escalations": 0,
  "tokens_estimate": 52000,
  "outcome": "report-only | fix-proposed | escalated | no-op"
}
```

## Recent Runs

<!-- Loop appends below this line -->

```json
{
  "run_id": "2026-08-06T06:00:00Z",
  "pattern": "daily-triage",
  "duration_s": 120,
  "items_found": 6,
  "actions_taken": 1,
  "escalations": 1,
  "tokens_estimate": 30000,
  "outcome": "report-only"
}
```

Notes: first run, manual. Escalation = 2 unpushed commits (incl. persister fix) need human push. No code edited.