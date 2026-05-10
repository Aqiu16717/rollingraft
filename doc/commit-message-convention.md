# RollingRaft Commit Message Convention

## 1. Philosophy: Why > What

A good commit message explains **why** the change is necessary, not merely **what** changed.
The code itself tells *what* changed. The commit message must tell *why* it changed,
*what problem* it solves, and *what happens if we do not apply it*.

This convention is inspired by the Linux kernel commit message style and
Chris Beams' "How to Write a Git Commit Message".

---

## 2. Format

```
type: Imperative summary under 50 chars

* Problem: what bug, limitation, or design debt triggered this change
* Impact: who is affected and how severely
* Solution: high-level approach (not a line-by-line code walkthrough)
* Side effects: any behavioural change, API change, or performance impact
* Testing: how the change was verified (test counts, sanitizer results, etc.)

Fixes: #<issue_number> (if applicable)
Refs: #<issue_number> (if applicable)
```

---

## 3. Subject Line (First Line)

### Rules

* **Hard limit**: 50 characters (soft limit: 72 if absolutely necessary)
* **Imperative mood**: write as if you are commanding the code to do something
  * Good: `fix: repair deadlock when Stop() holds mutex during callback`
  * Bad: `fixed deadlock` or `fixing deadlock` or `deadlock fix`
* **Type prefix**: one of the categories below, followed by a colon and a space
* **No period** at the end
* **Capitalise the first word after the prefix**

### Type Prefixes

| Type | Use when | Example |
|------|----------|---------|
| `feat` | New feature or capability | `feat: add BatchPropose API for throughput` |
| `fix` | Bug fix | `fix: ignore SaveState return value in election` |
| `refactor` | Code restructuring without behaviour change | `refactor: split RaftNodeImpl into 5 managers` |
| `perf` | Performance improvement | `perf: replace global mutex with per-manager locks` |
| `docs` | Documentation only | `docs: add commit-message-convention.md` |
| `build` | Build system or toolchain | `build: add ASan/TSan/UBSan CI matrix` |
| `ci` | CI/CD configuration | `ci: fix macOS test timeout in GitHub Actions` |
| `style` | Formatting, no logic change | `style: apply clang-format to new manager files` |
| `test` | Test-only change | `test: add chaos test for network partition` |
| `chore` | Miscellaneous maintenance | `chore: bump spdlog to v1.12.0` |

---

## 4. Body

### 4.1 Problem (Why)

Describe the problem that existed *before* this commit.

* What was the symptom?
* Who was affected?
* What was the root cause?

### 4.2 Impact

Quantify or qualify the severity.

* Data loss risk? Crash? Performance regression?
* Which configurations or platforms are affected?

### 4.3 Solution (How)

Explain the high-level approach.

* Do **not** walk through every changed line.
* Do explain *why* this approach was chosen over alternatives.
* Mention any trade-offs.

### 4.4 Side Effects / Behavioural Changes

* API changes (breaking or non-breaking)
* New configuration options
* Performance deltas (improvement or regression)
* Anything that operators or users need to know

### 4.5 Testing

* Unit test results: `167/167 passing`
* Integration test results: `9/9 passing`
* Sanitizer results: `ASan clean`, `TSan clean`, `UBSan clean`
* Platform coverage: `verified on macOS (Apple Silicon) + Ubuntu 22.04 (GCC/Clang)`
* Any manual verification steps

---

## 5. Examples

### Example 1: Bug Fix

```
fix: repair deadlock when Stop() holds mutex during callback

* Problem: Stop() iterates pending_proposals_ while holding mtx_,
  then directly invokes user-provided callbacks. If the callback
  re-enters RaftNode (e.g., calls IsLeader()), mtx_ is already
  held -> immediate deadlock.

* Impact: Any client that queries RaftNode state inside a proposal
  callback will deadlock the node on shutdown. 100%% reproducible
  with the attached regression test.

* Solution: Collect callbacks into a local vector, release mtx_,
  then invoke callbacks outside the critical section. Use a
  LockReacquireGuard to ensure mtx_ is re-locked even if a
  callback throws.

* Side effects: None. Public API unchanged.

* Testing: 167/167 unit tests pass. 9/9 integration tests pass.
  TSan clean. New regression test added in test_deadlock.cpp.

Fixes: #4
```

### Example 2: Modular Refactoring

```
refactor: split RaftNodeImpl into 5 functional managers

* Problem: raft_node.cpp had grown to 2400+ lines, mixing election
  logic, log replication, snapshot transfer, membership changes,
  and state machine application in a single "god class". This
  made code review, unit testing, and safe evolution impossible.

* Impact: Adding new features (e.g., Batch Propose, Leader Lease)
  required touching the same monolithic file, increasing regression
  risk. Unit tests could only test the full RaftNode, not isolated
  subsystems.

* Solution: Decompose RaftNodeImpl into:
  * ElectionManager   -- term, voting, role transitions
  * LogReplicator     -- AppendEntries, commit tracking, ReadIndex
  * SnapshotManager   -- auto-trigger, InstallSnapshot RPC
  * MembershipManager -- cluster config, add/remove nodes
  * StateMachineApplier -- Apply loop, proposal/read callbacks
  A thin RaftNodeCore coordinator holds the global mutex and
  delegates to managers via well-defined callbacks.

* Side effects: Public API (RaftNode) is unchanged. Internal
  manager headers are in src/internal/ and are not public.

* Testing: Zero logic change -- this is a pure code move.
  Verified with git diff --stat that method bodies are identical.
  177/177 unit tests pass. 9/9 integration tests pass.

Refs: #3
```

### Example 3: CI / Build Improvement

```
ci: add ASan/TSan/UBSan matrix and compiler coverage

* Problem: CI only ran on ubuntu-22.04 + GCC + Release. Concurrent
  bugs, undefined behaviour, and Clang-specific warnings were only
  caught late in the development cycle or by users on macOS.

* Impact: Silent data races and memory leaks could reach main
  branch undetected. macOS developers had no CI signal.

* Solution: Expand GitHub Actions workflow to a full matrix:
  * Compilers: GCC 11/12, Clang 14/15
  * Platforms: ubuntu-22.04, macos-latest
  * Sanitizers: ASan, TSan, UBSan (each as a separate job)
  * Build types: Release, Debug, Werror
  Add scripts/wait-for-cluster.sh to replace flaky docker-compose
  depends_on behaviour.

* Side effects: CI runtime increases from ~8 min to ~25 min.
  ASan/TSan jobs are allowed to fail for now until existing
  issues are fixed.

* Testing: Verified on fork. All green jobs pass. 177/177 tests
  under ASan. TSan reports 3 pre-existing races (filed as #8).

Refs: #2
```

### Example 4: Bad Example (Do Not Use)

```
fixed some bugs and changed code

- fixed the thing Tom found
- updated raft_node.cpp
- tests pass
```

**Why it is bad**:

* Subject is vague ("some bugs", "changed code").
* No explanation of *why* the bugs existed or *why* the fix works.
* No mention of impact or testing details.
* Uses `-` for bullets instead of `*`.

---

## 6. Quick Reference Checklist

Before pushing, run through this checklist:

* [ ] Subject is under 50 characters
* [ ] Subject uses imperative mood (`Add`, `Fix`, `Refactor`, not `Added`/`Fixed`)
* [ ] Subject has a type prefix followed by a colon and space
* [ ] Subject has no trailing period
* [ ] Blank line separates subject from body
* [ ] Body explains **why** the change is necessary
* [ ] Body includes **testing** evidence (test counts, sanitizer results)
* [ ] Body uses `*` for bullet points, not `-`
* [ ] If fixing an issue, include `Fixes: #<number>` on its own line
* [ ] The commit message is in English

---

## 7. References

* Linux kernel commit message style:
  <https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/Documentation/process/submitting-patches.rst>
* Chris Beams: "How to Write a Git Commit Message"
  <https://cbea.ms/git-commit/>
* Conventional Commits (type prefixes):
  <https://www.conventionalcommits.org/>
