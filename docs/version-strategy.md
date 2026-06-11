# Version Strategy Note

## Current State

- **Git tag `v0.2.0`**: Exists (2026-05-20, commit `72f52eb`). Tag message: "RollingRaft v0.2.0 — Control Plane & Deterministic Tests".
- **CMakeLists.txt**: Still declares `VERSION 0.1.0`.
- **No CHANGELOG.md**: Prior to this document, no structured changelog existed.
- **HEAD**: 33 commits ahead of `v0.2.0`.

## Recommendation

Release current HEAD as **v0.3.0** rather than re-tagging v0.2.0.

### Rationale

1. **Tag immutability**: `v0.2.0` already points to a specific commit. Re-tagging is an anti-pattern that breaks downstream references.
2. **Semantic Versioning**: The 33 commits since v0.2.0 include major new features (Pre-vote, CheckQuorum, Joint Consensus, TLS, Streaming Snapshot, Async Apply, Pipeline, WAL Separation) that warrant a minor bump under SemVer.
3. **Clear history**: v0.2.0 = "Control Plane & Deterministic Tests"; v0.3.0 = "Production Safety & Storage Architecture". This tells a clear product story.

### Proposed Version Timeline

| Version | Date | Theme | Commit |
|---------|------|-------|--------|
| v0.1.0 | 2025 | Core Raft | (initial) |
| v0.2.0 | 2026-05-20 | Control Plane & Deterministic Tests | `72f52eb` |
| **v0.3.0** | **TBD** | **Production Safety & Storage Architecture** | **current HEAD** |
| v0.4.0 | TBD | Multi-raft | (pending decision) |

### Actions Required

1. Update `CMakeLists.txt`: `VERSION 0.1.0` → `VERSION 0.3.0`
2. Update `CHANGELOG.md`: Add release date to `[Unreleased]` → `[0.3.0]`
3. Tag `v0.3.0` on release commit
4. (Optional but recommended) Create retroactive GitHub Release page for v0.2.0
