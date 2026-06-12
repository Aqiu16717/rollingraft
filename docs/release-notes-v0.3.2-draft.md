# RollingRaft v0.3.2 Release Notes (v0.3.2)

> **Tag**: `v0.3.2` → `923bb0f`  
> **Theme**: Snapshot Durability & Atomic Replacement  
> **Scope**: Single bugfix release closing SR3 from code quality audit.

---

## What's New

### 🛡️ Atomic Snapshot Replacement (SR3)

Fixed a durability bug in `StatePersister::SaveSnapshotStream()` where the existing snapshot was deleted **before** the new snapshot was fully written and verified. If the node crashed or the write failed during this window, the node would be left with **no valid snapshot**, requiring a full snapshot retransfer on restart.

**New behavior**:
1. New snapshot chunks are written to a temporary file (`snapshot.tmp`)
2. SHA-256 checksum is verified against the incremental digest
3. Only after successful verification is the temporary file atomically `rename()`d to the final snapshot path
4. If any step fails, the old snapshot remains intact and usable

**Impact**:
- Eliminates snapshot corruption window during leader snapshot installation
- Reduces recovery time after partial snapshot failures
- No manual migration required; existing snapshot files remain compatible

---

## Upgrade Notes

- No breaking changes
- No configuration changes
- Existing snapshots are fully compatible
- Recommended for all production deployments using streaming snapshots

---

## Test Coverage

- 3 new unit tests covering:
  - Interrupted write (temp file left behind, old snapshot preserved)
  - Checksum mismatch (temp file discarded, old snapshot preserved)
  - Successful replacement (atomic rename, new snapshot active)
- Full regression suite: 328/328 tests pass

---

*For detailed changelog, see [CHANGELOG.md](../CHANGELOG.md).*
