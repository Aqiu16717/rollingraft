# RollingRaft v0.3.3 Release Notes

> **Tag**: `v0.3.3` → `7fbf096`  
> **Theme**: Empty Snapshot Cleanup Fix  
> **Scope**: Follow-up bugfix for v0.3.2 atomic snapshot replacement.

---

## What's Fixed

### 🛠️ Empty Snapshot Old-Data Cleanup

The v0.3.2 atomic snapshot replacement refactor introduced a regression: when `StatePersister::SaveSnapshotStream()` received an **empty snapshot** (zero chunks), it did not clean up the previous snapshot data. This left stale snapshot chunks on disk, inconsistent with the pre-v0.3.2 behavior.

**Fix** (`7fbf096`):
- Empty snapshots now atomically delete the old snapshot data via a LevelDB WriteBatch
- Behavior is now consistent with the pre-v0.3.2 implementation
- Added a unit test covering the empty-snapshot cleanup path

---

## Upgrade Notes

- No breaking changes
- No configuration changes
- All v0.3.2 users are recommended to upgrade to v0.3.3 if their workload may install empty snapshots

---

## Test Coverage

- New unit test: empty snapshot correctly clears old snapshot data
- Full regression suite: **332/332 tests pass**

---

*For detailed changelog, see [CHANGELOG.md](../CHANGELOG.md).*
