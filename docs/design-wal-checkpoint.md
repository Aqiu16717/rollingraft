# WAL Index Checkpoint Design

## Problem

`WALPersister::Open()` currently rebuilds the in-memory `DenseIndex` by scanning
**every** segment file and parsing every log-entry payload to extract the log
index. For a 100k-entry WAL this takes ~1.28s on macOS, dominated by:

1. Sequential reads of all segment files.
2. Per-record protobuf/JSON deserialization solely to discover the log index.
3. `DenseIndex::Put()` for every record.

This cost is paid on every process restart, which hurts availability during
rolling upgrades or crash recovery.

## Goal

Reduce `Open()` latency for large WALs from O(N) to O(delta), where *delta* is
the number of records written after the most recent checkpoint.

Target: reopen 100k entries in **< 200 ms**.

## Checkpoint File Format

File name: `checkpoint.<last_covered_segment_id>.idx`

Binary layout (little-endian):

```
[Header 40 bytes]
  magic                 uint32   0x57494458 ("WIDX")
  version               uint16   1
  reserved              uint16   0
  first_index           uint64   first log index in checkpoint
  last_index            uint64   last log index in checkpoint
  entry_count           uint64   number of index entries
  last_covered_segment  uint64   newest segment fully indexed

[Body: entry_count * WALIndexEntry (24 bytes each)]
  segment_id            uint64
  file_offset           uint64
  length                uint64

[Footer: CRC32 (4 bytes) of header + body]
```

The format is intentionally simple and dense so that loading is a single bulk
`read()` followed by direct `DenseIndex::Put()` calls without any payload
parsing.

## Recovery Flow

1. Scan the WAL directory for segment files and checkpoint files.
2. Select the newest checkpoint whose `last_covered_segment_id` has a
corresponding segment file on disk.
3. Validate header magic/version, size, and CRC32. On any mismatch, discard the
checkpoint and fall back to full segment scan.
4. Load the body directly into `DenseIndex`.
5. For each segment with `segment_id > last_covered_segment_id`, perform the
normal `ScanSegment()` to replay log entries and truncation records.

Because Raft log indices are sequential and segments are append-only, this
correctly reconstructs the index even when the last checkpoint was taken in the
middle of a segment lifecycle.

## Creation Triggers

Checkpoints are created opportunistically so the durability path is not blocked:

* `Sync()` after a successful segment fsync, when either:
  * `active_segment_.id - last_checkpoint_segment_id >= 5`, or
  * entries written after the last checkpoint >= 50,000.
* `Close()` uses the same `ShouldCreateCheckpointLocked()` predicate, so small
WALs do not get a checkpoint and continue to use the full-scan behavior (which
preserves existing corruption-detection semantics in unit tests).

The thresholds are intentionally conservative: checkpoints are large sequential
files, and writing them too often would waste disk bandwidth.

## Crash Safety

A checkpoint is written atomically:

1. Write to `checkpoint.<id>.idx.tmp`.
2. `fsync`/`F_FULLFSYNC` the temp file.
3. `rename()` to `checkpoint.<id>.idx`.
4. `fsync` the WAL directory.

On restart, invalid or partial checkpoint files are ignored and recovery falls
back to scanning all segments.

## Interaction with Garbage Collection

`GarbageCollect(before_log_index)` deletes segment files whose entries are all
below `before_log_index`. Any checkpoint whose `last_covered_segment_id` is
smaller than the first retained segment is also removed, preventing orphaned
checkpoint files from accumulating.

## Validation

### Tests Added

* `CheckpointCreatedOnCloseAndUsedOnReopen` — 60k entries, checkpoint created,
reopen verifies range and replay count.
* `CorruptedCheckpointFallsBack` — corrupt checkpoint bytes, reopen still
succeeds via full segment scan.
* `GarbageCollectRemovesOldCheckpoints` — periodic `Sync()` creates multiple
checkpoints; GC removes ones covering deleted segments and data remains
readable.

### Benchmark Results

`benchmark_persister_micro` on macOS (Release):

| Scenario | Before | After |
|---|---|---|
| `wal_recovery_100000_128B` | ~161 ms | **~5.9 ms** |
| `wal_recovery_10000_128B` | ~34 ms | ~34 ms (single segment, no checkpoint) |

The 100k-entry recovery is ~270× faster and well under the 200 ms target.

## Future Work

* Periodically rewrite a single named checkpoint file instead of one per
segment, reducing directory scanning on Open().
* Add a metric for checkpoint creation/fallback events.
* Consider mmap-ing the checkpoint body for very large indexes.
