# WAL Protobuf Migration (v0.3.1)

## Summary

Starting with v0.3.1, `WALPersister` serializes Raft log entry payloads using Protocol Buffers instead of JSON + Base64. The existing segment framing (magic, version, CRC32, length, type, payload, trailer) is unchanged; only the **payload encoding** of `kLogEntry` records changed. Old JSON+Base64 segments remain readable via an automatic fallback parser, so existing WAL directories can be reopened without conversion.

## Motivation

The protobuf MVP benchmark (`docs/protobuf-mvp-benchmark.md`) showed that protobuf is dramatically faster and smaller than JSON+Base64 for log entries:

| Payload | Serialize speedup | Deserialize speedup | Size reduction |
|---------|------------------:|--------------------:|---------------:|
| 100 B   | 22×               | 55×                 | 1.64×          |
| 1 KB    | 91×               | 140×                | 1.36×          |
| 10 KB   | 98×               | 120×                | 1.34×          |

Applying protobuf to the WAL yields measurable end-to-end gains (see Benchmarks below).

## Format Changes

### Segment Header

The 16-byte segment header is unchanged except that bytes 6–7 now store a **format version** for the payload encoding:

| Offset | Size | Field            | Notes                                         |
|-------:|-----:|------------------|-----------------------------------------------|
| 0      | 4    | Magic (`0x57414C30`) | "WAL0"                                    |
| 4      | 2    | Version          | `1`                                           |
| 6      | 2    | Format version   | `1` = JSON+Base64, `2` = Protobuf (new default) |
| 8      | 8    | Segment ID       | Monotonically increasing                      |

Legacy segments that did not write a format version contain `0` in this field; the code treats `0` as JSON for backward compatibility.

### Log Entry Payload

New segments (format version `2`) store each log entry as a serialized `RaftLogEntryProto`:

```protobuf
syntax = "proto3";
package rollingraft;

message RaftLogEntryProto {
  uint64 index    = 1;
  uint64 term     = 2;
  bytes  data     = 3;
  bytes  command  = 4;
  uint64 checksum = 5;
}
```

`data` and `command` are written as raw `bytes` (no Base64). `checksum` is preserved as stored by the caller.

`kTruncatePrefix` and `kTruncateSuffix` records still use JSON payloads because they are small, human-readable metadata records.

## Backward Compatibility

- **Reading**: `ReadLogEntryAt()` looks up the segment's format version. If it is `kFormatVersionProtobuf`, protobuf parsing is attempted first; if that fails, the legacy JSON+Base64 parser is used as a fallback. JSON segments parse directly via the legacy path.
- **Replay/Index Rebuild**: `ScanSegment()` uses the same format-aware parser to extract the log index from each `kLogEntry` record. Mixed-format WALs are supported.
- **Writing**: All newly created segments use format version `2` and protobuf payloads.

No migration tool is required. Opening an existing WAL directory automatically rebuilds the index from the old segments and begins writing new segments in protobuf format after rotation.

## Build Changes

- `CMakeLists.txt` links `rollingraft_proto` **PUBLIC**ly against `rollingraft` so that consumers (tests, benchmarks, examples) receive both the protobuf-generated include directory and transitive abseil/protobuf linkage.
- The protobuf-generated header `raft_log_entry.pb.h` is therefore available to test and benchmark code.

## Code Changes

- `src/wal_persister.cpp`
  - `AppendLogEntry()` serializes via `RaftLogEntryProto::SerializeToString()`.
  - Added `ExtractLogIndexFromPayload()` helper for format-aware index reconstruction.
  - `ReadLogEntryAt()` uses format-aware parsing with JSON fallback.
  - Fixed `first_index_` tracking in `AppendLogEntry()` to use `std::min()` so concurrent out-of-order appends report the correct lower bound.
- `include/rollingraft/wal_persister.h`
  - Added `kFormatVersionJson` / `kFormatVersionProtobuf` constants.
  - Added `segment_format_versions_` map and `format_version` field in `Segment`.
  - `WriteSegmentHeader()` now accepts a `format_version` parameter.
- `tests/unit/test_wal_persister.cpp`
  - Includes `raft_log_entry.pb.h`.
  - `AppendAndReplay` parses replayed payloads as protobuf and verifies index/term/data/command.

## Test Results

All 328 tests pass in the protobuf-enabled build:

```text
100% tests passed, 0 tests failed out of 328
Total Test time (real) = 152.91 sec
```

Relevant WAL/Hybrid test suites:

- `WALPersisterTest.EmptyWAL`
- `WALPersisterTest.AppendAndReplay`
- `WALPersisterTest.CrashRecovery`
- `WALPersisterTest.SegmentRotationByCount`
- `WALPersisterTest.TruncatePrefix`
- `WALPersisterTest.TruncateSuffix`
- `WALPersisterTest.GarbageCollect`
- `WALPersisterTest.CorruptionDetection`
- `WALPersisterTest.ConcurrentAppend`
- `WALPersisterTest.ReopenPreservesData`
- `HybridPersisterTest.*`

## Benchmarks

Benchmark command:

```bash
./benchmark/benchmark_persister \
  --backend={leveldb,hybrid} \
  --entries=50000 \
  --payload=100 \
  --batch=1,10,100 \
  --threads=1 \
  --output=results.csv
```

### Hybrid backend (WAL path)

| Metric                        | v0.3.0 JSON+Base64 | v0.3.1 Protobuf | Change      |
|-------------------------------|-------------------:|----------------:|-------------|
| Append ops/sec (batch=1)      | 28,106             | 35,945          | **+28%**    |
| Append ops/sec (batch=10)     | 2,734              | 3,519           | **+29%**    |
| Append ops/sec (batch=100)    | 285                | 359             | **+26%**    |
| Recovery reopen (100k entries)| 1,269 ms           | 249 ms          | **-80%**    |
| On-disk directory size (100k) | 19.64 MB           | 11.24 MB        | **-43%**    |

### LevelDB backend (included for reference)

| Metric                        | v0.3.0 JSON+Base64 | v0.3.1 Protobuf | Change      |
|-------------------------------|-------------------:|----------------:|-------------|
| Append ops/sec (batch=1)      | 27,871             | 35,868          | **+29%**    |
| Recovery reopen (100k entries)| 1,256 ms           | 255 ms          | **-80%**    |
| On-disk directory size (100k) | 19.64 MB           | 11.24 MB        | **-43%**    |

The gains come from eliminating JSON/Base64 overhead for the per-entry payload; LevelDB itself stores the same serialized payload bytes, so it benefits as well.

## Rollout Considerations

1. **No offline migration needed** — old WAL segments are read transparently.
2. **Disk space** — after rotation and garbage collection, old JSON segments are deleted and overall WAL size shrinks.
3. **Mixed clusters** — this change affects on-disk format only; the network replication protocol is unchanged, so rolling upgrades are not impacted by the WAL format.
4. **Downgrade** — downgrading to v0.3.0 after new protobuf segments exist is safe as long as v0.3.0's reader can still parse the segment header (it ignores the new format-version field and treats payload as JSON, which will fail for protobuf segments). For this reason, a downgrade should be accompanied by truncating/rotating the WAL or restoring from snapshot.

## Files Changed

- `CMakeLists.txt`
- `include/rollingraft/wal_persister.h`
- `src/wal_persister.cpp`
- `tests/unit/test_wal_persister.cpp`
- `docs/wal-protobuf-migration.md` (this document)
