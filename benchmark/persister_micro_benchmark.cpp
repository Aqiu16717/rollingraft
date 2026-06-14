/**
 * @file persister_micro_benchmark.cpp
 * @brief Persister-layer micro-benchmarks to isolate serialization, fsync,
 *        LevelDB batching, and snapshot-chunking costs.
 *
 * Output: human-readable Markdown table to stdout, plus optional CSV.
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "raft_log_entry.pb.h"
#include "rollingraft/hybrid_persister.h"
#include "rollingraft/state_persister.h"
#include "rollingraft/wal_persister.h"

using namespace rollingraft;

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

static std::string RandomString(size_t len, std::mt19937& rng) {
  static const char kChars[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::uniform_int_distribution<size_t> dist(0, sizeof(kChars) - 2);
  std::string s;
  s.reserve(len);
  for (size_t i = 0; i < len; ++i) s.push_back(kChars[dist(rng)]);
  return s;
}

static void RemoveDir(const std::string& path) {
  std::filesystem::remove_all(path);
}

struct BenchResult {
  std::string scenario;
  int iterations = 0;
  double total_ms = 0.0;
  double avg_us = 0.0;
  double p50_us = 0.0;
  double p99_us = 0.0;
  double throughput = 0.0;  // MB/s or ops/s depending on scenario
  std::string unit = "ops/s";
};

static BenchResult Summarize(const std::string& scenario,
                             const std::vector<double>& latencies_us,
                             size_t bytes_processed, const std::string& unit) {
  BenchResult r;
  r.scenario = scenario;
  r.iterations = static_cast<int>(latencies_us.size());
  if (r.iterations == 0) return r;
  std::vector<double> sorted = latencies_us;
  std::sort(sorted.begin(), sorted.end());
  double sum = 0.0;
  for (double v : sorted) sum += v;
  r.avg_us = sum / sorted.size();
  r.p50_us = sorted[sorted.size() * 50 / 100];
  r.p99_us = sorted[std::min(sorted.size() * 99 / 100, sorted.size() - 1)];
  r.total_ms = sum / 1000.0;
  if (unit == "MB/s" && r.total_ms > 0) {
    r.throughput = (static_cast<double>(bytes_processed) / (1024.0 * 1024.0)) /
                   (r.total_ms / 1000.0);
  } else if (unit == "ops/s" && r.total_ms > 0) {
    r.throughput = static_cast<double>(r.iterations) / (r.total_ms / 1000.0);
  }
  r.unit = unit;
  return r;
}

static void PrintHeader() {
  std::cout << "| Scenario | Iterations | Total ms | Avg us | P50 us | P99 us | "
               "Throughput |\n";
  std::cout << "|---|---:|---:|---:|---:|---:|---:|\n";
}

static void PrintRow(const BenchResult& r) {
  std::cout << "| " << r.scenario << " | " << r.iterations << " | "
            << std::fixed << std::setprecision(2) << r.total_ms << " | "
            << r.avg_us << " | " << r.p50_us << " | " << r.p99_us << " | "
            << r.throughput << " " << r.unit << " |\n";
}

// ------------------------------------------------------------------
// 1. Serialization benchmarks
// ------------------------------------------------------------------

static BenchResult BenchProtobufSerialize(int iterations, size_t payload_bytes) {
  std::mt19937 rng(42);
  std::string data = RandomString(payload_bytes, rng);
  std::vector<double> latencies;
  latencies.reserve(iterations);
  size_t total_bytes = 0;
  for (int i = 0; i < iterations; ++i) {
    RaftLogEntryProto proto;
    proto.set_index(static_cast<uint64_t>(i + 1));
    proto.set_term(1);
    proto.set_data(data);
    proto.set_command("noop");
    proto.set_checksum(0xDEADBEEF);
    std::string payload;
    auto t0 = std::chrono::steady_clock::now();
    [[maybe_unused]] bool ok = proto.SerializeToString(&payload);
    auto t1 = std::chrono::steady_clock::now();
    latencies.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());
    total_bytes += payload.size();
  }
  return Summarize("protobuf_serialize_" + std::to_string(payload_bytes) + "B",
                   latencies, total_bytes, "MB/s");
}

static BenchResult BenchProtobufDeserialize(int iterations,
                                            size_t payload_bytes) {
  std::mt19937 rng(42);
  std::string data = RandomString(payload_bytes, rng);
  RaftLogEntryProto proto;
  proto.set_index(1);
  proto.set_term(1);
  proto.set_data(data);
  proto.set_command("noop");
  proto.set_checksum(0xDEADBEEF);
  std::string payload;
  [[maybe_unused]] bool ok = proto.SerializeToString(&payload);

  std::vector<double> latencies;
  latencies.reserve(iterations);
  size_t total_bytes = 0;
  for (int i = 0; i < iterations; ++i) {
    RaftLogEntryProto parsed;
    auto t0 = std::chrono::steady_clock::now();
    [[maybe_unused]] bool ok = parsed.ParseFromString(payload);
    auto t1 = std::chrono::steady_clock::now();
    latencies.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());
    total_bytes += payload.size();
  }
  return Summarize(
      "protobuf_deserialize_" + std::to_string(payload_bytes) + "B", latencies,
      total_bytes, "MB/s");
}

// ------------------------------------------------------------------
// 2. Raw file write / fsync benchmarks
// ------------------------------------------------------------------

static BenchResult BenchRawFileAppend(int iterations, size_t record_bytes,
                                      bool fsync_each) {
  std::string path = "/tmp/rollingraft_micro_raw.wal";
  RemoveDir(path);
  int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    std::cerr << "Failed to open raw wal file\n";
    return {};
  }
  std::string record(record_bytes, 'x');
  std::vector<double> latencies;
  latencies.reserve(iterations);
  for (int i = 0; i < iterations; ++i) {
    auto t0 = std::chrono::steady_clock::now();
    ssize_t n = write(fd, record.data(), record.size());
    if (fsync_each) {
#ifdef __APPLE__
      fcntl(fd, F_FULLFSYNC, 0);
#else
      fdatasync(fd);
#endif
    }
    auto t1 = std::chrono::steady_clock::now();
    if (n != static_cast<ssize_t>(record.size())) {
      std::cerr << "write failed\n";
      break;
    }
    latencies.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());
  }
  close(fd);
  RemoveDir(path);
  std::string name = std::string("raw_write_") + (fsync_each ? "fsync_" : "nosync_") +
                     std::to_string(record_bytes) + "B";
  return Summarize(name, latencies, iterations * record_bytes, "MB/s");
}

static BenchResult BenchWALAppend(int iterations, size_t payload_bytes,
                                  bool sync_each) {
  std::string dir = "/tmp/rollingraft_micro_wal";
  RemoveDir(dir);
  std::vector<double> latencies;
  latencies.reserve(iterations);
  size_t total_bytes = 0;
  {
    WALPersister wal;
    wal.Open(dir);
    std::mt19937 rng(42);
    std::string data = RandomString(payload_bytes, rng);
    for (int i = 0; i < iterations; ++i) {
      RaftLogEntry e;
      e.index_ = i + 1;
      e.term_ = 1;
      e.data_ = data;
      e.command_ = "noop";
      auto t0 = std::chrono::steady_clock::now();
      wal.AppendLogEntry(e);
      if (sync_each) wal.Sync();
      auto t1 = std::chrono::steady_clock::now();
      latencies.push_back(
          std::chrono::duration<double, std::micro>(t1 - t0).count());
      total_bytes += data.size();
    }
    if (!sync_each) wal.Sync();
    wal.Close();
  }
  RemoveDir(dir);
  std::string name = std::string("wal_append_") + (sync_each ? "sync_" : "nosync_") +
                     std::to_string(payload_bytes) + "B";
  return Summarize(name, latencies, total_bytes, "MB/s");
}

// ------------------------------------------------------------------
// 3. LevelDB batching benchmarks
// ------------------------------------------------------------------

static BenchResult BenchWALRecovery(int num_entries, size_t payload_bytes) {
  std::string dir = "/tmp/rollingraft_micro_wal_recovery";
  RemoveDir(dir);
  {
    WALPersister wal;
    wal.Open(dir);
    std::mt19937 rng(42);
    std::string data = RandomString(payload_bytes, rng);
    for (int i = 0; i < num_entries; ++i) {
      RaftLogEntry e;
      e.index_ = i + 1;
      e.term_ = 1;
      e.data_ = data;
      e.command_ = "noop";
      wal.AppendLogEntry(e);
    }
    wal.Sync();
    wal.Close();
  }

  std::vector<double> latencies;
  size_t total_bytes = 0;
  {
    WALPersister wal;
    auto t0 = std::chrono::steady_clock::now();
    wal.Open(dir);
    auto t1 = std::chrono::steady_clock::now();
    latencies.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());
    auto range = wal.GetLogRange();
    if (range.first != 1 || range.second != static_cast<uint64_t>(num_entries)) {
      std::cerr << "WAL recovery range mismatch: " << range.first << ".."
                << range.second << "\n";
    }
    wal.Close();
  }
  total_bytes = num_entries * payload_bytes;
  RemoveDir(dir);
  std::string name = "wal_recovery_" + std::to_string(num_entries) + "_" +
                     std::to_string(payload_bytes) + "B";
  return Summarize(name, latencies, total_bytes, "MB/s");
}

static BenchResult BenchHybridRecovery(int num_entries, size_t payload_bytes) {
  std::string dir = "/tmp/rollingraft_micro_hybrid_recovery";
  RemoveDir(dir);
  {
    HybridPersister p;
    p.Open(dir);
    std::mt19937 rng(42);
    std::string data = RandomString(payload_bytes, rng);
    std::vector<RaftLogEntry> batch;
    batch.reserve(100);
    for (int i = 0; i < num_entries; ++i) {
      RaftLogEntry e;
      e.index_ = i + 1;
      e.term_ = 1;
      e.data_ = data;
      e.command_ = "noop";
      batch.push_back(e);
      if (batch.size() >= 100) {
        p.AppendEntries(batch);
        batch.clear();
      }
    }
    if (!batch.empty()) p.AppendEntries(batch);
    p.Sync();
    p.Close();
  }

  std::vector<double> latencies;
  size_t total_bytes = 0;
  {
    HybridPersister p;
    auto t0 = std::chrono::steady_clock::now();
    p.Open(dir);
    auto t1 = std::chrono::steady_clock::now();
    latencies.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());
    p.Close();
  }
  total_bytes = num_entries * payload_bytes;
  RemoveDir(dir);
  std::string name = "hybrid_recovery_" + std::to_string(num_entries) + "_" +
                     std::to_string(payload_bytes) + "B";
  return Summarize(name, latencies, total_bytes, "MB/s");
}

static BenchResult BenchLevelDBBatch(const std::string& db_path, int iterations,
                                     int batch_size, size_t value_size,
                                     bool sync) {
  RemoveDir(db_path);
  leveldb::Options options;
  options.create_if_missing = true;
  options.compression = leveldb::kSnappyCompression;
  leveldb::DB* db_ptr = nullptr;
  leveldb::Status s = leveldb::DB::Open(options, db_path, &db_ptr);
  if (!s.ok()) {
    std::cerr << "LevelDB open failed: " << s.ToString() << "\n";
    return {};
  }
  std::unique_ptr<leveldb::DB> db(db_ptr);
  std::string value(value_size, 'v');
  std::vector<double> latencies;
  latencies.reserve(iterations / std::max(1, batch_size));
  int written = 0;
  while (written < iterations) {
    leveldb::WriteBatch batch;
    for (int i = 0; i < batch_size && written < iterations; ++i, ++written) {
      batch.Put("key" + std::to_string(written), value);
    }
    leveldb::WriteOptions wo;
    wo.sync = sync;
    auto t0 = std::chrono::steady_clock::now();
    db->Write(wo, &batch);
    auto t1 = std::chrono::steady_clock::now();
    latencies.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());
  }
  RemoveDir(db_path);
  std::string name = std::string("leveldb_batch_") + std::to_string(batch_size) +
                     "_" + (sync ? "sync_" : "nosync_") +
                     std::to_string(value_size) + "B";
  return Summarize(name, latencies, iterations * value_size, "MB/s");
}

// ------------------------------------------------------------------
// 4. Snapshot streaming benchmarks
// ------------------------------------------------------------------

static BenchResult BenchSnapshotStream(int total_bytes, int chunk_size,
                                       bool sync) {
  std::string dir = "/tmp/rollingraft_micro_snapshot";
  RemoveDir(dir);
  std::vector<double> latencies;
  size_t bytes_processed = 0;
  {
    StatePersister sp;
    sp.SetSyncOnWrite(sync);
    sp.Open(dir);

    std::string payload(total_bytes, 's');
    size_t offset = 0;
    auto provider = [&](std::string& chunk) -> bool {
      if (offset >= payload.size()) return false;
      size_t n = std::min<size_t>(chunk_size, payload.size() - offset);
      chunk.assign(payload.data() + offset, n);
      offset += n;
      return true;
    };

    auto t0 = std::chrono::steady_clock::now();
    sp.SaveSnapshotStream(provider, 1000, 5);
    auto t1 = std::chrono::steady_clock::now();
    latencies.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());
    bytes_processed = total_bytes;
    sp.Close();
  }
  RemoveDir(dir);
  std::string name = std::string("snapshot_stream_") +
                     std::to_string(chunk_size) + "_chunk_" +
                     (sync ? "sync" : "nosync");
  return Summarize(name, latencies, bytes_processed, "MB/s");
}

// ------------------------------------------------------------------
// Main
// ------------------------------------------------------------------

int main() {
  std::vector<BenchResult> results;

  const int kSerIter = 200000;
  results.push_back(BenchProtobufSerialize(kSerIter, 100));
  results.push_back(BenchProtobufSerialize(kSerIter, 1024));
  results.push_back(BenchProtobufDeserialize(kSerIter, 100));
  results.push_back(BenchProtobufDeserialize(kSerIter, 1024));

  results.push_back(BenchRawFileAppend(10000, 128, false));
  results.push_back(BenchRawFileAppend(10000, 128, true));
  results.push_back(BenchRawFileAppend(5000, 1024, false));
  results.push_back(BenchRawFileAppend(5000, 1024, true));

  results.push_back(BenchWALAppend(10000, 128, false));
  results.push_back(BenchWALAppend(10000, 128, true));

  results.push_back(BenchWALRecovery(10000, 128));
  results.push_back(BenchWALRecovery(100000, 128));

  results.push_back(BenchHybridRecovery(10000, 100));
  results.push_back(BenchHybridRecovery(100000, 100));

  results.push_back(BenchLevelDBBatch("/tmp/rollingraft_micro_leveldb1", 5000,
                                      1, 256, false));
  results.push_back(BenchLevelDBBatch("/tmp/rollingraft_micro_leveldb2", 5000,
                                      100, 256, false));
  results.push_back(BenchLevelDBBatch("/tmp/rollingraft_micro_leveldb3", 5000,
                                      100, 256, true));

  results.push_back(BenchSnapshotStream(10 * 1024 * 1024, 64 * 1024, false));
  results.push_back(BenchSnapshotStream(10 * 1024 * 1024, 64 * 1024, true));
  results.push_back(BenchSnapshotStream(10 * 1024 * 1024, 1024 * 1024, false));

  PrintHeader();
  for (const auto& r : results) PrintRow(r);
  return 0;
}
