/**
 * @file persister_benchmark.cpp
 * @brief LevelDBPersister microbenchmark for baseline establishment
 *
 * Measures three dimensions:
 * 1. Append throughput (ops/sec) vs entry size, batch size, compression
 * 2. Recovery time (reopen duration) vs entry count
 * 3. Memory footprint (peak RSS) vs entry count
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <numeric>
#include <random>
#include <string>
#include <sys/resource.h>
#include <vector>

#include "rollingraft/persister.h"

using namespace rollingraft;

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

static size_t GetPeakRssKb() {
  struct rusage usage {};
  if (getrusage(RUSAGE_SELF, &usage) == 0) {
#ifdef __APPLE__
    return usage.ru_maxrss / 1024;  // bytes -> KB
#else
    return usage.ru_maxrss;         // already KB
#endif
  }
  return 0;
}

static std::string RandomString(size_t len, std::mt19937& rng) {
  static const char kChars[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::uniform_int_distribution<size_t> dist(0, sizeof(kChars) - 2);
  std::string s;
  s.reserve(len);
  for (size_t i = 0; i < len; ++i) s.push_back(kChars[dist(rng)]);
  return s;
}

static RaftLogEntry MakeEntry(uint64_t index, uint64_t term,
                              const std::string& data) {
  RaftLogEntry e;
  e.index_ = index;
  e.term_ = term;
  e.data_ = data;
  return e;
}

static std::string FormatBytes(size_t bytes) {
  const char* units[] = {"B", "KB", "MB", "GB"};
  int unit = 0;
  double value = static_cast<double>(bytes);
  while (value >= 1024.0 && unit < 3) {
    value /= 1024.0;
    ++unit;
  }
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(2) << value << " " << units[unit];
  return oss.str();
}

// ------------------------------------------------------------------
// Benchmark: Append Throughput
// ------------------------------------------------------------------

struct AppendBenchConfig {
  int total_entries = 100000;
  size_t entry_size = 100;
  int batch_size = 1;
  Persister::CompressionType compression = Persister::kSnappyCompression;
};

struct AppendBenchResult {
  double ops_per_second = 0.0;
  double latency_avg_us = 0.0;
  double latency_p50_us = 0.0;
  double latency_p99_us = 0.0;
  size_t peak_rss_kb = 0;
  std::chrono::milliseconds duration_ms{0};
  size_t data_dir_size_bytes = 0;
};

static AppendBenchResult RunAppendBenchmark(const AppendBenchConfig& cfg,
                                            const std::string& data_dir) {
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  auto persister = CreateLevelDBPersister();
  persister->SetCompressionType(cfg.compression);
  if (!persister->Open(data_dir).ok()) {
    std::cerr << "Failed to open persister at " << data_dir << "\n";
    return {};
  }

  std::mt19937 rng(42);
  std::string payload = RandomString(cfg.entry_size, rng);

  std::vector<double> latencies_us;
  latencies_us.reserve(cfg.total_entries / std::max(1, cfg.batch_size));

  size_t rss_before_kb = GetPeakRssKb();

  auto t0 = std::chrono::steady_clock::now();

  int index = 1;
  while (index <= cfg.total_entries) {
    std::vector<RaftLogEntry> batch;
    batch.reserve(cfg.batch_size);
    for (int i = 0; i < cfg.batch_size && index <= cfg.total_entries; ++i, ++index) {
      batch.push_back(MakeEntry(index, 1, payload));
    }

    auto t_start = std::chrono::steady_clock::now();
    auto status = persister->AppendEntries(batch);
    auto t_end = std::chrono::steady_clock::now();

    if (!status.ok()) {
      std::cerr << "Append failed at index " << index << ": " << status.ToString() << "\n";
      break;
    }

    double us = std::chrono::duration<double, std::micro>(t_end - t_start).count();
    latencies_us.push_back(us);
  }

  auto t1 = std::chrono::steady_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);

  // Force sync to ensure all data is on disk for fair measurement
  persister->Sync();

  size_t rss_after_kb = GetPeakRssKb();

  // Compute data dir size
  size_t dir_size = 0;
  for (const auto& entry : std::filesystem::recursive_directory_iterator(data_dir)) {
    if (entry.is_regular_file()) dir_size += entry.file_size();
  }

  persister->Close();

  AppendBenchResult result;
  result.duration_ms = duration_ms;
  result.peak_rss_kb = rss_after_kb > rss_before_kb ? rss_after_kb - rss_before_kb : 0;
  result.data_dir_size_bytes = dir_size;

  int actual_ops = static_cast<int>(latencies_us.size());
  if (actual_ops > 0 && duration_ms.count() > 0) {
    result.ops_per_second = actual_ops * 1000.0 / duration_ms.count();

    std::sort(latencies_us.begin(), latencies_us.end());
    result.latency_avg_us = std::accumulate(latencies_us.begin(), latencies_us.end(), 0.0) / actual_ops;
    result.latency_p50_us = latencies_us[actual_ops * 50 / 100];
    result.latency_p99_us = latencies_us[std::min(actual_ops * 99 / 100, actual_ops - 1)];
  }

  std::filesystem::remove_all(data_dir);
  return result;
}

// ------------------------------------------------------------------
// Benchmark: Recovery Time
// ------------------------------------------------------------------

struct RecoveryBenchResult {
  std::chrono::milliseconds create_ms{0};
  std::chrono::milliseconds reopen_ms{0};
  size_t data_dir_size_bytes = 0;
};

static RecoveryBenchResult RunRecoveryBenchmark(int num_entries,
                                                size_t entry_size,
                                                Persister::CompressionType compression,
                                                const std::string& data_dir) {
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  std::mt19937 rng(42);
  std::string payload = RandomString(entry_size, rng);

  // Phase 1: Create DB
  {
    auto persister = CreateLevelDBPersister();
    persister->SetCompressionType(compression);
    persister->Open(data_dir);

    auto t0 = std::chrono::steady_clock::now();
    std::vector<RaftLogEntry> batch;
    batch.reserve(100);
    for (int i = 1; i <= num_entries; ++i) {
      batch.push_back(MakeEntry(i, 1, payload));
      if (batch.size() >= 100 || i == num_entries) {
        persister->AppendEntries(batch);
        batch.clear();
      }
    }
    persister->Sync();
    auto t1 = std::chrono::steady_clock::now();

    RecoveryBenchResult result;
    result.create_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);

    size_t dir_size = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(data_dir)) {
      if (entry.is_regular_file()) dir_size += entry.file_size();
    }
    result.data_dir_size_bytes = dir_size;

    persister->Close();
  }

  // Phase 2: Reopen and measure
  {
    auto persister = CreateLevelDBPersister();
    persister->SetCompressionType(compression);

    auto t0 = std::chrono::steady_clock::now();
    persister->Open(data_dir);
    auto t1 = std::chrono::steady_clock::now();

    RecoveryBenchResult result;
    result.reopen_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);

    size_t dir_size = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(data_dir)) {
      if (entry.is_regular_file()) dir_size += entry.file_size();
    }
    result.data_dir_size_bytes = dir_size;

    persister->Close();
    std::filesystem::remove_all(data_dir);
    return result;
  }
}

// ------------------------------------------------------------------
// Benchmark: Memory Footprint
// ------------------------------------------------------------------

struct MemoryBenchResult {
  size_t rss_before_kb = 0;
  size_t rss_after_kb = 0;
  size_t rss_delta_kb = 0;
  size_t data_dir_size_bytes = 0;
};

static MemoryBenchResult RunMemoryBenchmark(int num_entries,
                                            size_t entry_size,
                                            Persister::CompressionType compression,
                                            const std::string& data_dir) {
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  std::mt19937 rng(42);
  std::string payload = RandomString(entry_size, rng);

  auto persister = CreateLevelDBPersister();
  persister->SetCompressionType(compression);
  persister->Open(data_dir);

  // Get baseline RSS after open
  size_t rss_before_kb = GetPeakRssKb();

  std::vector<RaftLogEntry> batch;
  batch.reserve(100);
  for (int i = 1; i <= num_entries; ++i) {
    batch.push_back(MakeEntry(i, 1, payload));
    if (batch.size() >= 100 || i == num_entries) {
      persister->AppendEntries(batch);
      batch.clear();
    }
  }
  persister->Sync();

  size_t rss_after_kb = GetPeakRssKb();

  size_t dir_size = 0;
  for (const auto& entry : std::filesystem::recursive_directory_iterator(data_dir)) {
    if (entry.is_regular_file()) dir_size += entry.file_size();
  }

  persister->Close();
  std::filesystem::remove_all(data_dir);

  MemoryBenchResult result;
  result.rss_before_kb = rss_before_kb;
  result.rss_after_kb = rss_after_kb;
  result.rss_delta_kb = rss_after_kb > rss_before_kb ? rss_after_kb - rss_before_kb : 0;
  result.data_dir_size_bytes = dir_size;
  return result;
}

// ------------------------------------------------------------------
// Main
// ------------------------------------------------------------------

static void PrintHeader(const std::string& title) {
  std::cout << "\n" << std::string(70, '=') << "\n";
  std::cout << "  " << title << "\n";
  std::cout << std::string(70, '=') << "\n";
}

static void PrintAppendResult(const std::string& label,
                              const AppendBenchResult& r) {
  std::cout << std::left << std::setw(30) << label
            << " | Ops/sec: " << std::right << std::setw(10) << std::fixed << std::setprecision(0) << r.ops_per_second
            << " | Avg: " << std::setw(6) << std::setprecision(1) << r.latency_avg_us << " us"
            << " | P50: " << std::setw(6) << r.latency_p50_us << " us"
            << " | P99: " << std::setw(6) << r.latency_p99_us << " us"
            << " | RSS+: " << std::setw(6) << r.peak_rss_kb << " KB"
            << " | Dir: " << FormatBytes(r.data_dir_size_bytes)
            << "\n";
}

static void PrintRecoveryResult(const std::string& label,
                                const RecoveryBenchResult& r) {
  std::cout << std::left << std::setw(30) << label
            << " | Create: " << std::right << std::setw(6) << r.create_ms.count() << " ms"
            << " | Reopen: " << std::setw(6) << r.reopen_ms.count() << " ms"
            << " | Dir: " << FormatBytes(r.data_dir_size_bytes)
            << "\n";
}

static void PrintMemoryResult(const std::string& label,
                              const MemoryBenchResult& r) {
  std::cout << std::left << std::setw(30) << label
            << " | RSS before: " << std::right << std::setw(8) << r.rss_before_kb << " KB"
            << " | RSS after: " << std::setw(8) << r.rss_after_kb << " KB"
            << " | Delta: " << std::setw(8) << r.rss_delta_kb << " KB"
            << " | Dir: " << FormatBytes(r.data_dir_size_bytes)
            << "\n";
}

int main(int argc, char** argv) {
  (void)argc;
  (void)argv;

  const std::string kDataDir = "/tmp/rollingraft_persister_bench";

  std::cout << "RollingRaft LevelDBPersister Baseline Benchmark\n";
  std::cout << "Platform: " << (sizeof(void*) == 8 ? "x86_64" : "unknown") << "\n";
  std::cout << "Timestamp: " << std::chrono::system_clock::now().time_since_epoch().count() << "\n";

  // ================================================================
  // 1. Append Throughput
  // ================================================================
  PrintHeader("1. Append Throughput");
  std::cout << std::left << std::setw(30) << "Scenario"
            << " | Ops/sec  | Avg us | P50 us | P99 us | RSS+   | Dir size\n";
  std::cout << std::string(100, '-') << "\n";

  // Single-entry append, small payload, both compression modes
  for (auto compression : {Persister::kNoCompression, Persister::kSnappyCompression}) {
    std::string comp_label = (compression == Persister::kNoCompression) ? "no-compression" : "snappy";

    AppendBenchConfig cfg;
    cfg.total_entries = 50000;
    cfg.entry_size = 100;
    cfg.batch_size = 1;
    cfg.compression = compression;
    auto r = RunAppendBenchmark(cfg, kDataDir + "_append_single_100B_" + comp_label);
    PrintAppendResult("single-append 100B " + comp_label, r);
  }

  // Batched append, small payload
  for (int batch_size : {10, 100}) {
    AppendBenchConfig cfg;
    cfg.total_entries = 50000;
    cfg.entry_size = 100;
    cfg.batch_size = batch_size;
    cfg.compression = Persister::kSnappyCompression;
    auto r = RunAppendBenchmark(cfg, kDataDir + "_append_batch_" + std::to_string(batch_size));
    PrintAppendResult("batch-append " + std::to_string(batch_size) + "x100B snappy", r);
  }

  // Larger payload
  for (auto compression : {Persister::kNoCompression, Persister::kSnappyCompression}) {
    std::string comp_label = (compression == Persister::kNoCompression) ? "no-compression" : "snappy";

    AppendBenchConfig cfg;
    cfg.total_entries = 10000;
    cfg.entry_size = 1024;
    cfg.batch_size = 1;
    cfg.compression = compression;
    auto r = RunAppendBenchmark(cfg, kDataDir + "_append_single_1KB_" + comp_label);
    PrintAppendResult("single-append 1KB " + comp_label, r);
  }

  // ================================================================
  // 2. Recovery Time
  // ================================================================
  PrintHeader("2. Recovery Time (Reopen Duration)");
  std::cout << std::left << std::setw(30) << "Scenario"
            << " | Create ms | Reopen ms | Dir size\n";
  std::cout << std::string(70, '-') << "\n";

  for (int num_entries : {1000, 10000, 100000}) {
    for (auto compression : {Persister::kNoCompression, Persister::kSnappyCompression}) {
      std::string comp_label = (compression == Persister::kNoCompression) ? "no-comp" : "snappy";
      auto r = RunRecoveryBenchmark(num_entries, 100, compression,
                                    kDataDir + "_recovery_" + std::to_string(num_entries) + "_" + comp_label);
      PrintRecoveryResult(std::to_string(num_entries) + " entries 100B " + comp_label, r);
    }
  }

  // ================================================================
  // 3. Memory Footprint
  // ================================================================
  PrintHeader("3. Memory Footprint (Peak RSS)");
  std::cout << std::left << std::setw(30) << "Scenario"
            << " | RSS before | RSS after  | Delta      | Dir size\n";
  std::cout << std::string(80, '-') << "\n";

  for (int num_entries : {1000, 10000, 100000}) {
    for (auto compression : {Persister::kNoCompression, Persister::kSnappyCompression}) {
      std::string comp_label = (compression == Persister::kNoCompression) ? "no-comp" : "snappy";
      auto r = RunMemoryBenchmark(num_entries, 100, compression,
                                  kDataDir + "_memory_" + std::to_string(num_entries) + "_" + comp_label);
      PrintMemoryResult(std::to_string(num_entries) + " entries 100B " + comp_label, r);
    }
  }

  std::cout << "\n" << std::string(70, '=') << "\n";
  std::cout << "Benchmark complete.\n";
  return 0;
}
