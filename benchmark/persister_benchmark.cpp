/**
 * @file persister_benchmark.cpp
 * @brief Persister microbenchmark with pluggable backend support
 *
 * Supports both LevelDBPersister and HybridPersister (T3 Phase 3).
 * Produces CSV output for easy comparison across backends.
 *
 * Usage:
 *   ./benchmark_persister --backend=leveldb
 *   ./benchmark_persister --backend=hybrid --entries=50000 --payload=100
 *                         --batch=1,10,100 --compression=0,1
 *                         --output=results.csv
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "rollingraft/hybrid_persister.h"
#include "rollingraft/persister.h"

#include <sys/resource.h>

using namespace rollingraft;

// ------------------------------------------------------------------
// Backend selection
// ------------------------------------------------------------------

enum class Backend {
  kLevelDB,
  kHybrid,
};

static Backend ParseBackend(const std::string& s) {
  if (s == "hybrid") {
    return Backend::kHybrid;
  }
  if (s == "leveldb") {
    return Backend::kLevelDB;
  }
  std::cerr << "Unknown backend: " << s << ", defaulting to leveldb\n";
  return Backend::kLevelDB;
}

static std::string BackendName(Backend b) {
  switch (b) {
    case Backend::kHybrid:
      return "hybrid";
    case Backend::kLevelDB:
      return "leveldb";
  }
  return "unknown";
}

// Factory: wires LevelDBPersister or HybridPersister depending on backend.
static std::unique_ptr<Persister> CreatePersister(Backend backend) {
  if (backend == Backend::kHybrid) {
    return CreateHybridPersister();
  }
  return CreateLevelDBPersister();
}

// ------------------------------------------------------------------
// CLI arguments
// ------------------------------------------------------------------

struct BenchmarkArgs {
  Backend backend = Backend::kLevelDB;
  int entries = 50000;
  int payload_bytes = 100;
  std::vector<int> batch_sizes = {1, 10, 100};
  std::vector<int> compression_values = {0, 1};
  int threads = 1;
  std::string output_path = "persister_benchmark_results.csv";
  std::string data_dir_prefix = "/tmp/rollingraft_persister_bench";
};

static std::vector<int> ParseIntList(const std::string& s) {
  std::vector<int> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ',')) {
    try {
      out.push_back(std::stoi(item));
    } catch (...) {
      std::cerr << "Ignoring invalid integer: " << item << "\n";
    }
  }
  return out;
}

static BenchmarkArgs ParseArgs(int argc, char** argv) {
  BenchmarkArgs args;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--backend=", 0) == 0) {
      args.backend = ParseBackend(arg.substr(10));
    } else if (arg.rfind("--entries=", 0) == 0) {
      args.entries = std::stoi(arg.substr(10));
    } else if (arg.rfind("--payload=", 0) == 0) {
      args.payload_bytes = std::stoi(arg.substr(10));
    } else if (arg.rfind("--batch=", 0) == 0) {
      args.batch_sizes = ParseIntList(arg.substr(8));
    } else if (arg.rfind("--compression=", 0) == 0) {
      args.compression_values = ParseIntList(arg.substr(14));
    } else if (arg.rfind("--output=", 0) == 0) {
      args.output_path = arg.substr(9);
    } else if (arg.rfind("--data-dir=", 0) == 0) {
      args.data_dir_prefix = arg.substr(11);
    } else if (arg.rfind("--threads=", 0) == 0) {
      args.threads = std::stoi(arg.substr(10));
    } else if (arg == "--help" || arg == "-h") {
      std::cout
          << "Usage: " << argv[0] << " [options]\n"
          << "  --backend=NAME        leveldb (default) or hybrid\n"
          << "  --entries=N           entries per scenario (default 50000)\n"
          << "  --payload=N           payload size in bytes (default 100)\n"
          << "  --batch=LIST          comma-separated batch sizes (default 1,10,100)\n"
          << "  --compression=LIST    comma-separated 0/1 values (default 0,1)\n"
          << "  --threads=N           concurrent writer threads (default 1)\n"
          << "  --output=PATH         CSV output path (default persister_benchmark_results.csv)\n"
          << "  --data-dir=PATH       temp directory prefix (default "
             "/tmp/rollingraft_persister_bench)\n";
      std::exit(0);
    } else {
      std::cerr << "Unknown option: " << arg << "\n";
    }
  }
  return args;
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

static size_t GetPeakRssKb() {
  struct rusage usage {};
  if (getrusage(RUSAGE_SELF, &usage) == 0) {
#ifdef __APPLE__
    return usage.ru_maxrss / 1024;  // bytes -> KB
#else
    return usage.ru_maxrss;  // already KB
#endif
  }
  return 0;
}

static std::string RandomString(size_t len, std::mt19937& rng) {
  static const char kChars[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::uniform_int_distribution<size_t> dist(0, sizeof(kChars) - 2);
  std::string s;
  s.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    s.push_back(kChars[dist(rng)]);
  }
  return s;
}

static RaftLogEntry MakeEntry(uint64_t index, uint64_t term, const std::string& data) {
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

static size_t GetDirectorySize(const std::string& path) {
  size_t total = 0;
  if (!std::filesystem::exists(path)) {
    return 0;
  }
  for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
    if (entry.is_regular_file()) {
      total += entry.file_size();
    }
  }
  return total;
}

// ------------------------------------------------------------------
// Benchmark: Append Throughput
// ------------------------------------------------------------------

struct AppendResult {
  int batch_size = 0;
  Persister::CompressionType compression = Persister::kSnappyCompression;
  double ops_per_second = 0.0;
  double latency_avg_us = 0.0;
  double latency_p50_us = 0.0;
  double latency_p99_us = 0.0;
  size_t peak_rss_kb = 0;
  size_t data_dir_size_bytes = 0;
  std::chrono::milliseconds duration_ms{0};
};

static AppendResult RunAppendBenchmark(Backend backend, const std::string& data_dir,
                                       int total_entries, size_t entry_size, int batch_size,
                                       Persister::CompressionType compression) {
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  auto persister = CreatePersister(backend);
  persister->SetCompressionType(compression);
  if (!persister->Open(data_dir).ok()) {
    std::cerr << "Failed to open persister at " << data_dir << "\n";
    return {};
  }

  std::mt19937 rng(42);
  std::string payload = RandomString(entry_size, rng);

  std::vector<double> latencies_us;
  latencies_us.reserve(total_entries / std::max(1, batch_size));

  size_t rss_before_kb = GetPeakRssKb();

  auto t0 = std::chrono::steady_clock::now();

  int index = 1;
  while (index <= total_entries) {
    std::vector<RaftLogEntry> batch;
    batch.reserve(batch_size);
    for (int i = 0; i < batch_size && index <= total_entries; ++i, ++index) {
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

  persister->Sync();

  size_t rss_after_kb = GetPeakRssKb();
  size_t dir_size = GetDirectorySize(data_dir);

  persister->Close();
  std::filesystem::remove_all(data_dir);

  AppendResult result;
  result.batch_size = batch_size;
  result.compression = compression;
  result.duration_ms = duration_ms;
  result.peak_rss_kb = rss_after_kb > rss_before_kb ? rss_after_kb - rss_before_kb : 0;
  result.data_dir_size_bytes = dir_size;

  int actual_ops = static_cast<int>(latencies_us.size());
  if (actual_ops > 0 && duration_ms.count() > 0) {
    result.ops_per_second = actual_ops * 1000.0 / duration_ms.count();
    std::sort(latencies_us.begin(), latencies_us.end());
    result.latency_avg_us =
        std::accumulate(latencies_us.begin(), latencies_us.end(), 0.0) / actual_ops;
    result.latency_p50_us = latencies_us[actual_ops * 50 / 100];
    result.latency_p99_us = latencies_us[std::min(actual_ops * 99 / 100, actual_ops - 1)];
  }

  return result;
}

// ------------------------------------------------------------------
// Benchmark: Concurrent Append Throughput
// ------------------------------------------------------------------

struct ConcurrentAppendResult {
  int threads = 1;
  int batch_size = 0;
  Persister::CompressionType compression = Persister::kSnappyCompression;
  double total_ops_per_second = 0.0;
  double latency_avg_us = 0.0;
  double latency_p50_us = 0.0;
  double latency_p99_us = 0.0;
  size_t peak_rss_kb = 0;
  size_t data_dir_size_bytes = 0;
  std::chrono::milliseconds duration_ms{0};
};

static ConcurrentAppendResult RunConcurrentAppendBenchmark(
    Backend backend, const std::string& data_dir_prefix, int total_entries, size_t entry_size,
    int batch_size, Persister::CompressionType compression, int num_threads) {
  std::filesystem::remove_all(data_dir_prefix);

  std::string payload;
  {
    std::mt19937 rng(42);
    payload = RandomString(entry_size, rng);
  }

  int entries_per_thread = total_entries / num_threads;
  std::atomic<int> remaining{num_threads};
  std::vector<std::vector<double>> thread_latencies(num_threads);
  std::vector<std::thread> workers;
  workers.reserve(num_threads);

  size_t rss_before_kb = GetPeakRssKb();
  auto t0 = std::chrono::steady_clock::now();

  for (int t = 0; t < num_threads; ++t) {
    workers.emplace_back([&, t]() {
      std::string data_dir = data_dir_prefix + "_t" + std::to_string(t);
      std::filesystem::create_directories(data_dir);

      auto persister = CreatePersister(backend);
      persister->SetCompressionType(compression);
      if (!persister->Open(data_dir).ok()) {
        std::cerr << "Failed to open persister for thread " << t << "\n";
        remaining.fetch_sub(1);
        return;
      }

      std::vector<double>& latencies = thread_latencies[t];
      latencies.reserve(entries_per_thread / std::max(1, batch_size));

      int start_index = t * entries_per_thread + 1;
      int end_index = start_index + entries_per_thread;
      int index = start_index;

      while (index < end_index) {
        std::vector<RaftLogEntry> batch;
        batch.reserve(batch_size);
        for (int i = 0; i < batch_size && index < end_index; ++i, ++index) {
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
        latencies.push_back(us);
      }

      persister->Sync();
      persister->Close();
      remaining.fetch_sub(1);
    });
  }

  for (auto& w : workers) {
    w.join();
  }

  auto t1 = std::chrono::steady_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);

  size_t rss_after_kb = GetPeakRssKb();

  size_t total_dir_size = 0;
  for (int t = 0; t < num_threads; ++t) {
    total_dir_size += GetDirectorySize(data_dir_prefix + "_t" + std::to_string(t));
  }

  // Merge latencies from all threads
  std::vector<double> all_latencies;
  for (auto& vec : thread_latencies) {
    all_latencies.insert(all_latencies.end(), vec.begin(), vec.end());
  }

  ConcurrentAppendResult result;
  result.threads = num_threads;
  result.batch_size = batch_size;
  result.compression = compression;
  result.duration_ms = duration_ms;
  result.peak_rss_kb = rss_after_kb > rss_before_kb ? rss_after_kb - rss_before_kb : 0;
  result.data_dir_size_bytes = total_dir_size;

  int actual_ops = static_cast<int>(all_latencies.size());
  if (actual_ops > 0 && duration_ms.count() > 0) {
    result.total_ops_per_second = actual_ops * 1000.0 / duration_ms.count();
    std::sort(all_latencies.begin(), all_latencies.end());
    result.latency_avg_us =
        std::accumulate(all_latencies.begin(), all_latencies.end(), 0.0) / actual_ops;
    result.latency_p50_us = all_latencies[actual_ops * 50 / 100];
    result.latency_p99_us = all_latencies[std::min(actual_ops * 99 / 100, actual_ops - 1)];
  }

  std::filesystem::remove_all(data_dir_prefix);
  return result;
}

// ------------------------------------------------------------------
// Benchmark: Recovery Time
// ------------------------------------------------------------------

struct RecoveryResult {
  int num_entries = 0;
  Persister::CompressionType compression = Persister::kSnappyCompression;
  std::chrono::milliseconds create_ms{0};
  std::chrono::milliseconds reopen_ms{0};
  size_t data_dir_size_bytes = 0;
};

static RecoveryResult RunRecoveryBenchmark(Backend backend, const std::string& data_dir,
                                           int num_entries, size_t entry_size,
                                           Persister::CompressionType compression) {
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  std::mt19937 rng(42);
  std::string payload = RandomString(entry_size, rng);

  // Phase 1: Create DB
  {
    auto persister = CreatePersister(backend);
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

    RecoveryResult result;
    result.num_entries = num_entries;
    result.compression = compression;
    result.create_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
    result.data_dir_size_bytes = GetDirectorySize(data_dir);
    persister->Close();
  }

  // Phase 2: Reopen
  {
    auto persister = CreatePersister(backend);
    persister->SetCompressionType(compression);

    auto t0 = std::chrono::steady_clock::now();
    persister->Open(data_dir);
    auto t1 = std::chrono::steady_clock::now();

    RecoveryResult result;
    result.num_entries = num_entries;
    result.compression = compression;
    result.reopen_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
    result.data_dir_size_bytes = GetDirectorySize(data_dir);

    persister->Close();
    std::filesystem::remove_all(data_dir);
    return result;
  }
}

// ------------------------------------------------------------------
// Benchmark: Memory Footprint
// ------------------------------------------------------------------

struct MemoryResult {
  int num_entries = 0;
  Persister::CompressionType compression = Persister::kSnappyCompression;
  size_t rss_before_kb = 0;
  size_t rss_after_kb = 0;
  size_t rss_delta_kb = 0;
  size_t data_dir_size_bytes = 0;
};

static MemoryResult RunMemoryBenchmark(Backend backend, const std::string& data_dir,
                                       int num_entries, size_t entry_size,
                                       Persister::CompressionType compression) {
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  std::mt19937 rng(42);
  std::string payload = RandomString(entry_size, rng);

  auto persister = CreatePersister(backend);
  persister->SetCompressionType(compression);
  persister->Open(data_dir);

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

  MemoryResult result;
  result.num_entries = num_entries;
  result.compression = compression;
  result.rss_before_kb = rss_before_kb;
  result.rss_after_kb = rss_after_kb;
  result.rss_delta_kb = rss_after_kb > rss_before_kb ? rss_after_kb - rss_before_kb : 0;
  result.data_dir_size_bytes = GetDirectorySize(data_dir);

  persister->Close();
  std::filesystem::remove_all(data_dir);
  return result;
}

// ------------------------------------------------------------------
// CSV output
// ------------------------------------------------------------------

struct CsvRow {
  std::string scenario;
  std::string backend;
  int entries = 0;
  int payload_bytes = 0;
  int batch_size = 0;
  int compression = 0;
  int threads = 1;
  double ops_per_sec = 0.0;
  double latency_p50_us = 0.0;
  double latency_p99_us = 0.0;
  double latency_avg_us = 0.0;
  size_t rss_kb = 0;
  double dir_size_mb = 0.0;
  double duration_ms = 0.0;
  int recovery_entries = 0;
  double reopen_ms = 0.0;
  double create_ms = 0.0;
};

static void WriteCsvHeader(std::ostream& out) {
  out << "scenario,backend,entries,payload_bytes,batch_size,compression,threads,"
      << "ops_per_sec,latency_p50_us,latency_p99_us,latency_avg_us,"
      << "rss_kb,dir_size_mb,duration_ms,recovery_entries,reopen_ms,create_ms\n";
}

static void WriteCsvRow(std::ostream& out, const CsvRow& row) {
  out << row.scenario << "," << row.backend << "," << row.entries << "," << row.payload_bytes << ","
      << row.batch_size << "," << row.compression << "," << row.threads << "," << std::fixed
      << std::setprecision(2) << row.ops_per_sec << "," << row.latency_p50_us << ","
      << row.latency_p99_us << "," << row.latency_avg_us << "," << row.rss_kb << ","
      << row.dir_size_mb << "," << row.duration_ms << "," << row.recovery_entries << ","
      << row.reopen_ms << "," << row.create_ms << "\n";
}

// ------------------------------------------------------------------
// Main
// ------------------------------------------------------------------

int main(int argc, char** argv) {
  BenchmarkArgs args = ParseArgs(argc, argv);

  std::vector<CsvRow> rows;

  std::cout << "RollingRaft Persister Benchmark\n";
  std::cout << "Backend: " << BackendName(args.backend) << "\n";
  std::cout << "Entries: " << args.entries << "\n";
  std::cout << "Payload: " << args.payload_bytes << " B\n";
  std::cout << "Batch sizes: ";
  for (auto b : args.batch_sizes) {
    std::cout << b << " ";
  }
  std::cout << "\n";
  std::cout << "Compression modes: ";
  for (auto c : args.compression_values) {
    std::cout << c << " ";
  }
  std::cout << "\n";
  std::cout << "Threads: " << args.threads << "\n\n";

  // ================================================================
  // 1. Append Throughput (single-threaded)
  // ================================================================
  std::cout << "[1/4] Running append throughput benchmarks...\n";
  for (int compression_val : args.compression_values) {
    auto compression =
        (compression_val == 0) ? Persister::kNoCompression : Persister::kSnappyCompression;
    for (int batch_size : args.batch_sizes) {
      std::string data_dir = args.data_dir_prefix + "_append_" +
                             std::to_string(args.payload_bytes) + "B_" +
                             std::to_string(batch_size) + "x_" + std::to_string(compression_val);
      auto r = RunAppendBenchmark(args.backend, data_dir, args.entries, args.payload_bytes,
                                  batch_size, compression);

      CsvRow row;
      row.scenario = "append";
      row.backend = BackendName(args.backend);
      row.entries = args.entries;
      row.payload_bytes = args.payload_bytes;
      row.batch_size = batch_size;
      row.compression = compression_val;
      row.ops_per_sec = r.ops_per_second;
      row.latency_p50_us = r.latency_p50_us;
      row.latency_p99_us = r.latency_p99_us;
      row.latency_avg_us = r.latency_avg_us;
      row.rss_kb = r.peak_rss_kb;
      row.dir_size_mb = r.data_dir_size_bytes / (1024.0 * 1024.0);
      row.duration_ms = r.duration_ms.count();
      rows.push_back(row);

      std::cout << "  batch=" << batch_size << " compression=" << compression_val
                << " ops/sec=" << std::fixed << std::setprecision(0) << r.ops_per_second
                << " p50=" << r.latency_p50_us << "us"
                << " p99=" << r.latency_p99_us << "us"
                << " rss+=" << r.peak_rss_kb << "KB"
                << " dir=" << FormatBytes(r.data_dir_size_bytes) << "\n";
    }
  }

  // ================================================================
  // 2. Recovery Time
  // ================================================================
  std::cout << "\n[2/4] Running recovery benchmarks...\n";
  std::vector<int> recovery_entry_counts = {1000, 10000, 100000};
  for (int num_entries : recovery_entry_counts) {
    for (int compression_val : args.compression_values) {
      auto compression =
          (compression_val == 0) ? Persister::kNoCompression : Persister::kSnappyCompression;
      std::string data_dir = args.data_dir_prefix + "_recovery_" + std::to_string(num_entries) +
                             "_" + std::to_string(compression_val);
      auto r = RunRecoveryBenchmark(args.backend, data_dir, num_entries, args.payload_bytes,
                                    compression);

      CsvRow row;
      row.scenario = "recovery";
      row.backend = BackendName(args.backend);
      row.entries = num_entries;
      row.payload_bytes = args.payload_bytes;
      row.batch_size = 0;
      row.compression = compression_val;
      row.dir_size_mb = r.data_dir_size_bytes / (1024.0 * 1024.0);
      row.recovery_entries = num_entries;
      row.reopen_ms = r.reopen_ms.count();
      row.create_ms = r.create_ms.count();
      rows.push_back(row);

      std::cout << "  entries=" << num_entries << " compression=" << compression_val
                << " create=" << r.create_ms.count() << "ms"
                << " reopen=" << r.reopen_ms.count() << "ms"
                << " dir=" << FormatBytes(r.data_dir_size_bytes) << "\n";
    }
  }

  // ================================================================
  // 3. Memory Footprint
  // ================================================================
  std::cout << "\n[3/4] Running memory benchmarks...\n";
  for (int num_entries : recovery_entry_counts) {
    for (int compression_val : args.compression_values) {
      auto compression =
          (compression_val == 0) ? Persister::kNoCompression : Persister::kSnappyCompression;
      std::string data_dir = args.data_dir_prefix + "_memory_" + std::to_string(num_entries) + "_" +
                             std::to_string(compression_val);
      auto r =
          RunMemoryBenchmark(args.backend, data_dir, num_entries, args.payload_bytes, compression);

      CsvRow row;
      row.scenario = "memory";
      row.backend = BackendName(args.backend);
      row.entries = num_entries;
      row.payload_bytes = args.payload_bytes;
      row.batch_size = 0;
      row.compression = compression_val;
      row.rss_kb = r.rss_delta_kb;
      row.dir_size_mb = r.data_dir_size_bytes / (1024.0 * 1024.0);
      rows.push_back(row);

      std::cout << "  entries=" << num_entries << " compression=" << compression_val
                << " rss_delta=" << r.rss_delta_kb << "KB"
                << " dir=" << FormatBytes(r.data_dir_size_bytes) << "\n";
    }
  }

  // ================================================================
  // 4. Concurrent Append Throughput (multi-threaded)
  // ================================================================
  if (args.threads > 1) {
    std::cout << "\n[4/4] Running concurrent append throughput benchmarks (" << args.threads
              << " threads)...\n";
    for (int compression_val : args.compression_values) {
      auto compression =
          (compression_val == 0) ? Persister::kNoCompression : Persister::kSnappyCompression;
      for (int batch_size : args.batch_sizes) {
        std::string data_dir = args.data_dir_prefix + "_concurrent_" +
                               std::to_string(args.payload_bytes) + "B_" +
                               std::to_string(batch_size) + "x_" + std::to_string(compression_val);
        auto r =
            RunConcurrentAppendBenchmark(args.backend, data_dir, args.entries, args.payload_bytes,
                                         batch_size, compression, args.threads);

        CsvRow row;
        row.scenario = "concurrent_append";
        row.backend = BackendName(args.backend);
        row.entries = args.entries;
        row.payload_bytes = args.payload_bytes;
        row.batch_size = batch_size;
        row.compression = compression_val;
        row.threads = args.threads;
        row.ops_per_sec = r.total_ops_per_second;
        row.latency_p50_us = r.latency_p50_us;
        row.latency_p99_us = r.latency_p99_us;
        row.latency_avg_us = r.latency_avg_us;
        row.rss_kb = r.peak_rss_kb;
        row.dir_size_mb = r.data_dir_size_bytes / (1024.0 * 1024.0);
        row.duration_ms = r.duration_ms.count();
        rows.push_back(row);

        std::cout << "  threads=" << args.threads << " batch=" << batch_size
                  << " compression=" << compression_val << " ops/sec=" << std::fixed
                  << std::setprecision(0) << r.total_ops_per_second << " p50=" << r.latency_p50_us
                  << "us"
                  << " p99=" << r.latency_p99_us << "us"
                  << " rss+=" << r.peak_rss_kb << "KB"
                  << " dir=" << FormatBytes(r.data_dir_size_bytes) << "\n";
      }
    }
  } else {
    std::cout << "\n[4/4] Skipping concurrent benchmark (use --threads=N where N>1)\n";
  }

  // ================================================================
  // Write CSV
  // ================================================================
  std::ofstream csv(args.output_path);
  if (csv) {
    WriteCsvHeader(csv);
    for (const auto& row : rows) {
      WriteCsvRow(csv, row);
    }
    std::cout << "\nCSV written to: " << args.output_path << "\n";
  } else {
    std::cerr << "Failed to write CSV: " << args.output_path << "\n";
  }

  std::cout << "Benchmark complete.\n";
  return 0;
}
