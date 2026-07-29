/**
 * @file concurrent_writes.cpp
 * @brief Scenario: Concurrent write throughput benchmark
 *
 * Measures throughput and latency under varying concurrency levels
 * and payload sizes. Spawns N worker threads, each issuing write
 * requests via ClusterBenchmark::ExecuteCommand (internal path).
 */

#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../benchmark.h"
#include "../cluster_benchmark.h"
#include "../scenario_registry.h"

namespace rollingraft {

using namespace std::chrono;

struct ConcurrentConfigResult {
  int num_clients = 0;
  size_t payload_size = 0;
  BenchmarkStats stats;
};

class ConcurrentWritesScenario : public ClusterBenchmark {
 public:
  ConcurrentWritesScenario()
      : ClusterBenchmark(
            []() {
              BenchmarkConfig config;
              config.duration = std::chrono::seconds(30);
              config.num_clients = 1;
              config.payload_size = 100;
              return config;
            }(),
            []() {
              BenchmarkClusterConfig config;
              config.num_nodes = 3;
              config.election_timeout = std::chrono::milliseconds(300);
              config.heartbeat_interval = std::chrono::milliseconds(50);
              return config;
            }()) {}

  std::string Name() const override { return "concurrent_writes"; }

  BenchmarkStats Run() override;

 protected:
  OperationResult DoOperation() override { return OperationResult{}; }

 private:
  ConcurrentConfigResult RunConfiguration(int num_clients, size_t payload_size, int duration_sec);
  void PrintCsvResults(const std::vector<ConcurrentConfigResult>& results) const;
  void SaveCsvResults(const std::vector<ConcurrentConfigResult>& results,
                      const std::string& filename) const;
};

BenchmarkStats ConcurrentWritesScenario::Run() {
  if (!SetUp()) {
    BenchmarkStats fail;
    fail.failure_count = 1;
    return fail;
  }

  std::cout << "\n========== Concurrent Writes Benchmark ==========\n" << std::endl;

  // Test matrix: concurrency levels × payload sizes
  std::vector<int> client_counts = {1, 5, 10};
  std::vector<size_t> payload_sizes = {100};
  constexpr int kDurationSec = 3;

  std::vector<ConcurrentConfigResult> all_results;

  for (size_t payload_size : payload_sizes) {
    for (int num_clients : client_counts) {
      std::cout << "\n--- Configuration: " << num_clients << " clients, " << payload_size
                << " B payload, " << kDurationSec << " s duration ---" << std::endl;

      auto result = RunConfiguration(num_clients, payload_size, kDurationSec);
      all_results.push_back(result);

      std::cout << "Throughput: " << result.stats.operations_per_second
                << " ops/sec | P50: " << result.stats.latency_p50_us
                << " us | P99: " << result.stats.latency_p99_us
                << " us | Success: " << (result.stats.success_rate * 100.0) << "%" << std::endl;
    }
  }

  PrintCsvResults(all_results);
  SaveCsvResults(all_results, "benchmark/results/concurrent_writes.csv");

  TearDown();

  BenchmarkStats aggregate;
  for (const auto& r : all_results) {
    aggregate.operations_per_second += r.stats.operations_per_second;
    aggregate.latency_p50_us += r.stats.latency_p50_us;
    aggregate.latency_p99_us += r.stats.latency_p99_us;
    aggregate.success_rate += r.stats.success_rate;
    aggregate.total_operations += r.stats.total_operations;
    aggregate.success_count += r.stats.success_count;
    aggregate.failure_count += r.stats.failure_count;
  }
  double n = static_cast<double>(all_results.size());
  aggregate.operations_per_second /= n;
  aggregate.latency_p50_us /= n;
  aggregate.latency_p99_us /= n;
  aggregate.success_rate /= n;
  return aggregate;
}

ConcurrentConfigResult ConcurrentWritesScenario::RunConfiguration(int num_clients,
                                                                  size_t payload_size,
                                                                  int duration_sec) {
  ConcurrentConfigResult result;
  result.num_clients = num_clients;
  result.payload_size = payload_size;

  std::string payload(payload_size, 'x');

  std::vector<std::vector<double>> per_thread_latencies(num_clients);
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> success_ops{0};
  std::atomic<uint64_t> failed_ops{0};

  auto start_time = steady_clock::now();
  auto end_time = start_time + seconds(duration_sec);

  std::vector<std::thread> workers;
  workers.reserve(num_clients);

  for (int i = 0; i < num_clients; ++i) {
    workers.emplace_back([&, i]() {
      int local_errs = 0;
      while (steady_clock::now() < end_time) {
        auto op_start = steady_clock::now();
        auto status = ExecuteCommand(payload);
        auto op_end = steady_clock::now();

        if (!status.ok()) {
          ++failed_ops;
          if (local_errs < 3) {
            std::cerr << "[Worker " << i << " ERR] " << status.ToString() << std::endl;
            ++local_errs;
          }
          // Stop this worker on first error to avoid timeout artifacts
          break;
        }

        auto latency_us = duration_cast<microseconds>(op_end - op_start).count();
        per_thread_latencies[i].push_back(static_cast<double>(latency_us));

        ++total_ops;
        ++success_ops;
      }
    });
  }

  for (auto& t : workers) {
    t.join();
  }

  auto actual_end = steady_clock::now();
  auto duration = duration_cast<milliseconds>(actual_end - start_time);

  std::vector<double> all_latencies;
  size_t total_latencies = 0;
  for (const auto& tl : per_thread_latencies) {
    total_latencies += tl.size();
  }
  all_latencies.reserve(total_latencies);

  for (const auto& tl : per_thread_latencies) {
    all_latencies.insert(all_latencies.end(), tl.begin(), tl.end());
  }

  BenchmarkStats& stats = result.stats;
  stats.duration_ms = duration;
  stats.total_operations = total_ops.load();
  stats.success_count = success_ops.load();
  stats.failure_count = failed_ops.load();
  stats.success_rate = (stats.total_operations > 0)
                           ? static_cast<double>(stats.success_count) / stats.total_operations
                           : 0.0;
  stats.operations_per_second =
      (duration.count() > 0)
          ? (static_cast<double>(stats.total_operations) * 1000.0) / duration.count()
          : 0.0;

  if (!all_latencies.empty()) {
    std::sort(all_latencies.begin(), all_latencies.end());

    stats.latency_min_us = all_latencies.front();
    stats.latency_max_us = all_latencies.back();

    double sum = 0.0;
    for (double lat : all_latencies) {
      sum += lat;
    }
    stats.latency_avg_us = sum / all_latencies.size();

    auto percentile = [&](double p) -> double {
      size_t idx = static_cast<size_t>(std::ceil((p / 100.0) * all_latencies.size())) - 1;
      if (idx >= all_latencies.size()) {
        idx = all_latencies.size() - 1;
      }
      return all_latencies[idx];
    };

    stats.latency_p50_us = percentile(50.0);
    stats.latency_p99_us = percentile(99.0);
    stats.latency_p999_us = percentile(99.9);
  }

  return result;
}

void ConcurrentWritesScenario::PrintCsvResults(
    const std::vector<ConcurrentConfigResult>& results) const {
  std::cout << "\n========== CSV Results ==========\n" << std::endl;

  std::cout << "clients,payload_bytes,duration_ms,total_ops,ops_per_sec,"
               "success_rate,latency_min_us,latency_avg_us,latency_p50_us,"
               "latency_p99_us,latency_p999_us,latency_max_us"
            << std::endl;

  for (const auto& r : results) {
    const auto& s = r.stats;
    std::cout << r.num_clients << "," << r.payload_size << "," << s.duration_ms.count() << ","
              << s.total_operations << "," << s.operations_per_second << "," << s.success_rate
              << "," << s.latency_min_us << "," << s.latency_avg_us << "," << s.latency_p50_us
              << "," << s.latency_p99_us << "," << s.latency_p999_us << "," << s.latency_max_us
              << std::endl;
  }
}

void ConcurrentWritesScenario::SaveCsvResults(const std::vector<ConcurrentConfigResult>& results,
                                              const std::string& filename) const {
  std::filesystem::create_directories(std::filesystem::path(filename).parent_path());

  std::ofstream file(filename);
  if (!file.is_open()) {
    std::cerr << "[WARN] Failed to open CSV file: " << filename << std::endl;
    return;
  }

  file << "clients,payload_bytes,duration_ms,total_ops,ops_per_sec,"
          "success_rate,latency_min_us,latency_avg_us,latency_p50_us,"
          "latency_p99_us,latency_p999_us,latency_max_us\n";

  for (const auto& r : results) {
    const auto& s = r.stats;
    file << r.num_clients << "," << r.payload_size << "," << s.duration_ms.count() << ","
         << s.total_operations << "," << s.operations_per_second << "," << s.success_rate << ","
         << s.latency_min_us << "," << s.latency_avg_us << "," << s.latency_p50_us << ","
         << s.latency_p99_us << "," << s.latency_p999_us << "," << s.latency_max_us << "\n";
  }

  std::cout << "\nCSV saved to: " << filename << std::endl;
}

REGISTER_SCENARIO(concurrent_writes, []() { return std::make_unique<ConcurrentWritesScenario>(); });

}  // namespace rollingraft
