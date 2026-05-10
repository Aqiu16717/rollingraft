/**
 * @file benchmark.cpp
 * @brief Benchmark framework implementation
 */

#include "benchmark.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>

namespace rollingraft {

// ========== BenchmarkStats ==========

std::string BenchmarkStats::ToString() const {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(2);
  oss << "Duration: " << duration_ms.count() << " ms\n";
  oss << "Total Operations: " << total_operations << "\n";
  oss << "Throughput: " << operations_per_second << " ops/sec\n";
  oss << "Success Rate: " << (success_rate * 100.0) << "%\n";
  oss << "Latency (us): min=" << latency_min_us << " avg=" << latency_avg_us
      << " p50=" << latency_p50_us << " p99=" << latency_p99_us
      << " max=" << latency_max_us << "\n";
  return oss.str();
}

// ========== Benchmark ==========

Benchmark::Benchmark(const BenchmarkConfig& config) : config_(config) {}

BenchmarkStats Benchmark::Run() {
  if (!SetUp()) {
    BenchmarkStats stats;
    stats.failure_count = 1;
    return stats;
  }

  std::vector<double> latencies;
  latencies.reserve(100000);

  uint64_t success_count = 0;
  uint64_t failure_count = 0;

  auto start_time = std::chrono::steady_clock::now();
  auto end_time = start_time + config_.duration;

  uint64_t op_count = 0;

  while (std::chrono::steady_clock::now() < end_time) {
    auto op_start = std::chrono::steady_clock::now();
    auto result = DoOperation();
    auto op_end = std::chrono::steady_clock::now();

    auto latency_us =
        std::chrono::duration_cast<std::chrono::microseconds>(op_end - op_start)
            .count();
    latencies.push_back(static_cast<double>(latency_us));

    if (result.success) {
      ++success_count;
    } else {
      ++failure_count;
    }

    ++op_count;

    if (config_.progress_interval > 0 &&
        op_count % config_.progress_interval == 0) {
      std::cout << "Progress: " << op_count << " operations\r" << std::flush;
    }
  }

  if (config_.progress_interval > 0) {
    std::cout << std::endl;
  }

  auto actual_end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      actual_end - start_time);

  TearDown();

  // Calculate statistics
  BenchmarkStats stats;
  stats.total_operations = op_count;
  stats.success_count = success_count;
  stats.failure_count = failure_count;
  stats.success_rate =
      (op_count > 0) ? static_cast<double>(success_count) / op_count : 0.0;
  stats.duration_ms = duration;
  stats.operations_per_second =
      (duration.count() > 0)
          ? (static_cast<double>(op_count) * 1000.0) / duration.count()
          : 0.0;

  // Calculate latency statistics
  if (!latencies.empty()) {
    std::sort(latencies.begin(), latencies.end());

    stats.latency_min_us = latencies.front();
    stats.latency_max_us = latencies.back();

    double sum = 0.0;
    for (double lat : latencies) {
      sum += lat;
    }
    stats.latency_avg_us = sum / latencies.size();

    // Percentiles
    auto percentile = [&](double p) -> double {
      size_t idx =
          static_cast<size_t>(std::ceil((p / 100.0) * latencies.size())) - 1;
      if (idx >= latencies.size()) idx = latencies.size() - 1;
      return latencies[idx];
    };

    stats.latency_p50_us = percentile(50.0);
    stats.latency_p99_us = percentile(99.0);
    stats.latency_p999_us = percentile(99.9);
  }

  return stats;
}

// ========== ThroughputBenchmark ==========

ThroughputBenchmark::ThroughputBenchmark(
    const BenchmarkConfig& config, std::function<OperationResult()> operation)
    : Benchmark(config), operation_(std::move(operation)) {}

OperationResult ThroughputBenchmark::DoOperation() { return operation_(); }

// ========== LatencyBenchmark ==========

LatencyBenchmark::LatencyBenchmark(const BenchmarkConfig& config,
                                   std::function<OperationResult()> operation)
    : Benchmark(config), operation_(std::move(operation)) {}

OperationResult LatencyBenchmark::DoOperation() { return operation_(); }

std::map<int, BenchmarkStats> LatencyBenchmark::RunLatencyCurve() {
  std::map<int, BenchmarkStats> results;

  // Test at different target throughputs (ops/sec)
  std::vector<int> target_throughputs = {100, 500, 1000, 2000, 5000, 10000};

  for (int target : target_throughputs) {
    std::cout << "Testing at target throughput: " << target << " ops/sec"
              << std::endl;

    // Adjust config for this target throughput
    auto config = config_;
    config.duration = std::chrono::seconds(5);  // Shorter runs for curve

    // Calculate delay between operations to achieve target rate
    auto delay_between_ops = std::chrono::microseconds(1000000 / target);

    // Wrap operation with rate limiting
    auto original_op = operation_;
    auto last_op_time = std::chrono::steady_clock::now();

    auto rate_limited_op = [&]() -> OperationResult {
      auto now = std::chrono::steady_clock::now();
      auto elapsed = now - last_op_time;
      if (elapsed < delay_between_ops) {
        std::this_thread::sleep_for(delay_between_ops - elapsed);
      }
      last_op_time = std::chrono::steady_clock::now();
      return original_op();
    };

    // Temporarily replace operation
    operation_ = rate_limited_op;
    auto stats = Run();
    operation_ = original_op;

    results[target] = stats;
  }

  return results;
}

// ========== FailoverBenchmark ==========

FailoverBenchmark::FailoverBenchmark(std::function<OperationResult()> operation,
                                     std::function<void()> kill_leader,
                                     std::function<bool()> is_leader_elected)
    : Benchmark(BenchmarkConfig{}),
      operation_(std::move(operation)),
      kill_leader_(std::move(kill_leader)),
      is_leader_elected_(std::move(is_leader_elected)) {}

OperationResult FailoverBenchmark::DoOperation() { return operation_(); }

FailoverBenchmark::FailoverResult FailoverBenchmark::RunFailover() {
  FailoverResult result;

  std::cout << "Starting failover benchmark..." << std::endl;
  std::cout << "Running steady load for 5 seconds..." << std::endl;

  // Run steady load for a few seconds
  auto steady_start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - steady_start <
         std::chrono::seconds(5)) {
    auto r = operation_();
    if (!r.success) {
      std::cout << "Operation failed during steady state!" << std::endl;
    }
  }

  std::cout << "Killing leader..." << std::endl;

  // Kill the leader
  auto kill_time = std::chrono::steady_clock::now();
  kill_leader_();

  // Continue operations and measure recovery
  auto detection_time = kill_time;
  bool detected = false;
  bool recovered = false;

  uint64_t ops_during_failover = 0;
  uint64_t ops_failed = 0;

  auto failover_start = kill_time;
  auto max_wait = std::chrono::seconds(30);

  while (std::chrono::steady_clock::now() - failover_start < max_wait) {
    auto r = operation_();
    ++ops_during_failover;

    if (!r.success) {
      ++ops_failed;
      if (!detected) {
        // First failure indicates detection
        detection_time = std::chrono::steady_clock::now();
        detected = true;
        std::cout << "Leader failure detected after "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(
                         detection_time - kill_time)
                         .count()
                  << " ms" << std::endl;
      }
    } else {
      if (detected && !recovered) {
        // Success after failure indicates recovery
        recovered = true;
        auto recovery_time = std::chrono::steady_clock::now();

        result.detection_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                detection_time - kill_time);
        result.election_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                recovery_time - detection_time);
        result.recovery_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                recovery_time - kill_time);
        result.operations_during_failover = ops_during_failover;
        result.operations_failed = ops_failed;

        std::cout << "Failover complete!" << std::endl;
        std::cout << "  Detection time: " << result.detection_time.count()
                  << " ms" << std::endl;
        std::cout << "  Election time: " << result.election_time.count()
                  << " ms" << std::endl;
        std::cout << "  Total recovery: " << result.recovery_time.count()
                  << " ms" << std::endl;

        return result;
      }
    }

    // Small delay to avoid spinning too fast
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  if (!recovered) {
    std::cout << "Failover test timed out!" << std::endl;
    result.recovery_time = max_wait;
  }

  return result;
}

// ========== Utility Functions ==========

namespace benchmark {

void PrintResults(const std::vector<BenchmarkStats>& results) {
  std::cout << "\n========== Benchmark Results ==========\n" << std::endl;

  std::cout << std::left << std::setw(10) << "Run" << std::setw(15)
            << "Throughput" << std::setw(12) << "Success%" << std::setw(12)
            << "P50 Lat" << std::setw(12) << "P99 Lat" << std::setw(12)
            << "Max Lat" << std::endl;
  std::cout << std::string(73, '-') << std::endl;

  for (size_t i = 0; i < results.size(); ++i) {
    const auto& s = results[i];
    std::cout << std::left << std::setw(10) << (i + 1) << std::setw(15)
              << static_cast<int>(s.operations_per_second) << std::setw(12)
              << std::fixed << std::setprecision(1) << (s.success_rate * 100)
              << std::setw(12) << static_cast<int>(s.latency_p50_us)
              << std::setw(12) << static_cast<int>(s.latency_p99_us)
              << std::setw(12) << static_cast<int>(s.latency_max_us)
              << std::endl;
  }
}

void PrintComparison(const std::map<std::string, BenchmarkStats>& results) {
  std::cout << "\n========== Comparison ==========\n" << std::endl;

  std::cout << std::left << std::setw(20) << "Benchmark" << std::setw(15)
            << "Throughput" << std::setw(12) << "P50 Lat" << std::setw(12)
            << "P99 Lat" << std::endl;
  std::cout << std::string(59, '-') << std::endl;

  for (const auto& [name, s] : results) {
    std::cout << std::left << std::setw(20) << name << std::setw(15)
              << static_cast<int>(s.operations_per_second) << std::setw(12)
              << static_cast<int>(s.latency_p50_us) << std::setw(12)
              << static_cast<int>(s.latency_p99_us) << std::endl;
  }
}

void SaveToJson(const std::string& filename, const BenchmarkStats& stats) {
  // Simple JSON serialization
  std::ofstream file(filename);
  if (!file.is_open()) return;

  file << "{\n";
  file << "  \"throughput_ops_per_sec\": " << stats.operations_per_second
       << ",\n";
  file << "  \"total_operations\": " << stats.total_operations << ",\n";
  file << "  \"success_count\": " << stats.success_count << ",\n";
  file << "  \"failure_count\": " << stats.failure_count << ",\n";
  file << "  \"success_rate\": " << stats.success_rate << ",\n";
  file << "  \"latency_us\": {\n";
  file << "    \"min\": " << stats.latency_min_us << ",\n";
  file << "    \"avg\": " << stats.latency_avg_us << ",\n";
  file << "    \"p50\": " << stats.latency_p50_us << ",\n";
  file << "    \"p99\": " << stats.latency_p99_us << ",\n";
  file << "    \"p999\": " << stats.latency_p999_us << ",\n";
  file << "    \"max\": " << stats.latency_max_us << "\n";
  file << "  },\n";
  file << "  \"duration_ms\": " << stats.duration_ms.count() << "\n";
  file << "}\n";
}

}  // namespace benchmark

}  // namespace rollingraft
