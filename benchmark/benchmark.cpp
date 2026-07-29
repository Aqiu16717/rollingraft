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
      << " p50=" << latency_p50_us << " p99=" << latency_p99_us << " max=" << latency_max_us
      << "\n";
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
        std::chrono::duration_cast<std::chrono::microseconds>(op_end - op_start).count();
    latencies.push_back(static_cast<double>(latency_us));

    if (result.success) {
      ++success_count;
    } else {
      ++failure_count;
    }

    ++op_count;

    if (config_.progress_interval > 0 && op_count % config_.progress_interval == 0) {
      std::cout << "Progress: " << op_count << " operations\r" << std::flush;
    }
  }

  if (config_.progress_interval > 0) {
    std::cout << std::endl;
  }

  auto actual_end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(actual_end - start_time);

  TearDown();

  // Calculate statistics
  BenchmarkStats stats;
  stats.total_operations = op_count;
  stats.success_count = success_count;
  stats.failure_count = failure_count;
  stats.success_rate = (op_count > 0) ? static_cast<double>(success_count) / op_count : 0.0;
  stats.duration_ms = duration;
  stats.operations_per_second =
      (duration.count() > 0) ? (static_cast<double>(op_count) * 1000.0) / duration.count() : 0.0;

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
      size_t idx = static_cast<size_t>(std::ceil((p / 100.0) * latencies.size())) - 1;
      if (idx >= latencies.size()) {
        idx = latencies.size() - 1;
      }
      return latencies[idx];
    };

    stats.latency_p50_us = percentile(50.0);
    stats.latency_p99_us = percentile(99.0);
    stats.latency_p999_us = percentile(99.9);
  }

  return stats;
}

// ========== ThroughputBenchmark ==========

ThroughputBenchmark::ThroughputBenchmark(const BenchmarkConfig& config,
                                         std::function<OperationResult()> operation)
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
    std::cout << "Testing at target throughput: " << target << " ops/sec" << std::endl;

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
  while (std::chrono::steady_clock::now() - steady_start < std::chrono::seconds(5)) {
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
                  << std::chrono::duration_cast<std::chrono::milliseconds>(detection_time -
                                                                           kill_time)
                         .count()
                  << " ms" << std::endl;
      }
    } else {
      if (detected && !recovered) {
        // Success after failure indicates recovery
        recovered = true;
        auto recovery_time = std::chrono::steady_clock::now();

        result.detection_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(detection_time - kill_time);
        result.election_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(recovery_time - detection_time);
        result.recovery_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(recovery_time - kill_time);
        result.operations_during_failover = ops_during_failover;
        result.operations_failed = ops_failed;

        std::cout << "Failover complete!" << std::endl;
        std::cout << "  Detection time: " << result.detection_time.count() << " ms" << std::endl;
        std::cout << "  Election time: " << result.election_time.count() << " ms" << std::endl;
        std::cout << "  Total recovery: " << result.recovery_time.count() << " ms" << std::endl;

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

  std::cout << std::left << std::setw(10) << "Run" << std::setw(15) << "Throughput" << std::setw(12)
            << "Success%" << std::setw(12) << "P50 Lat" << std::setw(12) << "P99 Lat"
            << std::setw(12) << "Max Lat" << std::endl;
  std::cout << std::string(73, '-') << std::endl;

  for (size_t i = 0; i < results.size(); ++i) {
    const auto& s = results[i];
    std::cout << std::left << std::setw(10) << (i + 1) << std::setw(15)
              << static_cast<int>(s.operations_per_second) << std::setw(12) << std::fixed
              << std::setprecision(1) << (s.success_rate * 100) << std::setw(12)
              << static_cast<int>(s.latency_p50_us) << std::setw(12)
              << static_cast<int>(s.latency_p99_us) << std::setw(12)
              << static_cast<int>(s.latency_max_us) << std::endl;
  }
}

void PrintComparison(const std::map<std::string, BenchmarkStats>& results) {
  std::cout << "\n========== Comparison ==========\n" << std::endl;

  std::cout << std::left << std::setw(20) << "Benchmark" << std::setw(15) << "Throughput"
            << std::setw(12) << "P50 Lat" << std::setw(12) << "P99 Lat" << std::endl;
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
  if (!file.is_open()) {
    return;
  }

  file << "{\n";
  file << "  \"throughput_ops_per_sec\": " << stats.operations_per_second << ",\n";
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

// ========== RepeatedBenchmarkStats ==========

RepeatedBenchmarkStats RunRepeated(Benchmark* benchmark, int repetitions) {
  RepeatedBenchmarkStats result;
  result.repetition_count = repetitions;
  result.runs.reserve(repetitions);

  for (int i = 0; i < repetitions; ++i) {
    std::cout << "Run " << (i + 1) << "/" << repetitions << "..." << std::endl;
    auto stats = benchmark->Run();
    result.runs.push_back(stats);
  }

  if (result.runs.empty()) {
    return result;
  }

  // Calculate mean
  auto& mean = result.mean;
  mean.total_operations = 0;
  mean.success_count = 0;
  mean.failure_count = 0;
  mean.operations_per_second = 0.0;
  mean.latency_min_us = 0.0;
  mean.latency_max_us = 0.0;
  mean.latency_avg_us = 0.0;
  mean.latency_p50_us = 0.0;
  mean.latency_p99_us = 0.0;
  mean.latency_p999_us = 0.0;
  mean.success_rate = 0.0;
  mean.duration_ms = result.runs[0].duration_ms;

  for (const auto& run : result.runs) {
    mean.total_operations += run.total_operations;
    mean.success_count += run.success_count;
    mean.failure_count += run.failure_count;
    mean.operations_per_second += run.operations_per_second;
    mean.latency_min_us += run.latency_min_us;
    mean.latency_max_us += run.latency_max_us;
    mean.latency_avg_us += run.latency_avg_us;
    mean.latency_p50_us += run.latency_p50_us;
    mean.latency_p99_us += run.latency_p99_us;
    mean.latency_p999_us += run.latency_p999_us;
    mean.success_rate += run.success_rate;
  }

  double n = static_cast<double>(result.runs.size());
  mean.total_operations = static_cast<uint64_t>(mean.total_operations / n);
  mean.success_count = static_cast<uint64_t>(mean.success_count / n);
  mean.failure_count = static_cast<uint64_t>(mean.failure_count / n);
  mean.operations_per_second /= n;
  mean.latency_min_us /= n;
  mean.latency_max_us /= n;
  mean.latency_avg_us /= n;
  mean.latency_p50_us /= n;
  mean.latency_p99_us /= n;
  mean.latency_p999_us /= n;
  mean.success_rate /= n;

  // Calculate stddev
  auto& stddev = result.stddev;
  auto calc_stddev = [&](double mean_val, std::function<double(const BenchmarkStats&)> getter) {
    double sum_sq = 0.0;
    for (const auto& run : result.runs) {
      double diff = getter(run) - mean_val;
      sum_sq += diff * diff;
    }
    return std::sqrt(sum_sq / n);
  };

  stddev.operations_per_second = calc_stddev(
      mean.operations_per_second, [](const BenchmarkStats& s) { return s.operations_per_second; });
  stddev.latency_p50_us =
      calc_stddev(mean.latency_p50_us, [](const BenchmarkStats& s) { return s.latency_p50_us; });
  stddev.latency_p99_us =
      calc_stddev(mean.latency_p99_us, [](const BenchmarkStats& s) { return s.latency_p99_us; });
  stddev.latency_p999_us =
      calc_stddev(mean.latency_p999_us, [](const BenchmarkStats& s) { return s.latency_p999_us; });
  stddev.success_rate =
      calc_stddev(mean.success_rate, [](const BenchmarkStats& s) { return s.success_rate; });

  return result;
}

std::string RepeatedBenchmarkStats::ToString() const {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(2);
  oss << "Repeated Benchmark Results (" << repetition_count << " runs)\n";
  oss << "========================================\n";
  oss << "Throughput: " << mean.operations_per_second << " ± " << stddev.operations_per_second
      << " ops/sec\n";
  oss << "Success Rate: " << (mean.success_rate * 100.0) << " ± " << (stddev.success_rate * 100.0)
      << "%\n";
  oss << "Latency P50: " << mean.latency_p50_us << " ± " << stddev.latency_p50_us << " us\n";
  oss << "Latency P99: " << mean.latency_p99_us << " ± " << stddev.latency_p99_us << " us\n";
  oss << "Latency P999: " << mean.latency_p999_us << " ± " << stddev.latency_p999_us << " us\n";
  return oss.str();
}

void RepeatedBenchmarkStats::SaveToJson(
    const std::string& filename, const std::string& benchmark_name,
    const std::map<std::string, std::string>& parameters) const {
  benchmark::SaveToJson(filename, *this, benchmark_name, parameters);
}

// ========== Enhanced JSON (Schema v1.0) ==========

namespace benchmark {

void SaveToJson(const std::string& filename, const RepeatedBenchmarkStats& stats,
                const std::string& benchmark_name,
                const std::map<std::string, std::string>& parameters) {
  std::ofstream file(filename);
  if (!file.is_open()) {
    return;
  }

  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  std::tm tm = *std::gmtime(&time_t);

  file << "{\n";
  file << "  \"schema_version\": \"1.0\",\n";
  file << "  \"benchmark_name\": \"" << benchmark_name << "\",\n";
  file << "  \"timestamp\": \"";
  file << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
  file << "\",\n";

  // Parameters
  file << "  \"parameters\": {\n";
  bool first = true;
  for (const auto& [key, value] : parameters) {
    if (!first) {
      file << ",\n";
    }
    file << "    \"" << key << "\": " << value;
    first = false;
  }
  file << "\n  },\n";

  // Statistics
  file << "  \"statistics\": {\n";
  file << "    \"repetitions\": " << stats.repetition_count << ",\n";

  // Throughput
  file << "    \"throughput_ops_per_sec\": {\n";
  file << "      \"mean\": " << stats.mean.operations_per_second << ",\n";
  file << "      \"stddev\": " << stats.stddev.operations_per_second << ",\n";
  file << "      \"min\": " << stats.mean.operations_per_second - stats.stddev.operations_per_second
       << ",\n";
  file << "      \"max\": " << stats.mean.operations_per_second + stats.stddev.operations_per_second
       << "\n";
  file << "    },\n";

  // Latency
  file << "    \"latency_us\": {\n";
  file << "      \"p50\": { \"mean\": " << stats.mean.latency_p50_us
       << ", \"stddev\": " << stats.stddev.latency_p50_us << " },\n";
  file << "      \"p99\": { \"mean\": " << stats.mean.latency_p99_us
       << ", \"stddev\": " << stats.stddev.latency_p99_us << " },\n";
  file << "      \"p999\": { \"mean\": " << stats.mean.latency_p999_us
       << ", \"stddev\": " << stats.stddev.latency_p999_us << " }\n";
  file << "    },\n";

  // Success rate
  file << "    \"success_rate\": {\n";
  file << "      \"mean\": " << stats.mean.success_rate << ",\n";
  file << "      \"stddev\": " << stats.stddev.success_rate << "\n";
  file << "    }\n";
  file << "  },\n";

  // Individual runs
  file << "  \"runs\": [\n";
  for (size_t i = 0; i < stats.runs.size(); ++i) {
    const auto& run = stats.runs[i];
    file << "    {\n";
    file << "      \"run_id\": " << (i + 1) << ",\n";
    file << "      \"throughput\": " << run.operations_per_second << ",\n";
    file << "      \"latency_p50\": " << run.latency_p50_us << ",\n";
    file << "      \"latency_p99\": " << run.latency_p99_us << ",\n";
    file << "      \"latency_p999\": " << run.latency_p999_us << ",\n";
    file << "      \"success_rate\": " << run.success_rate << ",\n";
    file << "      \"total_operations\": " << run.total_operations << "\n";
    file << "    }";
    if (i + 1 < stats.runs.size()) {
      file << ",";
    }
    file << "\n";
  }
  file << "  ]\n";
  file << "}\n";
}

}  // namespace benchmark

}  // namespace rollingraft
