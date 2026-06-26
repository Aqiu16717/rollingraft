/**
 * @file latency_curve.cpp
 * @brief Scenario B: 3-node replication latency curve
 */

#include <chrono>
#include <cmath>
#include <iostream>
#include <map>
#include <string>

#include "../benchmark.h"
#include "../cluster_benchmark.h"
#include "../scenario_registry.h"

namespace rollingraft {

class LatencyCurveScenario : public ClusterBenchmark {
 public:
  LatencyCurveScenario()
      : ClusterBenchmark(
            []() {
              BenchmarkConfig config;
              config.duration = std::chrono::seconds(10);
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

  std::string Name() const override { return "latency_curve"; }

  // Override Run to test at multiple throughput levels
  BenchmarkStats Run() override {
    if (!SetUp()) {
      BenchmarkStats stats;
      stats.failure_count = 1;
      return stats;
    }

    // Warmup
    std::cout << "Warming up..." << std::endl;
    for (int i = 0; i < 100; ++i) {
      ExecuteCommand("warmup");
    }

    std::vector<int> target_throughputs = {100, 500, 1000, 2000, 5000};
    std::map<int, BenchmarkStats> results;

    for (int target : target_throughputs) {
      std::cout << "\nTesting at target throughput: " << target << " ops/sec" << std::endl;

      auto delay_between_ops = std::chrono::microseconds(1000000 / target);
      auto start_time = std::chrono::steady_clock::now();
      auto end_time = start_time + std::chrono::seconds(10);

      std::vector<double> latencies;
      latencies.reserve(target * 10);
      uint64_t success_count = 0;
      uint64_t failure_count = 0;
      uint64_t op_count = 0;
      auto last_op_time = std::chrono::steady_clock::now();

      while (std::chrono::steady_clock::now() < end_time) {
        // Rate limiting
        auto now = std::chrono::steady_clock::now();
        auto elapsed = now - last_op_time;
        if (elapsed < delay_between_ops) {
          std::this_thread::sleep_for(delay_between_ops - elapsed);
        }

        auto op_start = std::chrono::steady_clock::now();
        auto status = ExecuteCommand("payload");
        auto op_end = std::chrono::steady_clock::now();
        last_op_time = op_start;

        auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(op_end - op_start).count();
        latencies.push_back(static_cast<double>(latency_us));

        if (status.ok()) {
          ++success_count;
        } else {
          ++failure_count;
        }
        ++op_count;
      }

      auto actual_end = std::chrono::steady_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(actual_end - start_time);

      BenchmarkStats stats;
      stats.total_operations = op_count;
      stats.success_count = success_count;
      stats.failure_count = failure_count;
      stats.success_rate = (op_count > 0) ? static_cast<double>(success_count) / op_count : 0.0;
      stats.duration_ms = duration;
      stats.operations_per_second =
          (duration.count() > 0) ? (static_cast<double>(op_count) * 1000.0) / duration.count()
                                 : 0.0;

      if (!latencies.empty()) {
        std::sort(latencies.begin(), latencies.end());
        stats.latency_min_us = latencies.front();
        stats.latency_max_us = latencies.back();

        double sum = 0.0;
        for (double lat : latencies) sum += lat;
        stats.latency_avg_us = sum / latencies.size();

        auto percentile = [&](double p) -> double {
          size_t idx = static_cast<size_t>(std::ceil((p / 100.0) * latencies.size())) - 1;
          if (idx >= latencies.size()) idx = latencies.size() - 1;
          return latencies[idx];
        };
        stats.latency_p50_us = percentile(50.0);
        stats.latency_p99_us = percentile(99.0);
        stats.latency_p999_us = percentile(99.9);
      }

      std::cout << "  Actual throughput: " << stats.operations_per_second << " ops/sec"
                << std::endl;
      std::cout << "  P50 latency: " << stats.latency_p50_us << " us" << std::endl;
      std::cout << "  P99 latency: " << stats.latency_p99_us << " us" << std::endl;

      results[target] = stats;
    }

    TearDown();

    // Return stats for the highest throughput level as representative
    return results.empty() ? BenchmarkStats{} : results.rbegin()->second;
  }

 protected:
  OperationResult DoOperation() override {
    // Not used directly - overridden Run() handles the logic
    return OperationResult{};
  }
};

REGISTER_SCENARIO(latency_curve, []() { return std::make_unique<LatencyCurveScenario>(); });

}  // namespace rollingraft
