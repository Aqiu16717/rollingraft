/**
 * @file failover.cpp
 * @brief Scenario C: Leader failover recovery benchmark
 */

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "../benchmark.h"
#include "../cluster_benchmark.h"
#include "../scenario_registry.h"

namespace rollingraft {

class FailoverScenario : public ClusterBenchmark {
 public:
  FailoverScenario()
      : ClusterBenchmark(
            []() {
              BenchmarkConfig config;
              config.duration = std::chrono::seconds(30);
              return config;
            }(),
            []() {
              BenchmarkClusterConfig config;
              config.num_nodes = 3;
              config.election_timeout = std::chrono::milliseconds(300);
              config.heartbeat_interval = std::chrono::milliseconds(50);
              return config;
            }()) {}

  std::string Name() const override { return "failover"; }

 protected:
  bool SetUp() override {
    if (!ClusterBenchmark::SetUp()) return false;
    leader_idx_ = GetLeaderIndex();
    return leader_idx_ >= 0;
  }

  OperationResult DoOperation() override {
    // This scenario uses a custom Run() override, so DoOperation
    // is not called directly in the standard loop.
    static thread_local std::string payload(100, 'x');
    OperationResult result;
    auto status = ExecuteCommand(payload);
    result.success = status.ok();
    if (!result.success) {
      result.error_message = status.ToString();
    }
    return result;
  }

 public:
  // Custom run that measures failover
  BenchmarkStats Run() override {
    if (!SetUp()) {
      BenchmarkStats stats;
      stats.failure_count = 1;
      return stats;
    }

    std::cout << "Starting failover benchmark..." << std::endl;

    // Run steady load for 5 seconds
    std::cout << "Running steady load for 5 seconds..." << std::endl;
    auto steady_start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - steady_start <
           std::chrono::seconds(5)) {
      auto status = ExecuteCommand("load");
      if (!status.ok()) {
        std::cout << "Operation failed during steady state: "
                  << status.ToString() << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Kill the leader
    std::cout << "\nKilling leader (node " << (leader_idx_ + 1) << ")..."
              << std::endl;
    auto kill_time = std::chrono::steady_clock::now();
    StopNode(leader_idx_);

    // Continue operations and measure recovery
    auto detection_time = kill_time;
    bool detected = false;
    bool recovered = false;

    uint64_t ops_during_failover = 0;
    uint64_t ops_failed = 0;

    auto max_wait = std::chrono::seconds(10);
    auto failover_start = kill_time;

    std::cout << "Waiting for failover..." << std::flush;

    while (std::chrono::steady_clock::now() - failover_start < max_wait) {
      auto status = ProposeToLeader("load");
      ++ops_during_failover;

      if (!status.ok()) {
        ++ops_failed;
        if (!detected) {
          detection_time = std::chrono::steady_clock::now();
          detected = true;
          std::cout << "\nFailure detected after "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(
                           detection_time - kill_time)
                           .count()
                    << " ms" << std::endl;
        }
      } else {
        if (detected && !recovered) {
          recovered = true;
          auto recovery_time = std::chrono::steady_clock::now();

          detection_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
              detection_time - kill_time);
          election_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
              recovery_time - detection_time);
          recovery_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
              recovery_time - kill_time);

          std::cout << "Failover complete!" << std::endl;
          std::cout << "  Detection time: " << detection_ms_.count() << " ms"
                    << std::endl;
          std::cout << "  Election time: " << election_ms_.count() << " ms"
                    << std::endl;
          std::cout << "  Total recovery: " << recovery_ms_.count() << " ms"
                    << std::endl;
          break;
        }
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      std::cout << "." << std::flush;
    }

    if (!recovered) {
      std::cout << "\nFailover test timed out!" << std::endl;
      recovery_ms_ = max_wait;
    }

    TearDown();

    // Build stats
    BenchmarkStats stats;
    stats.total_operations = ops_during_failover;
    stats.success_count = ops_during_failover - ops_failed;
    stats.failure_count = ops_failed;
    stats.success_rate =
        (ops_during_failover > 0)
            ? static_cast<double>(ops_during_failover - ops_failed) /
                  ops_during_failover
            : 0.0;
    stats.duration_ms = recovery_ms_;
    // Use recovery time as the key metric (lower is better)
    stats.operations_per_second =
        recovery_ms_.count() > 0
            ? 1000.0 / recovery_ms_.count()  // inverse for comparison
            : 0.0;
    stats.latency_p50_us = static_cast<double>(detection_ms_.count() * 1000);
    stats.latency_p99_us = static_cast<double>(recovery_ms_.count() * 1000);

    return stats;
  }

 private:
  int leader_idx_ = -1;
  std::chrono::milliseconds detection_ms_{0};
  std::chrono::milliseconds election_ms_{0};
  std::chrono::milliseconds recovery_ms_{0};
};

REGISTER_SCENARIO(failover,
                  []() { return std::make_unique<FailoverScenario>(); });

}  // namespace rollingraft
