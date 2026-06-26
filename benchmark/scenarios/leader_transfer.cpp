/**
 * @file leader_transfer.cpp
 * @brief Scenario E: Leader transfer benchmark
 */

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "../benchmark.h"
#include "../cluster_benchmark.h"
#include "../scenario_registry.h"

namespace rollingraft {

class LeaderTransferScenario : public ClusterBenchmark {
 public:
  LeaderTransferScenario()
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

  std::string Name() const override { return "leader_transfer"; }

 protected:
  bool SetUp() override {
    if (!ClusterBenchmark::SetUp()) return false;
    leader_idx_ = GetLeaderIndex();
    return leader_idx_ >= 0;
  }

  OperationResult DoOperation() override { return OperationResult{}; }

 public:
  BenchmarkStats Run() override {
    if (!SetUp()) {
      BenchmarkStats stats;
      stats.failure_count = 1;
      return stats;
    }

    std::cout << "Starting leader transfer benchmark..." << std::endl;

    // Run steady load for 5 seconds
    std::cout << "Running steady load for 5 seconds..." << std::endl;
    auto steady_start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - steady_start < std::chrono::seconds(5)) {
      auto status = ExecuteCommand("load");
      if (!status.ok()) {
        std::cout << "Operation failed during steady state: " << status.ToString() << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    // Stop the leader to simulate transfer
    std::cout << "\nStopping leader (node " << (leader_idx_ + 1) << ")..." << std::endl;
    auto transfer_start = std::chrono::steady_clock::now();
    StopNode(leader_idx_);

    // Measure downtime until new leader is elected
    bool new_leader_elected = false;
    auto max_wait = std::chrono::seconds(10);
    uint64_t ops_during_transfer = 0;
    uint64_t ops_failed = 0;

    std::cout << "Waiting for new leader..." << std::flush;

    while (std::chrono::steady_clock::now() - transfer_start < max_wait) {
      // Check if any remaining node is leader
      for (size_t i = 0; i < nodes_.size(); ++i) {
        if (i != static_cast<size_t>(leader_idx_) && nodes_[i] && nodes_[i]->IsLeader()) {
          new_leader_elected = true;
          break;
        }
      }

      if (new_leader_elected) break;

      // Try an operation to measure failure
      auto status = ExecuteCommand("load");
      ++ops_during_transfer;
      if (!status.ok()) {
        ++ops_failed;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      std::cout << "." << std::flush;
    }

    auto transfer_end = std::chrono::steady_clock::now();
    auto downtime =
        std::chrono::duration_cast<std::chrono::milliseconds>(transfer_end - transfer_start);

    std::cout << "\nTransfer complete!" << std::endl;
    std::cout << "Downtime: " << downtime.count() << " ms" << std::endl;

    TearDown();

    // Build stats
    BenchmarkStats stats;
    stats.total_operations = ops_during_transfer;
    stats.success_count = ops_during_transfer - ops_failed;
    stats.failure_count = ops_failed;
    stats.success_rate =
        (ops_during_transfer > 0)
            ? static_cast<double>(ops_during_transfer - ops_failed) / ops_during_transfer
            : 0.0;
    stats.duration_ms = downtime;
    stats.operations_per_second = downtime.count() > 0 ? 1000.0 / downtime.count() : 0.0;
    stats.latency_p50_us = static_cast<double>(downtime.count() * 1000);
    stats.latency_p99_us = static_cast<double>(downtime.count() * 1000);

    return stats;
  }

 private:
  int leader_idx_ = -1;
};

REGISTER_SCENARIO(leader_transfer, []() { return std::make_unique<LeaderTransferScenario>(); });

}  // namespace rollingraft
