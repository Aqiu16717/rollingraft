/**
 * @file benchmark.h
 * @brief Performance benchmarking framework for RollingRaft
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>
#include <functional>
#include <map>

namespace rollingraft {

/**
 * Statistics collected during a benchmark run.
 */
struct BenchmarkStats {
  // Throughput metrics
  uint64_t total_operations = 0;
  double operations_per_second = 0.0;
  
  // Latency metrics (in microseconds)
  double latency_min_us = 0.0;
  double latency_max_us = 0.0;
  double latency_avg_us = 0.0;
  double latency_p50_us = 0.0;
  double latency_p99_us = 0.0;
  double latency_p999_us = 0.0;
  
  // Error metrics
  uint64_t success_count = 0;
  uint64_t failure_count = 0;
  double success_rate = 0.0;
  
  // Duration
  std::chrono::milliseconds duration_ms{0};
  
  std::string ToString() const;
};

/**
 * Configuration for benchmark runs.
 */
struct BenchmarkConfig {
  // Test duration
  std::chrono::seconds duration{10};
  
  // Number of concurrent clients
  int num_clients = 1;
  
  // Operations to perform
  enum class OperationType {
    kWrite,      // Propose commands
    kRead,       // ReadIndex queries
    kMixed       // 80% write, 20% read
  };
  OperationType operation_type = OperationType::kWrite;
  
  // Payload size in bytes
  size_t payload_size = 100;
  
  // Print progress every N operations (0 to disable)
  uint64_t progress_interval = 0;
};

/**
 * Benchmark result for a single operation.
 */
struct OperationResult {
  bool success = false;
  std::chrono::microseconds latency{0};
  std::string error_message;
};

/**
 * Base class for benchmarks.
 */
class Benchmark {
 public:
  explicit Benchmark(const BenchmarkConfig& config);
  virtual ~Benchmark() = default;
  
  // Run the benchmark and return statistics
  BenchmarkStats Run();
  
  // Get the name of this benchmark
  virtual std::string Name() const = 0;
  
 protected:
  // Override this to perform setup before benchmark
  virtual bool SetUp() { return true; }
  
  // Override this to perform a single operation
  virtual OperationResult DoOperation() = 0;
  
  // Override this to perform cleanup after benchmark
  virtual void TearDown() {}
  
  BenchmarkConfig config_;
};

/**
 * Throughput benchmark - measures ops/sec under load.
 */
class ThroughputBenchmark : public Benchmark {
 public:
  ThroughputBenchmark(const BenchmarkConfig& config,
                      std::function<OperationResult()> operation);
  
  std::string Name() const override { return "Throughput"; }
  
 protected:
  OperationResult DoOperation() override;
  
 private:
  std::function<OperationResult()> operation_;
};

/**
 * Latency benchmark - measures P50/P99 latency at various throughputs.
 */
class LatencyBenchmark : public Benchmark {
 public:
  LatencyBenchmark(const BenchmarkConfig& config,
                   std::function<OperationResult()> operation);
  
  std::string Name() const override { return "Latency"; }
  
  // Run at different target throughputs and collect latency curves
  std::map<int, BenchmarkStats> RunLatencyCurve();
  
 protected:
  OperationResult DoOperation() override;
  
 private:
  std::function<OperationResult()> operation_;
};

/**
 * Failover benchmark - measures recovery time after leader failure.
 */
class FailoverBenchmark : public Benchmark {
 public:
  struct FailoverResult {
    std::chrono::milliseconds detection_time;    // Time to detect leader failure
    std::chrono::milliseconds election_time;     // Time to elect new leader
    std::chrono::milliseconds recovery_time;     // Total recovery time
    uint64_t operations_during_failover = 0;     // Ops completed during failover
    uint64_t operations_failed = 0;              // Ops failed during failover
  };
  
  FailoverBenchmark(std::function<OperationResult()> operation,
                    std::function<void()> kill_leader,
                    std::function<bool()> is_leader_elected);
  
  std::string Name() const override { return "Failover"; }
  
  // Run single failover test
  FailoverResult RunFailover();
  
 protected:
  OperationResult DoOperation() override;
  
 private:
  std::function<OperationResult()> operation_;
  std::function<void()> kill_leader_;
  std::function<bool()> is_leader_elected_;
};

/**
 * Utility functions for benchmark output.
 */
namespace benchmark {

// Print benchmark results in a formatted table
void PrintResults(const std::vector<BenchmarkStats>& results);

// Compare multiple benchmark runs
void PrintComparison(const std::map<std::string, BenchmarkStats>& results);

// Save results to JSON file
void SaveToJson(const std::string& filename, const BenchmarkStats& stats);

}  // namespace benchmark

}  // namespace rollingraft
