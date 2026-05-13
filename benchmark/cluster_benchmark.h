/**
 * @file cluster_benchmark.h
 * @brief Self-contained cluster benchmark base class
 *
 * Spins up in-process RaftNode instances with ephemeral ports,
 * runs benchmarks, and tears down automatically. CI-friendly.
 */

#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "benchmark.h"

// Include ephemeral port allocator from tests
#include "tests/integration/ephemeral_port.h"
#include "tests/mock/mock_state_machine.h"

#include "rollingraft/raft_node.h"

namespace rollingraft {

/**
 * Configuration for the benchmark cluster.
 */
struct ClusterConfig {
  size_t num_nodes = 3;
  std::chrono::milliseconds election_timeout{300};
  std::chrono::milliseconds heartbeat_interval{50};
  size_t snapshot_threshold_entries = 0;  // 0 = disabled
  std::chrono::milliseconds snapshot_interval{0};  // 0 = disabled
};

/**
 * Base class for benchmarks that need a self-contained Raft cluster.
 */
class ClusterBenchmark : public Benchmark {
 public:
  explicit ClusterBenchmark(const BenchmarkConfig& config,
                            const ClusterConfig& cluster_config = {});
  ~ClusterBenchmark() override;

 protected:
  bool SetUp() override;
  void TearDown() override;

  // Wait for leader election (with timeout)
  bool WaitForLeader(std::chrono::seconds timeout = std::chrono::seconds(5));

  // Find current leader node index, or -1 if none
  int GetLeaderIndex() const;

  // Propose a command to the current leader
  Status ProposeToLeader(const std::string& command);

  // Execute a command via the client-like interface (auto-retry on redirect)
  Status ExecuteCommand(const std::string& command);

  // Get the address of the leader
  std::string GetLeaderAddr() const;

  // Stop a specific node (for failover/transfer scenarios)
  void StopNode(size_t index);

  // Restart a specific node (for recovery scenarios)
  Status RestartNode(size_t index);

 private:
  RaftNodeConfig MakeConfig(NodeId id, const std::string& addr,
                            const std::vector<std::string>& all_addrs);

  ClusterConfig cluster_config_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
  std::vector<std::shared_ptr<MockStateMachine>> state_machines_;
  std::vector<std::string> addrs_;
  std::vector<std::string> data_dirs_;
};

}  // namespace rollingraft
