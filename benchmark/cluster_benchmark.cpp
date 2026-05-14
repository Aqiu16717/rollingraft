/**
 * @file cluster_benchmark.cpp
 * @brief Self-contained cluster benchmark implementation
 */

#include "cluster_benchmark.h"

#include <filesystem>
#include <iostream>
#include <thread>

namespace rollingraft {

ClusterBenchmark::ClusterBenchmark(const BenchmarkConfig& config,
                                   const BenchmarkClusterConfig& cluster_config)
    : Benchmark(config), cluster_config_(cluster_config) {}

ClusterBenchmark::~ClusterBenchmark() {
  TearDown();
}

bool ClusterBenchmark::SetUp() {
  std::cerr << "[BENCH] SetUp: starting cluster..." << std::endl;
  // Create temp data directories
  data_dirs_.clear();
  for (size_t i = 0; i < cluster_config_.num_nodes; ++i) {
    std::string dir = "/tmp/raft_benchmark_node_" + std::to_string(i + 1) +
                      "_" + std::to_string(
                          std::chrono::steady_clock::now().time_since_epoch() /
                          std::chrono::milliseconds(1));
    data_dirs_.push_back(dir);
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
  }

  // Allocate ephemeral ports
  auto ports = AllocateEphemeralPorts(cluster_config_.num_nodes);
  addrs_ = FormatAddrs(ports);

  // Create nodes
  nodes_.clear();
  state_machines_.clear();

  for (size_t i = 0; i < cluster_config_.num_nodes; ++i) {
    auto config = MakeConfig(static_cast<NodeId>(i + 1), addrs_[i], addrs_);
    auto sm = std::make_shared<MockStateMachine>();
    state_machines_.push_back(sm);

    nodes_.push_back(std::make_unique<RaftNode>(config, sm));
    auto status = nodes_[i]->Start();
    if (!status.ok()) {
      std::cerr << "Failed to start node " << (i + 1) << ": "
                << status.ToString() << std::endl;
      return false;
    }
  }

  // Wait for leader election
  if (!WaitForLeader(std::chrono::seconds(5))) {
    std::cerr << "[BENCH] Failed to elect leader within timeout" << std::endl;
    return false;
  }
  std::cerr << "[BENCH] Leader elected: node " << (GetLeaderIndex() + 1)
            << std::endl;

  // Give cluster time to stabilize heartbeat before benchmark load
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  std::cerr << "[BENCH] SetUp complete." << std::endl;

  return true;
}

void ClusterBenchmark::TearDown() {
  // Stop all nodes
  for (auto& node : nodes_) {
    if (node) {
      try {
        node->Stop();
      } catch (...) {
        // Ignore errors during cleanup
      }
    }
  }
  nodes_.clear();
  state_machines_.clear();

  // Small delay for resources to settle
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Clean up data directories
  for (const auto& dir : data_dirs_) {
    std::filesystem::remove_all(dir);
  }
  data_dirs_.clear();
}

bool ClusterBenchmark::WaitForLeader(std::chrono::seconds timeout) {
  auto start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start < timeout) {
    for (size_t i = 0; i < nodes_.size(); ++i) {
      if (nodes_[i] && nodes_[i]->IsLeader()) {
        return true;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;
}

int ClusterBenchmark::GetLeaderIndex() const {
  for (size_t i = 0; i < nodes_.size(); ++i) {
    if (nodes_[i] && nodes_[i]->IsLeader()) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

Status ClusterBenchmark::ProposeToLeader(const std::string& command) {
  int leader_idx = GetLeaderIndex();
  if (leader_idx < 0) {
    std::cerr << "[BENCH] No leader!" << std::endl;
    return Status::Error("No leader elected");
  }

  // Propose is async callback-based; block until callback fires
  std::atomic<bool> done{false};
  std::atomic<bool> success{false};
  std::string error_msg;

  auto status = nodes_[leader_idx]->Propose(
      command, [&](const ApplyResult& result) {
        success.store(result.success, std::memory_order_release);
        if (!result.success) {
          error_msg = result.error_message;
        }
        done.store(true, std::memory_order_release);
      });

  if (!status.ok()) {
    std::cerr << "[BENCH] Propose rejected: " << status.ToString()
              << std::endl;
    return Status::Error("Propose rejected: " + status.ToString());
  }

  auto wait_start = std::chrono::steady_clock::now();
  int dots = 0;
  while (!done.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // If leader changed, proposal may be lost; return error for retry
    if (GetLeaderIndex() != leader_idx) {
      std::cerr << "[BENCH] Leader changed!" << std::endl;
      return Status::Error("Leader changed during proposal");
    }

    if (++dots % 100 == 0) {
      std::cerr << "." << std::flush;
    }

    if (std::chrono::steady_clock::now() - wait_start >
        std::chrono::seconds(5)) {
      std::cerr << "[BENCH] TIMEOUT" << std::endl;
      return Status::Error("Timeout waiting for commit");
    }
  }

  if (!success.load(std::memory_order_acquire)) {
    std::cerr << "[BENCH] Failed: " << error_msg << std::endl;
    return Status::Error("Propose failed: " + error_msg);
  }
  return Status::OK();
}

Status ClusterBenchmark::ExecuteCommand(const std::string& command) {
  // Try up to 3 times with leader rediscovery
  for (int attempt = 0; attempt < 3; ++attempt) {
    auto status = ProposeToLeader(command);
    if (status.ok()) {
      return status;
    }

    // Wait a bit and retry (leader may have changed)
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    WaitForLeader(std::chrono::seconds(10));
  }

  return Status::Error("Failed to execute command after 3 attempts");
}

std::string ClusterBenchmark::GetLeaderAddr() const {
  int idx = GetLeaderIndex();
  if (idx >= 0 && static_cast<size_t>(idx) < addrs_.size()) {
    return addrs_[idx];
  }
  return "";
}

void ClusterBenchmark::StopNode(size_t index) {
  if (index < nodes_.size() && nodes_[index]) {
    nodes_[index]->Stop();
    nodes_[index].reset();  // Clear pointer so GetLeaderIndex skips it
  }
}

Status ClusterBenchmark::RestartNode(size_t index) {
  if (index >= nodes_.size()) {
    return Status::Error("Invalid node index");
  }

  auto config =
      MakeConfig(static_cast<NodeId>(index + 1), addrs_[index], addrs_);
  auto sm = std::make_shared<MockStateMachine>();
  state_machines_[index] = sm;

  nodes_[index] = std::make_unique<RaftNode>(config, sm);
  return nodes_[index]->Start();
}

RaftNodeConfig ClusterBenchmark::MakeConfig(NodeId id,
                                            const std::string& addr,
                                            const std::vector<std::string>&
                                                all_addrs) {
  RaftNodeConfig config;
  config.node_id = id;
  config.listen_addr = addr;
  config.data_dir = data_dirs_[id - 1];
  config.election_timeout_ms =
      static_cast<int>(cluster_config_.election_timeout.count());
  config.heartbeat_interval_ms =
      static_cast<int>(cluster_config_.heartbeat_interval.count());
  config.snapshot_threshold_entries = cluster_config_.snapshot_threshold_entries;

  for (const auto& peer_addr : all_addrs) {
    if (peer_addr != addr) {
      config.peers.push_back(peer_addr);
    }
  }

  return config;
}

}  // namespace rollingraft
