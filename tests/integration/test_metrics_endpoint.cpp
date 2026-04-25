#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "rollingraft/raft_node.h"
#include "mock/mock_state_machine.h"

using namespace rollingraft;

class MetricsEndpointTest : public ::testing::Test {
 protected:
  void SetUp() override {
    data_dirs_ = {
        "/tmp/raft_metrics_node_1",
        "/tmp/raft_metrics_node_2",
        "/tmp/raft_metrics_node_3"};

    for (const auto& dir : data_dirs_) {
      std::filesystem::remove_all(dir);
      std::filesystem::create_directories(dir);
    }
  }

  void TearDown() override {
    for (auto& node : nodes_) {
      if (node) {
        try {
          node->Stop();
        } catch (...) {
        }
      }
    }
    nodes_.clear();

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    for (const auto& dir : data_dirs_) {
      std::filesystem::remove_all(dir);
    }
  }

  void StartCluster() {
    std::vector<std::string> addrs = {
        "127.0.0.1:19101", "127.0.0.1:19102", "127.0.0.1:19103"};

    for (int i = 0; i < 3; ++i) {
      auto config = MakeConfig(i + 1, addrs[i], addrs);
      auto sm = std::make_shared<MockStateMachine>();
      state_machines_.push_back(sm);
      nodes_.push_back(std::make_unique<RaftNode>(config, sm));
      auto status = nodes_[i]->Start();
      EXPECT_TRUE(status.ok()) << "Failed to start node " << (i + 1) << ": " << status.ToString();
    }
  }

  RaftNodeConfig MakeConfig(NodeId id, const std::string& addr,
                            const std::vector<std::string>& all_addrs) {
    RaftNodeConfig config;
    config.node_id = id;
    config.listen_addr = addr;
    config.data_dir = data_dirs_[id - 1];
    config.election_timeout_ms = 300;
    config.heartbeat_interval_ms = 50;
    config.rpc_timeout_ms = 200;
    config.base_retry_delay_ms = 5;
    config.max_retry_delay_ms = 100;
    config.max_retry_attempts = 10;
    config.metrics_enabled = true;
    config.metrics_addr = "127.0.0.1:" + std::to_string(9200 + id);

    for (const auto& peer_addr : all_addrs) {
      if (peer_addr != addr) {
        config.peers.push_back(peer_addr);
      }
    }
    return config;
  }

  RaftNode* GetLeader(int timeout_sec = 5) {
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::steady_clock::now() - start)
               .count() < timeout_sec) {
      for (auto& node : nodes_) {
        if (node->IsLeader()) {
          return node.get();
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return nullptr;
  }

  void WaitForLeader(int timeout_sec = 5) {
    ASSERT_NE(GetLeader(timeout_sec), nullptr) << "No leader elected";
  }

  std::string FetchMetrics(const std::string& addr) {
    std::string cmd = "curl -s --max-time 2 http://" + addr + "/metrics";
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) return "";
    char buffer[4096];
    std::string result;
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
      result += buffer;
    }
    pclose(pipe);
    return result;
  }

  std::vector<std::string> data_dirs_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
  std::vector<std::shared_ptr<MockStateMachine>> state_machines_;
};

TEST_F(MetricsEndpointTest, MetricsServerResponds) {
  StartCluster();
  WaitForLeader();

  // Give metrics server time to start
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  for (int i = 0; i < 3; ++i) {
    std::string addr = "127.0.0.1:" + std::to_string(9201 + i);
    std::string output = FetchMetrics(addr);
    EXPECT_FALSE(output.empty()) << "Node " << (i + 1) << " metrics empty";
    EXPECT_NE(output.find("# TYPE"), std::string::npos)
        << "Node " << (i + 1) << " missing TYPE header";
  }
}

TEST_F(MetricsEndpointTest, MetricsShowRoleAndTerm) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Find leader and follower outputs
  std::string leader_output;
  std::string follower_output;
  for (int i = 0; i < 3; ++i) {
    std::string addr = "127.0.0.1:" + std::to_string(9201 + i);
    std::string out = FetchMetrics(addr);
    if (nodes_[i]->IsLeader()) {
      leader_output = out;
    } else {
      follower_output = out;
    }
  }

  EXPECT_NE(leader_output.find("raft_role{node_id="), std::string::npos);
  EXPECT_NE(leader_output.find("raft_current_term{node_id="),
            std::string::npos);
}

TEST_F(MetricsEndpointTest, MetricsShowProposeCount) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);

  // Propose a command and wait for it to commit
  std::atomic<bool> done{false};
  leader->Propose("metrics_test_cmd", [&done](const ApplyResult& result) {
    done = result.success;
  });

  auto start = std::chrono::steady_clock::now();
  while (!done && std::chrono::duration_cast<std::chrono::seconds>(
                      std::chrono::steady_clock::now() - start)
                      .count() < 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // Check leader metrics
  int leader_idx = 0;
  for (int i = 0; i < 3; ++i) {
    if (nodes_[i]->IsLeader()) leader_idx = i;
  }
  std::string addr = "127.0.0.1:" + std::to_string(9201 + leader_idx);
  std::string output = FetchMetrics(addr);

  EXPECT_NE(output.find("raft_propose_total"), std::string::npos)
      << "Missing propose counter";
}
