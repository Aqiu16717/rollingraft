#include <chrono>
#include <filesystem>
#include <memory>
#include <regex>
#include <thread>
#include <vector>

#include "rollingraft/raft_node.h"

#include "ephemeral_port.h"
#include "mock/mock_state_machine.h"
#include <gtest/gtest.h>

using namespace rollingraft;

class MetricsEndpointTest : public ::testing::Test {
 protected:
  bool leader_lease_enabled_ = true;

  void SetUp() override {
    data_dirs_ = {"/tmp/raft_metrics_node_1", "/tmp/raft_metrics_node_2",
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
    auto ports = AllocateEphemeralPorts(6);
    raft_addrs_ = FormatAddrs({ports[0], ports[1], ports[2]});
    metrics_addrs_ = FormatAddrs({ports[3], ports[4], ports[5]});

    for (int i = 0; i < 3; ++i) {
      auto config = MakeConfig(i + 1, raft_addrs_[i], raft_addrs_, metrics_addrs_[i]);
      auto sm = std::make_shared<MockStateMachine>();
      state_machines_.push_back(sm);
      nodes_.push_back(std::make_unique<RaftNode>(config, sm));
      auto status = nodes_[i]->Start();
      EXPECT_TRUE(status.ok()) << "Failed to start node " << (i + 1) << ": " << status.ToString();
    }
  }

  RaftNodeConfig MakeConfig(NodeId id, const std::string& addr,
                            const std::vector<std::string>& all_addrs,
                            const std::string& metrics_addr) {
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
    config.metrics_addr = metrics_addr;
    config.leader_lease_enabled = leader_lease_enabled_;

    for (size_t j = 0; j < all_addrs.size(); ++j) {
      if (all_addrs[j] != addr) {
        config.peers.push_back(all_addrs[j]);
        config.peer_node_ids.push_back(static_cast<NodeId>(j + 1));
      }
    }
    return config;
  }

  RaftNode* GetLeader(int timeout_sec = 15) {
    auto start = std::chrono::steady_clock::now();
    while (
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start)
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

  void WaitForLeader(int timeout_sec = 15) {
    ASSERT_NE(GetLeader(timeout_sec), nullptr) << "No leader elected";
  }

  std::string FetchUrl(const std::string& addr, const std::string& path) {
    std::string cmd = "curl -s --max-time 2 http://" + addr + path;
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
      return "";
    }
    char buffer[4096];
    std::string result;
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
      result += buffer;
    }
    pclose(pipe);
    return result;
  }

  std::string FetchMetrics(const std::string& addr) { return FetchUrl(addr, "/metrics"); }

  std::string FetchHealthz(const std::string& addr) { return FetchUrl(addr, "/healthz"); }

  std::string FetchReadyz(const std::string& addr) { return FetchUrl(addr, "/readyz"); }

  std::string FetchStatus(const std::string& addr) { return FetchUrl(addr, "/v1/status"); }

  std::string PostUrl(const std::string& addr, const std::string& path,
                      const std::string& body = "") {
    std::string cmd = "curl -s --max-time 2 -X POST";
    if (!body.empty()) {
      cmd += " -H 'Content-Type: application/json' -d '" + body + "'";
    }
    cmd += " http://" + addr + path;
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
      return "";
    }
    char buffer[4096];
    std::string result;
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
      result += buffer;
    }
    pclose(pipe);
    return result;
  }

  std::vector<std::string> data_dirs_;
  std::vector<std::string> raft_addrs_;
  std::vector<std::string> metrics_addrs_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
  std::vector<std::shared_ptr<MockStateMachine>> state_machines_;
};

TEST_F(MetricsEndpointTest, MetricsServerResponds) {
  StartCluster();
  WaitForLeader();

  // Give metrics server time to start
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  for (int i = 0; i < 3; ++i) {
    std::string output = FetchMetrics(metrics_addrs_[i]);
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
    std::string out = FetchMetrics(metrics_addrs_[i]);
    if (nodes_[i]->IsLeader()) {
      leader_output = out;
    } else {
      follower_output = out;
    }
  }

  // Labels are sorted alphabetically (group_id before node_id), so use regex
  // to match regardless of label order.
  EXPECT_TRUE(
      std::regex_search(leader_output, std::regex("raft_role\\{[^}]*node_id=\"[^\"]+\"[^}]*\\}")))
      << "raft_role metric not found in leader output";
  EXPECT_TRUE(std::regex_search(leader_output,
                                std::regex("raft_current_term\\{[^}]*node_id=\"[^\"]+\"[^}]*\\}")))
      << "raft_current_term metric not found in leader output";
}

TEST_F(MetricsEndpointTest, MetricsShowProposeCount) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);

  // Propose a command and wait for it to commit
  std::atomic<bool> done{false};
  leader->Propose("metrics_test_cmd",
                  [&done](const ApplyResult& result) { done = result.success; });

  auto start = std::chrono::steady_clock::now();
  while (!done &&
         std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start)
                 .count() < 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // Check leader metrics
  int leader_idx = 0;
  for (int i = 0; i < 3; ++i) {
    if (nodes_[i]->IsLeader()) {
      leader_idx = i;
    }
  }
  std::string output = FetchMetrics(metrics_addrs_[leader_idx]);

  EXPECT_NE(output.find("raft_propose_total"), std::string::npos) << "Missing propose counter";
}

TEST_F(MetricsEndpointTest, HealthzReturnsAlive) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  for (int i = 0; i < 3; ++i) {
    std::string output = FetchHealthz(metrics_addrs_[i]);
    EXPECT_NE(output.find("\"status\":\"alive\""), std::string::npos)
        << "Node " << (i + 1) << " healthz unexpected: " << output;
  }
}

TEST_F(MetricsEndpointTest, LivezReturnsAlive) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  for (int i = 0; i < 3; ++i) {
    std::string output = FetchUrl(metrics_addrs_[i], "/livez");
    EXPECT_NE(output.find("\"status\":\"alive\""), std::string::npos)
        << "Node " << (i + 1) << " livez unexpected: " << output;
  }
}

TEST_F(MetricsEndpointTest, ReadyzLeaderReturnsReady) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);

  int leader_idx = -1;
  for (int i = 0; i < 3; ++i) {
    if (nodes_[i].get() == leader) {
      leader_idx = i;
    }
  }
  ASSERT_GE(leader_idx, 0);

  std::string output = FetchReadyz(metrics_addrs_[leader_idx]);
  EXPECT_NE(output.find("\"status\":\"ready\""), std::string::npos)
      << "Leader readyz unexpected: " << output;
}

TEST_F(MetricsEndpointTest, ReadyzFollowerWithLeaderReturnsReady) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Find a follower
  int follower_idx = -1;
  for (int i = 0; i < 3; ++i) {
    if (!nodes_[i]->IsLeader()) {
      follower_idx = i;
      break;
    }
  }
  ASSERT_GE(follower_idx, 0);

  std::string output = FetchReadyz(metrics_addrs_[follower_idx]);
  EXPECT_NE(output.find("\"status\":\"ready\""), std::string::npos)
      << "Follower readyz unexpected: " << output;
}

TEST_F(MetricsEndpointTest, StatusReturnsValidJson) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  for (int i = 0; i < 3; ++i) {
    std::string output = FetchStatus(metrics_addrs_[i]);
    EXPECT_NE(output.find("\"node_id\""), std::string::npos)
        << "Node " << (i + 1) << " status missing node_id";
    EXPECT_NE(output.find("\"role\""), std::string::npos)
        << "Node " << (i + 1) << " status missing role";
    EXPECT_NE(output.find("\"term\""), std::string::npos)
        << "Node " << (i + 1) << " status missing term";
    EXPECT_NE(output.find("\"leader_id\""), std::string::npos)
        << "Node " << (i + 1) << " status missing leader_id";
  }
}

TEST_F(MetricsEndpointTest, TriggerSnapshotOnLeader) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);

  int leader_idx = -1;
  for (int i = 0; i < 3; ++i) {
    if (nodes_[i].get() == leader) {
      leader_idx = i;
    }
  }
  ASSERT_GE(leader_idx, 0);

  std::string output = PostUrl(metrics_addrs_[leader_idx], "/v1/snapshot/trigger");
  EXPECT_NE(output.find("\"status\""), std::string::npos)
      << "Trigger snapshot response: " << output;
}

TEST_F(MetricsEndpointTest, TriggerSnapshotOnFollowerFails) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  int follower_idx = -1;
  for (int i = 0; i < 3; ++i) {
    if (!nodes_[i]->IsLeader()) {
      follower_idx = i;
      break;
    }
  }
  ASSERT_GE(follower_idx, 0);

  std::string output = PostUrl(metrics_addrs_[follower_idx], "/v1/snapshot/trigger");
  EXPECT_NE(output.find("\"error\""), std::string::npos)
      << "Follower should reject snapshot trigger: " << output;
}

TEST_F(MetricsEndpointTest, TransferLeadershipFromLeader) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);

  int leader_idx = -1;
  for (int i = 0; i < 3; ++i) {
    if (nodes_[i].get() == leader) {
      leader_idx = i;
    }
  }
  ASSERT_GE(leader_idx, 0);

  int follower_idx = -1;
  for (int i = 0; i < 3; ++i) {
    if (!nodes_[i]->IsLeader()) {
      follower_idx = i;
      break;
    }
  }
  ASSERT_GE(follower_idx, 0);

  std::string body = "{\"target_node_id\":" + std::to_string(follower_idx + 1) + "}";
  std::string output = PostUrl(metrics_addrs_[leader_idx], "/v1/leadership/transfer", body);
  EXPECT_NE(output.find("\"status\""), std::string::npos)
      << "Transfer leadership response: " << output;
}

TEST_F(MetricsEndpointTest, TransferLeadershipOnFollowerFails) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  int follower_idx = -1;
  for (int i = 0; i < 3; ++i) {
    if (!nodes_[i]->IsLeader()) {
      follower_idx = i;
      break;
    }
  }
  ASSERT_GE(follower_idx, 0);

  std::string body = "{\"target_node_id\":1}";
  std::string output = PostUrl(metrics_addrs_[follower_idx], "/v1/leadership/transfer", body);
  EXPECT_NE(output.find("\"error\""), std::string::npos)
      << "Follower should reject transfer: " << output;
}

TEST_F(MetricsEndpointTest, MetricsShowLatencyHistograms) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);

  // Propose a command
  std::atomic<bool> propose_done{false};
  leader->Propose("latency_test_cmd",
                  [&propose_done](const ApplyResult& result) { propose_done = result.success; });

  // Issue a ReadIndex
  std::atomic<bool> read_done{false};
  leader->ReadIndex([&read_done]() { read_done = true; });

  auto start = std::chrono::steady_clock::now();
  while ((!propose_done || !read_done) &&
         std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start)
                 .count() < 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  int leader_idx = 0;
  for (int i = 0; i < 3; ++i) {
    if (nodes_[i]->IsLeader()) {
      leader_idx = i;
    }
  }
  std::string output = FetchMetrics(metrics_addrs_[leader_idx]);

  EXPECT_NE(output.find("raft_proposal_latency_seconds_bucket"), std::string::npos)
      << "Missing proposal latency histogram";
  EXPECT_NE(output.find("raft_proposal_latency_seconds_sum"), std::string::npos);
  EXPECT_NE(output.find("raft_proposal_latency_seconds_count"), std::string::npos);

  EXPECT_NE(output.find("raft_readindex_latency_seconds_bucket"), std::string::npos)
      << "Missing readindex latency histogram";
  EXPECT_NE(output.find("raft_readindex_latency_seconds_sum"), std::string::npos);
  EXPECT_NE(output.find("raft_readindex_latency_seconds_count"), std::string::npos);
}

TEST_F(MetricsEndpointTest, MetricsShowHeartbeatCoalescing) {
  // Disable leader lease so ReadIndex broadcasts heartbeats and triggers
  // the coalescing counter. With lease read enabled, heartbeats are skipped.
  leader_lease_enabled_ = false;
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);

  // Issue multiple ReadIndex requests rapidly to trigger coalescing
  for (int i = 0; i < 5; ++i) {
    leader->ReadIndex([]() {});
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int leader_idx = 0;
  for (int i = 0; i < 3; ++i) {
    if (nodes_[i]->IsLeader()) {
      leader_idx = i;
    }
  }
  std::string output = FetchMetrics(metrics_addrs_[leader_idx]);

  EXPECT_NE(output.find("raft_heartbeat_coalesced_total"), std::string::npos)
      << "Missing heartbeat coalesced counter";
}

TEST_F(MetricsEndpointTest, MetricsShowTransportPeerState) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  int leader_idx = 0;
  for (int i = 0; i < 3; ++i) {
    if (nodes_[i]->IsLeader()) {
      leader_idx = i;
    }
  }
  std::string output = FetchMetrics(metrics_addrs_[leader_idx]);

  EXPECT_NE(output.find("transport_peer_state"), std::string::npos)
      << "Missing transport_peer_state gauge";
  EXPECT_NE(output.find("raft_transport_peer_connected"), std::string::npos)
      << "Missing raft_transport_peer_connected gauge";
}

TEST_F(MetricsEndpointTest, MetricsShowLeaderLeaseValid) {
  StartCluster();
  WaitForLeader();

  // Wait for the leader to collect quorum acks and establish a lease.
  auto start = std::chrono::steady_clock::now();
  bool found_valid = false;
  while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start)
             .count() < 3) {
    for (int i = 0; i < 3; ++i) {
      std::string out = FetchMetrics(metrics_addrs_[i]);
      if (out.find("raft_leader_lease_valid") != std::string::npos) {
        if (nodes_[i]->IsLeader() &&
            std::regex_search(out, std::regex("raft_leader_lease_valid\\{[^}]*node_id=\"" +
                                              std::to_string(i + 1) + "\"[^}]*\\} 1"))) {
          found_valid = true;
          break;
        }
      }
    }
    if (found_valid) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_TRUE(found_valid) << "Leader never reported raft_leader_lease_valid=1";
}

TEST_F(MetricsEndpointTest, MetricsShowPeerLag) {
  StartCluster();
  WaitForLeader();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);

  int leader_idx = -1;
  for (int i = 0; i < 3; ++i) {
    if (nodes_[i].get() == leader) {
      leader_idx = i;
    }
  }
  ASSERT_GE(leader_idx, 0);

  // Propose a handful of commands to create replication lag lines.
  for (int i = 0; i < 5; ++i) {
    std::atomic<bool> done{false};
    leader->Propose("peer_lag_cmd_" + std::to_string(i),
                    [&done](const ApplyResult& result) { done = result.success; });
    auto start = std::chrono::steady_clock::now();
    while (!done && std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now() - start)
                            .count() < 3) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  std::string output = FetchMetrics(metrics_addrs_[leader_idx]);
  EXPECT_NE(output.find("raft_transport_peer_lag_entries"), std::string::npos)
      << "Missing raft_transport_peer_lag_entries gauge";

  // Each follower should have a lag line.
  for (int i = 0; i < 3; ++i) {
    if (i == leader_idx) {
      continue;
    }
    std::string label = "node_id=\"" + std::to_string(leader_idx + 1) + "\",peer_id=\"" +
                        std::to_string(i + 1) + "\"";
    EXPECT_NE(output.find(label), std::string::npos)
        << "Missing peer lag line for peer " << (i + 1) << " in:\n"
        << output;
  }
}
