#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "rollingraft/raft_node.h"
#include "mock/mock_state_machine.h"

using namespace rollingraft;

/**
 * 3-node cluster integration tests.
 *
 * These tests use real network connections to verify:
 * - Leader election
 * - Log replication
 * - Failover
 */

class Cluster3NodesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create temp data directories
    data_dirs_ = {
        "/tmp/raft_test_node_1",
        "/tmp/raft_test_node_2",
        "/tmp/raft_test_node_3"};

    for (const auto& dir : data_dirs_) {
      std::filesystem::remove_all(dir);
      std::filesystem::create_directories(dir);
    }

    nodes_.clear();
    state_machines_.clear();
  }

  void TearDown() override {
    // Stop all nodes
    for (auto& node : nodes_) {
      if (node) {
        node->Stop();
      }
    }

    // Clean up data directories
    for (const auto& dir : data_dirs_) {
      std::filesystem::remove_all(dir);
    }
  }

  void StartCluster() {
    std::vector<std::string> addrs = {
        "127.0.0.1:9001",
        "127.0.0.1:9002",
        "127.0.0.1:9003"};

    for (int i = 0; i < 3; ++i) {
      auto config = MakeConfig(i + 1, addrs[i], addrs);
      auto sm = std::make_shared<MockStateMachine>();
      state_machines_.push_back(sm);

      nodes_.push_back(std::make_unique<RaftNode>(config, sm));
      EXPECT_TRUE(nodes_[i]->Start().ok()) << "Failed to start node " << (i + 1);
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

  int CountLeaders() {
    int count = 0;
    for (auto& node : nodes_) {
      if (node->IsLeader()) ++count;
    }
    return count;
  }

  void WaitForLeader(int timeout_sec = 5) {
    ASSERT_NE(GetLeader(timeout_sec), nullptr) << "No leader elected";
  }

  std::vector<std::string> data_dirs_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
  std::vector<std::shared_ptr<MockStateMachine>> state_machines_;
};

TEST_F(Cluster3NodesTest, ElectsSingleLeader) {
  StartCluster();
  WaitForLeader();

  EXPECT_EQ(CountLeaders(), 1) << "Should have exactly one leader";
}

TEST_F(Cluster3NodesTest, AllNodesHaveSameTerm) {
  StartCluster();
  WaitForLeader();

  Term term = nodes_[0]->CurrentTerm();
  for (size_t i = 1; i < nodes_.size(); ++i) {
    EXPECT_EQ(nodes_[i]->CurrentTerm(), term)
        << "Node " << (i + 1) << " has different term";
  }
}

TEST_F(Cluster3NodesTest, LeaderCanPropose) {
  StartCluster();
  WaitForLeader();

  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);

  std::atomic<bool> completed{false};
  auto status = leader->Propose("test_command", [&](const ApplyResult& result) {
    completed = result.success;
  });

  EXPECT_TRUE(status.ok()) << "Propose failed: " << status.ToString();

  // Wait for commit (timeout 5s)
  auto start = std::chrono::steady_clock::now();
  while (!completed && std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::steady_clock::now() - start)
                              .count() < 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  EXPECT_TRUE(completed) << "Command was not committed";
}

TEST_F(Cluster3NodesTest, LogReplicatedToAllNodes) {
  StartCluster();
  WaitForLeader();

  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);

  // Propose a command
  std::atomic<bool> completed{false};
  leader->Propose("replicate_test", [&](const ApplyResult& result) {
    completed = result.success;
  });

  // Wait for commit
  auto start = std::chrono::steady_clock::now();
  while (!completed && std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::steady_clock::now() - start)
                              .count() < 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  ASSERT_TRUE(completed);

  // Wait a bit for replication
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Verify all state machines have the command
  for (size_t i = 0; i < state_machines_.size(); ++i) {
    auto commands = state_machines_[i]->GetAppliedCommands();
    bool found = false;
    for (const auto& cmd : commands) {
      if (cmd == "replicate_test") {
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found) << "Node " << (i + 1) << " missing the command";
  }
}

TEST_F(Cluster3NodesTest, RecoversAfterLeaderCrash) {
  StartCluster();
  WaitForLeader();

  auto* old_leader = GetLeader();
  ASSERT_NE(old_leader, nullptr);
  NodeId old_leader_id = 0;
  for (size_t i = 0; i < nodes_.size(); ++i) {
    if (nodes_[i].get() == old_leader) {
      old_leader_id = i + 1;
      break;
    }
  }

  // Propose some commands
  for (int i = 0; i < 3; ++i) {
    old_leader->Propose("cmd" + std::to_string(i), [](auto) {});
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Stop the leader
  for (size_t i = 0; i < nodes_.size(); ++i) {
    if (nodes_[i].get() == old_leader) {
      nodes_[i]->Stop();
      nodes_[i].reset();
      break;
    }
  }

  // Wait for new leader election
  auto start = std::chrono::steady_clock::now();
  RaftNode* new_leader = nullptr;
  while (std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::steady_clock::now() - start)
             .count() < 10) {
    for (auto& node : nodes_) {
      if (node && node->IsLeader()) {
        new_leader = node.get();
        break;
      }
    }
    if (new_leader && new_leader != old_leader) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  ASSERT_NE(new_leader, nullptr) << "No new leader elected";
  ASSERT_NE(new_leader, old_leader) << "Old leader still reporting as leader";

  // New leader should be able to propose
  std::atomic<bool> completed{false};
  auto status = new_leader->Propose("after_crash", [&](auto result) {
    completed = result.success;
  });

  EXPECT_TRUE(status.ok());

  // Wait for commit
  start = std::chrono::steady_clock::now();
  while (!completed && std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::steady_clock::now() - start)
                              .count() < 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  EXPECT_TRUE(completed) << "New leader could not commit command";
}

TEST_F(Cluster3NodesTest, FollowerRedirectsPropose) {
  StartCluster();
  WaitForLeader();

  // Find a follower
  RaftNode* follower = nullptr;
  for (auto& node : nodes_) {
    if (!node->IsLeader()) {
      follower = node.get();
      break;
    }
  }
  ASSERT_NE(follower, nullptr);

  // Propose to follower should fail
  auto status = follower->Propose("test", [](auto) {});
  EXPECT_FALSE(status.ok());
}

TEST_F(Cluster3NodesTest, NoSplitBrain) {
  StartCluster();
  WaitForLeader();

  // There should never be more than one leader at a time
  for (int i = 0; i < 10; ++i) {
    EXPECT_LE(CountLeaders(), 1) << "Split brain detected at iteration " << i;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
}
