#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "rollingraft/logger.h"
#include "rollingraft/raft_node.h"

#include "ephemeral_port.h"
#include "mock/mock_state_machine.h"
#include "raft_store.h"
#include <gtest/gtest.h>

using namespace rollingraft;

/**
 * Multi-raft integration test: 3 nodes, 2 independent groups.
 *
 * Each physical node runs a RaftStore that hosts both group 1 and group 2.
 * The shared transport routes inbound RPCs by group_id, so the two groups
 * elect leaders independently.
 */

class MultiRaft2GroupsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Use process-scoped temp directories so multiple gtest filters / ctest
    // processes can run concurrently without deleting each other's state.
    std::string base = "/tmp/raft_test_store_pid" + std::to_string(getpid());
    data_dirs_ = {base + "_1", base + "_2", base + "_3"};

    for (const auto& dir : data_dirs_) {
      std::filesystem::remove_all(dir);
      std::filesystem::create_directories(dir);
    }

    stores_.clear();
    state_machines_.clear();
  }

  void TearDown() override {
    // Let in-flight heartbeats settle before tearing down shared transport.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    for (auto& store : stores_) {
      if (store) {
        try {
          store->Stop();
        } catch (...) {
          // Ignore errors during cleanup
        }
      }
    }
    stores_.clear();

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    for (const auto& dir : data_dirs_) {
      std::filesystem::remove_all(dir);
    }
  }

  void StartCluster() {
    // Use process-scoped fixed ports to avoid cross-talk when ctest runs
    // multiple integration test processes concurrently.  Each process gets a
    // block of 10 ports so neighbouring pids do not overlap.
    uint16_t base_port = 20000 + static_cast<uint16_t>((getpid() % 4000) * 10);
    std::vector<uint16_t> ports = {base_port, static_cast<uint16_t>(base_port + 1),
                                   static_cast<uint16_t>(base_port + 2)};
    addrs_ = FormatAddrs(ports);

    // Start one RaftStore per physical node.
    for (int i = 0; i < 3; ++i) {
      RaftStoreConfig store_config;
      store_config.node_id = i + 1;
      store_config.listen_addr = addrs_[i];
      store_config.data_dir = data_dirs_[i];
      for (size_t j = 0; j < addrs_.size(); ++j) {
        if (j != static_cast<size_t>(i)) {
          store_config.peers.push_back(addrs_[j]);
          store_config.peer_node_ids.push_back(static_cast<NodeId>(j + 1));
        }
      }

      auto store = std::make_unique<RaftStore>(store_config);
      auto status = store->Initialize();
      ASSERT_TRUE(status.ok()) << "RaftStore init failed: " << status.ToString();
      status = store->Start();
      ASSERT_TRUE(status.ok()) << "RaftStore start failed: " << status.ToString();
      stores_.push_back(std::move(store));
    }

    // Create group 1 and group 2 on every store.
    for (uint64_t group_id = 1; group_id <= 2; ++group_id) {
      std::vector<std::shared_ptr<MockStateMachine>> group_sms;
      for (int i = 0; i < 3; ++i) {
        RaftGroupOptions options;
        options.group_id = group_id;
        options.election_timeout_ms = 300;
        options.heartbeat_interval_ms = 50;

        auto sm = std::make_shared<MockStateMachine>();
        group_sms.push_back(sm);
        auto status = stores_[i]->CreateGroup(group_id, options, sm);
        ASSERT_TRUE(status.ok()) << "CreateGroup " << group_id << " on node " << (i + 1)
                                 << " failed: " << status.ToString();
      }
      state_machines_[group_id] = std::move(group_sms);
    }
  }

  RaftNode::RaftNodeImpl* GetLeader(uint64_t group_id, int timeout_sec = 15) {
    auto start = std::chrono::steady_clock::now();
    while (
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start)
            .count() < timeout_sec) {
      for (auto& store : stores_) {
        auto* group = store->GetGroup(group_id);
        if (group && group->IsLeader()) {
          return group;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return nullptr;
  }

  int CountLeaders(uint64_t group_id) {
    int count = 0;
    for (auto& store : stores_) {
      auto* group = store->GetGroup(group_id);
      if (group && group->IsLeader()) {
        ++count;
      }
    }
    return count;
  }

  void WaitForLeader(uint64_t group_id, int timeout_sec = 15) {
    ASSERT_NE(GetLeader(group_id, timeout_sec), nullptr)
        << "No leader elected for group " << group_id;
  }

  void StopStores() {
    for (auto& store : stores_) {
      if (store) {
        try {
          store->Stop();
        } catch (...) {
          // Ignore errors during cleanup
        }
      }
    }
    stores_.clear();
    state_machines_.clear();
  }

  void RestartCluster() {
    StopStores();

    // Small delay for resources to settle before restart.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Recreate stores on the same addresses and data directories.
    for (int i = 0; i < 3; ++i) {
      RaftStoreConfig store_config;
      store_config.node_id = i + 1;
      store_config.listen_addr = addrs_[i];
      store_config.data_dir = data_dirs_[i];
      for (size_t j = 0; j < addrs_.size(); ++j) {
        if (j != static_cast<size_t>(i)) {
          store_config.peers.push_back(addrs_[j]);
          store_config.peer_node_ids.push_back(static_cast<NodeId>(j + 1));
        }
      }

      auto store = std::make_unique<RaftStore>(store_config);
      auto status = store->Initialize();
      ASSERT_TRUE(status.ok()) << "RaftStore restart init failed: " << status.ToString();
      status = store->Start();
      ASSERT_TRUE(status.ok()) << "RaftStore restart start failed: " << status.ToString();
      stores_.push_back(std::move(store));
    }

    // Recreate groups with fresh state machines.
    for (uint64_t group_id = 1; group_id <= 2; ++group_id) {
      std::vector<std::shared_ptr<MockStateMachine>> group_sms;
      for (int i = 0; i < 3; ++i) {
        RaftGroupOptions options;
        options.group_id = group_id;
        options.election_timeout_ms = 300;
        options.heartbeat_interval_ms = 50;

        auto sm = std::make_shared<MockStateMachine>();
        group_sms.push_back(sm);
        auto status = stores_[i]->CreateGroup(group_id, options, sm);
        ASSERT_TRUE(status.ok()) << "Restart CreateGroup " << group_id << " on node " << (i + 1)
                                 << " failed: " << status.ToString();
      }
      state_machines_[group_id] = std::move(group_sms);
    }
  }

  std::vector<std::string> data_dirs_;
  std::vector<std::string> addrs_;
  std::vector<std::unique_ptr<RaftStore>> stores_;
  std::unordered_map<uint64_t, std::vector<std::shared_ptr<MockStateMachine>>> state_machines_;
};

TEST_F(MultiRaft2GroupsTest, BothGroupsElectIndependentLeaders) {
  StartCluster();

  WaitForLeader(1);
  WaitForLeader(2);

  EXPECT_EQ(CountLeaders(1), 1) << "Group 1 should have exactly one leader";
  EXPECT_EQ(CountLeaders(2), 1) << "Group 2 should have exactly one leader";
}

TEST_F(MultiRaft2GroupsTest, GroupsHaveSameTermAfterElection) {
  StartCluster();

  WaitForLeader(1);
  WaitForLeader(2);

  Term term1 = stores_[0]->GetGroup(1)->CurrentTerm();
  Term term2 = stores_[0]->GetGroup(2)->CurrentTerm();

  for (size_t i = 0; i < stores_.size(); ++i) {
    auto* g1 = stores_[i]->GetGroup(1);
    auto* g2 = stores_[i]->GetGroup(2);
    ASSERT_NE(g1, nullptr);
    ASSERT_NE(g2, nullptr);
    EXPECT_EQ(g1->CurrentTerm(), term1) << "Node " << (i + 1) << " group 1 has different term";
    EXPECT_EQ(g2->CurrentTerm(), term2) << "Node " << (i + 1) << " group 2 has different term";
  }
}

TEST_F(MultiRaft2GroupsTest, LeaderCanProposeInEachGroup) {
  StartCluster();

  WaitForLeader(1);
  WaitForLeader(2);

  for (uint64_t group_id = 1; group_id <= 2; ++group_id) {
    auto* leader = GetLeader(group_id);
    ASSERT_NE(leader, nullptr);

    std::atomic<bool> completed{false};
    auto status = leader->Propose("group_" + std::to_string(group_id) + "_cmd",
                                  [&](const ApplyResult& result) { completed = result.success; });
    ASSERT_TRUE(status.ok()) << "Group " << group_id << " propose failed: " << status.ToString();

    auto start = std::chrono::steady_clock::now();
    while (!completed && std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::steady_clock::now() - start)
                                 .count() < 10) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_TRUE(completed) << "Group " << group_id << " command was not committed";
  }
}

TEST_F(MultiRaft2GroupsTest, ReElectsAfterRestart) {
  StartCluster();

  WaitForLeader(1);
  WaitForLeader(2);

  // Capture term before restart to verify persistent state is restored.
  Term term1_before = stores_[0]->GetGroup(1)->CurrentTerm();
  Term term2_before = stores_[0]->GetGroup(2)->CurrentTerm();

  // Stop all stores, then restart from the same data directories.
  RestartCluster();

  // After restart, each group should re-elect a leader.  This validates that
  // the MultiRaftPersister preserved term/voted_for across the restart.
  WaitForLeader(1);
  WaitForLeader(2);

  EXPECT_GE(stores_[0]->GetGroup(1)->CurrentTerm(), term1_before)
      << "Group 1 term should not regress after restart";
  EXPECT_GE(stores_[0]->GetGroup(2)->CurrentTerm(), term2_before)
      << "Group 2 term should not regress after restart";
}