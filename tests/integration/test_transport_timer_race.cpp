/**
 * @file test_transport_timer_race.cpp
 * @brief TSan stress test for the RPC timeout timer data race
 *
 * Each outbound RPC arms an asio::steady_timer in TcpConnection::Send. Send
 * runs on the PeerConnection strand (an io thread); the response path cancels
 * the same timer from the TcpConnection strand (a different io thread), and
 * Close() cancels from yet other threads. Operating on one ASIO timer object
 * from multiple threads without serialization is a data race (ASIO "shared
 * objects: unsafe").
 *
 * This test hammers Propose so arm/cancel pairs flow continuously; TSan
 * detects the race on the unfixed code.
 */

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rollingraft/logger.h"
#include "rollingraft/raft_node.h"

#include "ephemeral_port.h"
#include "mock/mock_state_machine.h"
#include <gtest/gtest.h>

using namespace rollingraft;

namespace {

RaftNodeConfig MakeRaceConfig(NodeId id, const std::string& addr,
                              const std::vector<std::string>& all_addrs,
                              const std::string& data_dir) {
  RaftNodeConfig config;
  config.node_id = id;
  config.listen_addr = addr;
  config.data_dir = data_dir;
  config.election_timeout_ms = 300;
  config.heartbeat_interval_ms = 20;  // Fast heartbeat keeps RPC timers flowing
  config.rpc_timeout_ms = 200;
  config.base_retry_delay_ms = 5;
  config.max_retry_delay_ms = 100;
  config.max_retry_attempts = 10;

  for (size_t j = 0; j < all_addrs.size(); ++j) {
    if (all_addrs[j] != addr) {
      config.peers.push_back(all_addrs[j]);
      config.peer_node_ids.push_back(static_cast<NodeId>(j + 1));
    }
  }

  return config;
}

// One behavior: concurrent Propose traffic must be free of the timer
// arm/cancel data race in the ASIO transport (checked by TSan).
TEST(TransportTimerRaceTest, ConcurrentProposeResponseCancelIsRaceFree) {
  const std::vector<std::string> data_dirs = {"/tmp/raft_race_node_1", "/tmp/raft_race_node_2",
                                              "/tmp/raft_race_node_3"};
  for (const auto& dir : data_dirs) {
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
  }

  // Three nodes: under TSan a 2-node cluster's split votes can starve the
  // initial election, while 3 nodes tolerate one delayed vote.
  auto ports = AllocateEphemeralPorts(3);
  auto addrs = FormatAddrs(ports);

  std::vector<RaftNodeConfig> configs;
  for (int i = 0; i < 3; ++i) {
    configs.push_back(MakeRaceConfig(i + 1, addrs[i], addrs, data_dirs[i]));
  }

  auto sm1 = std::make_shared<MockStateMachine>();
  auto sm2 = std::make_shared<MockStateMachine>();
  auto sm3 = std::make_shared<MockStateMachine>();
  RaftNode node1(configs[0], sm1);
  RaftNode node2(configs[1], sm2);
  RaftNode node3(configs[2], sm3);
  ASSERT_TRUE(node1.Start().ok());
  ASSERT_TRUE(node2.Start().ok());
  ASSERT_TRUE(node3.Start().ok());

  // Wait for a leader so Propose has a target. Generous timeout: TSan slows
  // the io threads down heavily.
  RaftNode* leader = nullptr;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (std::chrono::steady_clock::now() < deadline && leader == nullptr) {
    if (node1.IsLeader()) {
      leader = &node1;
    } else if (node2.IsLeader()) {
      leader = &node2;
    } else if (node3.IsLeader()) {
      leader = &node3;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  if (leader == nullptr) {
    // Stop before failing: unwinding into still-running io threads trips
    // shared_from_this during teardown.
    node1.Stop();
    node2.Stop();
    node3.Stop();
    for (const auto& dir : data_dirs) {
      std::filesystem::remove_all(dir);
    }
    FAIL() << "No leader elected";
  }

  // Hammer Propose from two caller threads; each round-trip arms a timer on
  // one io thread and cancels it on another.
  std::atomic<bool> stop{false};
  std::atomic<int> completed{0};
  auto worker = [&](int seed) {
    int i = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      std::atomic<bool> done{false};
      leader->Propose("race_" + std::to_string(seed) + "_" + std::to_string(i++),
                      [&](const ApplyResult&) { done = true; });
      auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
      while (!done && std::chrono::steady_clock::now() < wait_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
      }
      completed.fetch_add(1, std::memory_order_relaxed);
    }
  };
  std::thread t1(worker, 1);
  std::thread t2(worker, 2);
  std::this_thread::sleep_for(std::chrono::seconds(3));
  stop.store(true, std::memory_order_relaxed);
  t1.join();
  t2.join();

  EXPECT_GT(completed.load(), 0);

  node1.Stop();
  node2.Stop();
  node3.Stop();

  for (const auto& dir : data_dirs) {
    std::filesystem::remove_all(dir);
  }
}

}  // namespace
