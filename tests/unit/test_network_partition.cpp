#include <chrono>
#include <gtest/gtest.h>
#include <memory>

#include "rollingraft/raft_node.h"

#include "mock/mock_network.h"
#include "mock/mock_state_machine.h"

using namespace rollingraft;

/**
 * Network partition tests.
 *
 * These tests verify mock network behavior for simulating partitions.
 */

class NetworkPartitionTest : public ::testing::Test {
 protected:
  void SetUp() override { sm_ = std::make_shared<MockStateMachine>(); }

  void TearDown() override {
    if (node_) {
      node_->Stop();
    }
  }

  RaftNodeConfig MakeConfig(NodeId id, const std::vector<NodeId>& peers) {
    RaftNodeConfig config;
    config.node_id = id;
    config.listen_addr = "127.0.0.1:" + std::to_string(8000 + id);
    config.election_timeout_ms = 300;
    config.heartbeat_interval_ms = 100;
    config.data_dir = "/tmp/raft_test_node_" + std::to_string(id);

    for (NodeId peer_id : peers) {
      config.peers.push_back("127.0.0.1:" + std::to_string(8000 + peer_id));
    }

    return config;
  }

  std::shared_ptr<MockStateMachine> sm_;
  std::unique_ptr<RaftNode> node_;
};

TEST_F(NetworkPartitionTest, MockNetwork_RecordsSentMessages) {
  MockNetworkTransport network;

  // Send some RPCs
  std::atomic<bool> callback1_called{false};
  std::atomic<bool> callback2_called{false};

  network.Initialize("", nullptr);
  network.Start();

  network.SendRpc(2, "127.0.0.1:8002", "request1",
                  std::chrono::milliseconds(1000),
                  [&](const std::string&, bool, const std::string&) {
                    callback1_called = true;
                  });

  network.SendRpc(3, "127.0.0.1:8003", "request2",
                  std::chrono::milliseconds(1000),
                  [&](const std::string&, bool, const std::string&) {
                    callback2_called = true;
                  });

  // Verify recorded requests
  auto requests = network.GetRecordedRequests();
  EXPECT_EQ(requests.size(), 2);
  EXPECT_EQ(requests[0].to, 2);
  EXPECT_EQ(requests[1].to, 3);
  EXPECT_EQ(requests[0].request_data, "request1");
  EXPECT_EQ(requests[1].request_data, "request2");

  network.Stop();
}

TEST_F(NetworkPartitionTest, MockNetwork_AutoResponse) {
  MockNetworkTransport network;

  network.Initialize("", nullptr);
  network.Start();

  // Set auto-response
  network.SetAutoResponse("{\"success\": true}", true);

  std::atomic<bool> callback_called{false};
  std::string response_data;

  network.SendRpc(
      2, "127.0.0.1:8002", "request", std::chrono::milliseconds(1000),
      [&](const std::string& resp, bool success, const std::string&) {
        callback_called = true;
        if (success) response_data = resp;
      });

  EXPECT_TRUE(callback_called);
  EXPECT_EQ(response_data, "{\"success\": true}");

  network.Stop();
}

TEST_F(NetworkPartitionTest, MockNetwork_PartitionedPeer) {
  MockNetworkTransport network;

  network.Initialize("", nullptr);
  network.Start();

  // Partition node 2
  network.SetPartitioned(2, true);

  std::atomic<bool> callback1_called{false};
  std::atomic<bool> callback2_called{false};
  bool callback1_success = false;
  bool callback2_success = false;

  // Send to partitioned peer
  network.SendRpc(2, "127.0.0.1:8002", "request1",
                  std::chrono::milliseconds(1000),
                  [&](const std::string&, bool success, const std::string&) {
                    callback1_called = true;
                    callback1_success = success;
                  });

  // Send to normal peer
  network.SendRpc(3, "127.0.0.1:8003", "request2",
                  std::chrono::milliseconds(1000),
                  [&](const std::string&, bool success, const std::string&) {
                    callback2_called = true;
                    callback2_success = success;
                  });

  // Partitioned peer should get error callback
  EXPECT_TRUE(callback1_called);
  EXPECT_FALSE(callback1_success);

  // Normal peer should not be called yet (no auto-response)
  EXPECT_FALSE(callback2_called);

  network.Stop();
}

TEST_F(NetworkPartitionTest, MockNetwork_TriggerResponse) {
  MockNetworkTransport network;

  network.Initialize("", nullptr);
  network.Start();

  std::atomic<bool> callback_called{false};
  std::string received_response;

  network.SendRpc(
      2, "127.0.0.1:8002", "request", std::chrono::milliseconds(1000),
      [&](const std::string& resp, bool success, const std::string&) {
        if (success) {
          received_response = resp;
        }
        callback_called = true;
      });

  // Manually trigger response
  network.TriggerResponse(0, "{\"vote_granted\": true}", true);

  EXPECT_TRUE(callback_called);
  EXPECT_EQ(received_response, "{\"vote_granted\": true}");

  network.Stop();
}

TEST_F(NetworkPartitionTest, MockNetwork_ClearRecordedRequests) {
  MockNetworkTransport network;

  network.Initialize("", nullptr);
  network.Start();

  // Send some requests
  network.SendRpc(2, "127.0.0.1:8002", "request1",
                  std::chrono::milliseconds(1000), nullptr);
  network.SendRpc(3, "127.0.0.1:8003", "request2",
                  std::chrono::milliseconds(1000), nullptr);

  EXPECT_EQ(network.GetRecordedRequests().size(), 2);

  // Clear requests
  network.ClearRecordedRequests();

  EXPECT_EQ(network.GetRecordedRequests().size(), 0);

  network.Stop();
}

TEST_F(NetworkPartitionTest, MockNetwork_InjectResponse) {
  MockNetworkTransport network;

  network.Initialize("", nullptr);
  network.Start();

  // Inject a response as if from peer
  // This API exists for testing response injection
  network.InjectResponse(2, "{\"term\": 5, \"success\": true}");

  // API should work without error
  SUCCEED();

  network.Stop();
}

// Note: Full partition testing with RaftNode requires:
// 1. Custom factories that share mock instances
// 2. Careful lifetime management
// These are tested in integration tests.
