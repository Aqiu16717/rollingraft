#include <memory>

#include "mock/mock_network.h"
#include "mock/mock_timer.h"
#include "raft_store.h"
#include "test_port.h"
#include <gtest/gtest.h>

namespace rollingraft {
namespace {

RaftStoreConfig MakeStoreConfig(uint16_t metrics_port) {
  RaftStoreConfig config;
  config.node_id = 1;
  config.listen_addr = "127.0.0.1:19001";
  config.metrics_enabled = true;
  config.metrics_addr = "127.0.0.1:" + std::to_string(metrics_port);
  config.network_factory = [] { return std::make_unique<MockNetworkTransport>(); };
  config.timer_factory = [] { return std::make_unique<MockTimerService>(); };
  return config;
}

TEST(RaftStoreLifecycleTest, StartBeforeInitializeDoesNotPoisonRetry) {
  auto config = MakeStoreConfig(GetUniqueTestPort());
  config.metrics_enabled = false;
  RaftStore store(config);

  EXPECT_FALSE(store.Start().ok());
  ASSERT_TRUE(store.Initialize().ok());
  EXPECT_TRUE(store.Start().ok());
  EXPECT_TRUE(store.Stop().ok());
}

TEST(RaftStoreLifecycleTest, MetricsStartFailureRollsBackAndAllowsRetryAttempt) {
  auto config = MakeStoreConfig(GetUniqueTestPort());
  config.metrics_addr = "invalid-address:12345";
  RaftStore store(config);
  ASSERT_TRUE(store.Initialize().ok());
  auto first_status = store.Start();
  EXPECT_FALSE(first_status.ok());
  EXPECT_EQ(store.GetInfra()->metrics_server_, nullptr);

  auto retry_status = store.Start();
  EXPECT_FALSE(retry_status.ok());
  EXPECT_EQ(retry_status.GetMessage().find("Already started"), std::string::npos);
  EXPECT_EQ(store.GetInfra()->metrics_server_, nullptr);
}

}  // namespace
}  // namespace rollingraft
