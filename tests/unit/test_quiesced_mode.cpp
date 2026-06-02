#include <gtest/gtest.h>

#include "rollingraft/raft_node.h"

using namespace rollingraft;

TEST(QuiescedModeConfigTest, Defaults) {
  RaftNodeConfig config;
  EXPECT_FALSE(config.quiesced_mode_enabled);
  EXPECT_EQ(config.quiesced_idle_threshold_ms, 2000u);
  EXPECT_EQ(config.quiesced_heartbeat_interval_ms, 5000u);
  EXPECT_EQ(config.quiesced_election_timeout_ms, 10000u);
  EXPECT_EQ(config.quiesced_max_consecutive_timeouts, 3u);
}

TEST(QuiescedModeConfigTest, CustomValues) {
  RaftNodeConfig config;
  config.quiesced_mode_enabled = true;
  config.quiesced_idle_threshold_ms = 1000;
  config.quiesced_heartbeat_interval_ms = 3000;
  config.quiesced_election_timeout_ms = 8000;
  config.quiesced_max_consecutive_timeouts = 5;

  EXPECT_TRUE(config.quiesced_mode_enabled);
  EXPECT_EQ(config.quiesced_idle_threshold_ms, 1000u);
  EXPECT_EQ(config.quiesced_heartbeat_interval_ms, 3000u);
  EXPECT_EQ(config.quiesced_election_timeout_ms, 8000u);
  EXPECT_EQ(config.quiesced_max_consecutive_timeouts, 5u);
}
