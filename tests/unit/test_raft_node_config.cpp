/**
 * @file test_raft_node_config.cpp
 * @brief Unit tests for RaftNodeConfig::Validate()
 */

#include <gtest/gtest.h>

#include "rollingraft/raft_node.h"

using namespace rollingraft;

class RaftNodeConfigValidateTest : public ::testing::Test {
 protected:
  RaftNodeConfig MakeValidConfig() {
    RaftNodeConfig config;
    config.node_id = 1;
    config.listen_addr = "127.0.0.1:8001";
    config.peers = {"127.0.0.1:8002", "127.0.0.1:8003"};
    config.data_dir = "/tmp/raft_test";
    config.election_timeout_ms = 300;
    config.heartbeat_interval_ms = 50;
    return config;
  }
};

TEST_F(RaftNodeConfigValidateTest, ValidConfig) {
  auto config = MakeValidConfig();
  EXPECT_TRUE(config.Validate().ok());
}

TEST_F(RaftNodeConfigValidateTest, EmptyListenAddr) {
  auto config = MakeValidConfig();
  config.listen_addr = "";
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("listen_addr"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, InvalidListenAddrNoColon) {
  auto config = MakeValidConfig();
  config.listen_addr = "127.0.0.1";
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("host:port"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, InvalidListenAddrEmptyPort) {
  auto config = MakeValidConfig();
  config.listen_addr = "127.0.0.1:";
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
}

TEST_F(RaftNodeConfigValidateTest, InvalidListenAddrPortZero) {
  auto config = MakeValidConfig();
  config.listen_addr = "127.0.0.1:0";
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("port"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, InvalidListenAddrPortTooHigh) {
  auto config = MakeValidConfig();
  config.listen_addr = "127.0.0.1:70000";
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("port"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, InvalidListenAddrNonNumericPort) {
  auto config = MakeValidConfig();
  config.listen_addr = "127.0.0.1:abc";
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("port"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, EmptyDataDir) {
  auto config = MakeValidConfig();
  config.data_dir = "";
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("data_dir"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, ElectionTimeoutNotGreaterThanHeartbeat) {
  auto config = MakeValidConfig();
  config.election_timeout_ms = 50;
  config.heartbeat_interval_ms = 50;
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("election_timeout_ms"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, ElectionTimeoutLessThanHeartbeat) {
  auto config = MakeValidConfig();
  config.election_timeout_ms = 30;
  config.heartbeat_interval_ms = 50;
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
}

TEST_F(RaftNodeConfigValidateTest, PeerNodeIdsSizeMismatch) {
  auto config = MakeValidConfig();
  config.peer_node_ids = {2};  // Only 1 ID for 2 peers
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("peer_node_ids"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, PeerNodeIdsMatch) {
  auto config = MakeValidConfig();
  config.peer_node_ids = {2, 3};
  EXPECT_TRUE(config.Validate().ok());
}

TEST_F(RaftNodeConfigValidateTest, MetricsAddrInvalidWhenEnabled) {
  auto config = MakeValidConfig();
  config.metrics_enabled = true;
  config.metrics_addr = "invalid";
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("metrics_addr"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, MetricsAddrValidWhenEnabled) {
  auto config = MakeValidConfig();
  config.metrics_enabled = true;
  config.metrics_addr = "0.0.0.0:9001";
  EXPECT_TRUE(config.Validate().ok());
}

TEST_F(RaftNodeConfigValidateTest, MetricsAddrEmptyWhenEnabled) {
  auto config = MakeValidConfig();
  config.metrics_enabled = true;
  config.metrics_addr = "";
  // Empty metrics_addr is allowed even when enabled (may use default)
  EXPECT_TRUE(config.Validate().ok());
}

TEST_F(RaftNodeConfigValidateTest, TlsEnabledMissingCert) {
  auto config = MakeValidConfig();
  config.tls_enabled = true;
  config.tls_key_file = "/tmp/key.pem";
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("tls_cert_file"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, TlsEnabledMissingKey) {
  auto config = MakeValidConfig();
  config.tls_enabled = true;
  config.tls_cert_file = "/tmp/cert.pem";
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("tls_key_file"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, TlsEnabledWithCertAndKey) {
  auto config = MakeValidConfig();
  config.tls_enabled = true;
  config.tls_cert_file = "/tmp/cert.pem";
  config.tls_key_file = "/tmp/key.pem";
  EXPECT_TRUE(config.Validate().ok());
}

TEST_F(RaftNodeConfigValidateTest, TlsDisabledIgnoresCertFields) {
  auto config = MakeValidConfig();
  config.tls_enabled = false;
  config.tls_cert_file = "";
  config.tls_key_file = "";
  EXPECT_TRUE(config.Validate().ok());
}

TEST_F(RaftNodeConfigValidateTest, NegativeNodeId) {
  auto config = MakeValidConfig();
  config.node_id = -1;
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("node_id"), std::string::npos);
}

TEST_F(RaftNodeConfigValidateTest, ZeroElectionTimeout) {
  auto config = MakeValidConfig();
  config.election_timeout_ms = 0;
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
}

TEST_F(RaftNodeConfigValidateTest, ZeroHeartbeatInterval) {
  auto config = MakeValidConfig();
  config.heartbeat_interval_ms = 0;
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
}

TEST_F(RaftNodeConfigValidateTest, ZeroRpcTimeout) {
  auto config = MakeValidConfig();
  config.rpc_timeout_ms = 0;
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
}

TEST_F(RaftNodeConfigValidateTest, ZeroMaxEntriesPerAppend) {
  auto config = MakeValidConfig();
  config.max_entries_per_append = 0;
  auto status = config.Validate();
  EXPECT_FALSE(status.ok());
}

TEST_F(RaftNodeConfigValidateTest, RaftNodeConstructorThrowsOnInvalidConfig) {
  auto config = MakeValidConfig();
  config.listen_addr = "";
  EXPECT_THROW({
    RaftNode node(config, nullptr);
  }, std::invalid_argument);
}

TEST_F(RaftNodeConfigValidateTest, RaftNodeConstructorThrowsOnNullStateMachine) {
  auto config = MakeValidConfig();
  // Validation passes, but RaftNodeImpl will throw for null state_machine
  EXPECT_THROW({
    RaftNode node(config, nullptr);
  }, std::invalid_argument);
}
