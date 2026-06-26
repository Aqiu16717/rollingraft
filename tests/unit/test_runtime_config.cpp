/**
 * @file test_runtime_config.cpp
 * @brief Unit tests for RuntimeConfig hot reload
 */

#include <atomic>
#include <thread>
#include <vector>

#include "rollingraft/runtime_config.h"

#include <gtest/gtest.h>

using namespace rollingraft;

class RuntimeConfigTest : public ::testing::Test {
 protected:
  RuntimeConfig::Values MakeDefaultValues() {
    RuntimeConfig::Values v;
    v.election_timeout_ms = 300;
    v.heartbeat_interval_ms = 50;
    v.max_entries_per_append = 100;
    v.rpc_timeout_ms = 500;
    v.snapshot_threshold_entries = 10000;
    v.snapshot_threshold_bytes = 10 * 1024 * 1024;
    v.snapshot_check_interval_ms = 5000;
    v.max_retry_attempts = 5;
    v.base_retry_delay_ms = 10;
    v.max_retry_delay_ms = 500;
    v.log_retention_entries = 0;
    v.max_snapshot_size_bytes = 100 * 1024 * 1024;
    v.propose_timeout_ms = 5000;
    v.leader_lease_enabled = true;
    v.max_pipeline_window = 128;
    v.transport_batching_enabled = true;
    return v;
  }
};

TEST_F(RuntimeConfigTest, DefaultValues) {
  RuntimeConfig rc;
  auto v = rc.Get();
  EXPECT_EQ(v.election_timeout_ms, 300);
  EXPECT_EQ(v.heartbeat_interval_ms, 50);
  EXPECT_TRUE(v.transport_batching_enabled);
}

TEST_F(RuntimeConfigTest, UpdateFromJson_SingleField) {
  RuntimeConfig rc(MakeDefaultValues());
  auto status = rc.UpdateFromJson(R"({"election_timeout_ms": 400})");
  EXPECT_TRUE(status.ok());
  auto v = rc.Get();
  EXPECT_EQ(v.election_timeout_ms, 400);
  EXPECT_EQ(v.heartbeat_interval_ms, 50);  // unchanged
}

TEST_F(RuntimeConfigTest, UpdateFromJson_TransportBatchingEnabled) {
  RuntimeConfig rc(MakeDefaultValues());
  EXPECT_TRUE(rc.Get().transport_batching_enabled);

  auto status = rc.UpdateFromJson(R"({"transport_batching_enabled": false})");
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(rc.Get().transport_batching_enabled);

  status = rc.UpdateFromJson(R"({"transport_batching_enabled": true})");
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(rc.Get().transport_batching_enabled);
}

TEST_F(RuntimeConfigTest, UpdateFromJson_MultipleFields) {
  RuntimeConfig rc(MakeDefaultValues());
  auto status = rc.UpdateFromJson(R"({
    "heartbeat_interval_ms": 100,
    "election_timeout_ms": 500,
    "transport_batching_enabled": false
  })");
  EXPECT_TRUE(status.ok());
  auto v = rc.Get();
  EXPECT_EQ(v.heartbeat_interval_ms, 100);
  EXPECT_EQ(v.election_timeout_ms, 500);
  EXPECT_FALSE(v.transport_batching_enabled);
}

TEST_F(RuntimeConfigTest, UpdateFromJson_InvalidJson) {
  RuntimeConfig rc(MakeDefaultValues());
  auto status = rc.UpdateFromJson("not json");
  EXPECT_FALSE(status.ok());
}

TEST_F(RuntimeConfigTest, UpdateFromJson_InvalidRange) {
  RuntimeConfig rc(MakeDefaultValues());
  auto status = rc.UpdateFromJson(R"({"election_timeout_ms": 1})");
  EXPECT_FALSE(status.ok());
}

TEST_F(RuntimeConfigTest, UpdateFromJson_CrossParameterViolation) {
  RuntimeConfig rc(MakeDefaultValues());
  // heartbeat_interval_ms must be < election_timeout_ms
  auto status = rc.UpdateFromJson(R"({
    "heartbeat_interval_ms": 400,
    "election_timeout_ms": 400
  })");
  EXPECT_FALSE(status.ok());
}

TEST_F(RuntimeConfigTest, UpdateFromJson_AtomicAllOrNothing) {
  RuntimeConfig rc(MakeDefaultValues());
  auto before = rc.Get();

  // Partial invalid update should not apply any changes
  auto status = rc.UpdateFromJson(R"({
    "heartbeat_interval_ms": 100,
    "election_timeout_ms": 1
  })");
  EXPECT_FALSE(status.ok());

  auto after = rc.Get();
  EXPECT_EQ(after.heartbeat_interval_ms, before.heartbeat_interval_ms);
  EXPECT_EQ(after.election_timeout_ms, before.election_timeout_ms);
}

TEST_F(RuntimeConfigTest, UpdateFromJson_UnknownFieldIgnored) {
  RuntimeConfig rc(MakeDefaultValues());
  auto status = rc.UpdateFromJson(R"({"unknown_field": 123})");
  EXPECT_TRUE(status.ok());
}

TEST_F(RuntimeConfigTest, ToJson_RoundTrip) {
  RuntimeConfig rc(MakeDefaultValues());
  auto json = rc.ToJson();
  EXPECT_NE(json.find("\"election_timeout_ms\": 300"), std::string::npos);
  EXPECT_NE(json.find("\"heartbeat_interval_ms\": 50"), std::string::npos);
  EXPECT_NE(json.find("\"transport_batching_enabled\": true"), std::string::npos);
}

TEST_F(RuntimeConfigTest, Reset) {
  RuntimeConfig rc(MakeDefaultValues());
  rc.UpdateFromJson(R"({"election_timeout_ms": 400})");
  EXPECT_EQ(rc.Get().election_timeout_ms, 400);

  rc.Reset();
  EXPECT_EQ(rc.Get().election_timeout_ms, 300);
}

TEST_F(RuntimeConfigTest, Validate_MaxPipelineWindow) {
  RuntimeConfig rc(MakeDefaultValues());
  auto status = rc.UpdateFromJson(R"({"max_pipeline_window": 0})");
  EXPECT_FALSE(status.ok());

  status = rc.UpdateFromJson(R"({"max_pipeline_window": 10001})");
  EXPECT_FALSE(status.ok());

  status = rc.UpdateFromJson(R"({"max_pipeline_window": 256})");
  EXPECT_TRUE(status.ok());
}

TEST_F(RuntimeConfigTest, ThreadSafeConcurrentReads) {
  RuntimeConfig rc(MakeDefaultValues());
  std::atomic<int> failures{0};

  std::thread updater([&]() {
    for (int i = 0; i < 100; ++i) {
      auto json = "{\"election_timeout_ms\": " + std::to_string(300 + i % 10) + "}";
      rc.UpdateFromJson(json);
    }
  });

  std::vector<std::thread> readers;
  for (int t = 0; t < 4; ++t) {
    readers.emplace_back([&]() {
      for (int i = 0; i < 100; ++i) {
        auto v = rc.Get();
        if (v.election_timeout_ms < 50 || v.election_timeout_ms > 5000) {
          ++failures;
        }
      }
    });
  }

  updater.join();
  for (auto& t : readers) t.join();
  EXPECT_EQ(failures, 0);
}
