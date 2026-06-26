/**
 * @file test_group_commit_controller.cpp
 * @brief Unit tests for GroupCommitController
 */

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#include "rollingraft/log_persister.h"
#include "rollingraft/metrics.h"
#include "rollingraft/status.h"

#include "group_commit_controller.h"
#include <gtest/gtest.h>

namespace rollingraft {

using std::chrono::milliseconds;
using std::chrono::steady_clock;

class GroupCommitControllerTest : public ::testing::Test {
 protected:
  LogPersistenceConfig MakeConfig(
      LogPersistenceConfig::SyncPolicy policy = LogPersistenceConfig::SyncPolicy::kSyncAdaptive,
      uint32_t interval_ms = 50, size_t max_entries = 1000, size_t max_bytes = 4 * 1024 * 1024) {
    LogPersistenceConfig config;
    config.sync_policy = policy;
    config.group_commit_interval_ms = interval_ms;
    config.group_commit_max_entries = max_entries;
    config.group_commit_max_bytes = max_bytes;
    return config;
  }
};

TEST_F(GroupCommitControllerTest, RegisterSingleBatch) {
  auto config = MakeConfig();
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  auto epoch = controller.RegisterFlushedBatch(10, 1024, callbacks, error);
  EXPECT_NE(epoch, 0u);
  EXPECT_TRUE(error.ok());

  auto stats = controller.GetStats();
  EXPECT_EQ(stats.pending_epochs, 1u);
  EXPECT_EQ(stats.unsynced_entries, 10u);
  EXPECT_EQ(stats.unsynced_bytes, 1024u);
}

TEST_F(GroupCommitControllerTest, EpochsAreMonotonic) {
  auto config = MakeConfig();
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  auto e1 = controller.RegisterFlushedBatch(1, 1, callbacks, error);
  auto e2 = controller.RegisterFlushedBatch(1, 1, callbacks, error);
  auto e3 = controller.RegisterFlushedBatch(1, 1, callbacks, error);

  EXPECT_EQ(e1 + 1, e2);
  EXPECT_EQ(e2 + 1, e3);
}

TEST_F(GroupCommitControllerTest, AcquireSyncRangeReturnsAllPending) {
  auto config = MakeConfig();
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  controller.RegisterFlushedBatch(1, 1, callbacks, error);
  controller.RegisterFlushedBatch(1, 1, callbacks, error);

  auto range = controller.AcquireSyncRange();
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(range->first, 1u);
  EXPECT_EQ(range->second, 2u);
}

TEST_F(GroupCommitControllerTest, AcquireSyncRangeBlocksSecondSync) {
  auto config = MakeConfig();
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  controller.RegisterFlushedBatch(1, 1, callbacks, error);

  auto range1 = controller.AcquireSyncRange();
  ASSERT_TRUE(range1.has_value());

  auto range2 = controller.AcquireSyncRange();
  EXPECT_FALSE(range2.has_value());
}

TEST_F(GroupCommitControllerTest, OnSyncSuccessAdvancesDurableEpoch) {
  auto config = MakeConfig();
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  controller.RegisterFlushedBatch(1, 1, callbacks, error);
  controller.RegisterFlushedBatch(1, 1, callbacks, error);

  auto range = controller.AcquireSyncRange();
  ASSERT_TRUE(range.has_value());

  controller.OnSyncSuccess(range->second);

  auto stats = controller.GetStats();
  EXPECT_EQ(stats.durable_epoch, 2u);
  EXPECT_EQ(stats.pending_epochs, 0u);
}

TEST_F(GroupCommitControllerTest, CallbacksFireOnSuccess) {
  auto config = MakeConfig();
  GroupCommitController controller(config);

  int fired = 0;
  Status received;
  GroupCommitController::DurableCallback cb = [&fired, &received](Status s) {
    ++fired;
    received = s;
  };

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks{cb};
  controller.RegisterFlushedBatch(1, 1, callbacks, error);

  auto range = controller.AcquireSyncRange();
  ASSERT_TRUE(range.has_value());
  controller.OnSyncSuccess(range->second);

  EXPECT_EQ(fired, 1);
  EXPECT_TRUE(received.ok());
}

TEST_F(GroupCommitControllerTest, CallbacksFireInEpochOrder) {
  auto config = MakeConfig();
  GroupCommitController controller(config);

  std::vector<uint64_t> order;
  auto make_cb = [&order](uint64_t epoch) {
    return [&order, epoch](Status) { order.push_back(epoch); };
  };

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks1{make_cb(1)};
  std::vector<GroupCommitController::DurableCallback> callbacks2{make_cb(2)};
  controller.RegisterFlushedBatch(1, 1, callbacks1, error);
  controller.RegisterFlushedBatch(1, 1, callbacks2, error);

  auto range = controller.AcquireSyncRange();
  ASSERT_TRUE(range.has_value());
  controller.OnSyncSuccess(range->second);

  ASSERT_EQ(order.size(), 2u);
  EXPECT_EQ(order[0], 1u);
  EXPECT_EQ(order[1], 2u);
}

TEST_F(GroupCommitControllerTest, OnSyncFailureFailsAllPending) {
  auto config = MakeConfig();
  GroupCommitController controller(config);

  int fired = 0;
  GroupCommitController::DurableCallback cb = [&fired](Status s) {
    ++fired;
    EXPECT_FALSE(s.ok());
  };

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks1{cb};
  std::vector<GroupCommitController::DurableCallback> callbacks2;
  controller.RegisterFlushedBatch(1, 1, callbacks1, error);
  controller.RegisterFlushedBatch(1, 1, callbacks2, error);

  auto range = controller.AcquireSyncRange();
  ASSERT_TRUE(range.has_value());

  controller.OnSyncFailure(range->second, Status::Error("fsync failed"));

  EXPECT_EQ(fired, 1);
  EXPECT_FALSE(controller.IsHealthy());

  auto stats = controller.GetStats();
  EXPECT_EQ(stats.pending_epochs, 0u);
}

TEST_F(GroupCommitControllerTest, UnhealthyControllerRejectsNewBatches) {
  auto config = MakeConfig();
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  controller.RegisterFlushedBatch(1, 1, callbacks, error);
  auto range = controller.AcquireSyncRange();
  ASSERT_TRUE(range.has_value());
  controller.OnSyncFailure(range->second, Status::Error("fsync failed"));

  Status error2;
  auto epoch = controller.RegisterFlushedBatch(1, 1, callbacks, error2);
  EXPECT_EQ(epoch, 0u);
  EXPECT_FALSE(error2.ok());
}

TEST_F(GroupCommitControllerTest, ShouldSyncNowByInterval) {
  auto config =
      MakeConfig(LogPersistenceConfig::SyncPolicy::kSyncByInterval, 10, 1000, 1024 * 1024);
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  controller.RegisterFlushedBatch(1, 1, callbacks, error);

  auto now = steady_clock::now();
  EXPECT_FALSE(controller.ShouldSyncNow(now));

  now += milliseconds(15);
  EXPECT_TRUE(controller.ShouldSyncNow(now));
}

TEST_F(GroupCommitControllerTest, ShouldSyncNowByBatchSizeEntries) {
  auto config =
      MakeConfig(LogPersistenceConfig::SyncPolicy::kSyncByBatchSize, 1000, 5, 1024 * 1024);
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  controller.RegisterFlushedBatch(3, 1, callbacks, error);
  auto now = steady_clock::now();
  EXPECT_FALSE(controller.ShouldSyncNow(now));

  controller.RegisterFlushedBatch(3, 1, callbacks, error);
  EXPECT_TRUE(controller.ShouldSyncNow(now));
}

TEST_F(GroupCommitControllerTest, ShouldSyncNowByBatchSizeBytes) {
  auto config = MakeConfig(LogPersistenceConfig::SyncPolicy::kSyncByBatchSize, 1000, 1000, 100);
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  controller.RegisterFlushedBatch(1, 50, callbacks, error);
  auto now = steady_clock::now();
  EXPECT_FALSE(controller.ShouldSyncNow(now));

  controller.RegisterFlushedBatch(1, 60, callbacks, error);
  EXPECT_TRUE(controller.ShouldSyncNow(now));
}

TEST_F(GroupCommitControllerTest, ShouldSyncNowAdaptiveUsesEitherThreshold) {
  auto config = MakeConfig(LogPersistenceConfig::SyncPolicy::kSyncAdaptive, 1000, 100, 1024 * 1024);
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  auto now = steady_clock::now();

  // Neither threshold reached.
  controller.RegisterFlushedBatch(1, 1, callbacks, error);
  EXPECT_FALSE(controller.ShouldSyncNow(now));

  // Interval threshold reached.
  now += milliseconds(1005);
  EXPECT_TRUE(controller.ShouldSyncNow(now));

  // Reset by acquiring and completing sync.
  auto range = controller.AcquireSyncRange();
  ASSERT_TRUE(range.has_value());
  controller.OnSyncSuccess(range->second);

  now = steady_clock::now();
  controller.RegisterFlushedBatch(150, 1, callbacks, error);
  EXPECT_TRUE(controller.ShouldSyncNow(now));
}

TEST_F(GroupCommitControllerTest, RequestSyncForcesSync) {
  auto config =
      MakeConfig(LogPersistenceConfig::SyncPolicy::kSyncByInterval, 1000, 1000, 1024 * 1024);
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  controller.RegisterFlushedBatch(1, 1, callbacks, error);

  auto now = steady_clock::now();
  EXPECT_FALSE(controller.ShouldSyncNow(now));

  controller.RequestSync();
  EXPECT_TRUE(controller.ShouldSyncNow(now));
}

TEST_F(GroupCommitControllerTest, SyncEveryWriteNeverBackgroundSyncs) {
  auto config = MakeConfig(LogPersistenceConfig::SyncPolicy::kSyncEveryWrite);
  GroupCommitController controller(config);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  controller.RegisterFlushedBatch(1, 1, callbacks, error);

  auto now = steady_clock::now();
  EXPECT_FALSE(controller.ShouldSyncNow(now));
}

TEST_F(GroupCommitControllerTest, MetricsExposePendingAndUnsynced) {
  MetricsRegistry metrics;
  auto config =
      MakeConfig(LogPersistenceConfig::SyncPolicy::kSyncAdaptive, 1000, 1000, 1024 * 1024);
  GroupCommitController controller(config, &metrics);

  Status error;
  std::vector<GroupCommitController::DurableCallback> callbacks;
  controller.RegisterFlushedBatch(7, 1234, callbacks, error);
  controller.RegisterFlushedBatch(3, 567, callbacks, error);

  auto output = metrics.FormatPrometheus();
  EXPECT_NE(output.find("raft_group_commit_pending_epochs"), std::string::npos) << output;
  EXPECT_NE(output.find("raft_group_commit_unsynced_entries"), std::string::npos) << output;

  // Acquire the full range and complete the sync.
  auto range = controller.AcquireSyncRange();
  ASSERT_TRUE(range.has_value());
  controller.OnSyncSuccess(range->second);

  output = metrics.FormatPrometheus();
  EXPECT_NE(output.find("raft_group_commit_unsynced_entries 0"), std::string::npos) << output;
}

}  // namespace rollingraft
