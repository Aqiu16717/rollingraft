/**
 * @file test_raft_log.cpp
 * @brief Unit tests for in-memory Raft log accounting
 */

#include <string>

#include "rollingraft/raft_log.h"

#include <gtest/gtest.h>

namespace rollingraft {
namespace {

constexpr size_t kEntryMetadataBytes = sizeof(Index) + sizeof(Term);

TEST(RaftLogTest, GetLogStatsTracksAppendedEntries) {
  RaftLog log;

  const std::pair<size_t, size_t> empty_stats{0, 0};
  EXPECT_EQ(log.GetLogStats(), empty_stats);

  ASSERT_TRUE(log.Append(1, "first").second.ok());

  RaftLogEntry recovered_entry;
  recovered_entry.index_ = 2;
  recovered_entry.term_ = 1;
  recovered_entry.data_ = "second entry";
  ASSERT_TRUE(log.AppendLogEntry(recovered_entry).ok());

  auto [entry_count, estimated_bytes] = log.GetLogStats();
  EXPECT_EQ(entry_count, 2);
  EXPECT_EQ(estimated_bytes,
            2 * kEntryMetadataBytes + std::string("first").size() + recovered_entry.data_.size());
}

TEST(RaftLogTest, GetLogStatsTracksSuffixTruncation) {
  RaftLog log;
  ASSERT_TRUE(log.Append(1, "one").second.ok());
  ASSERT_TRUE(log.Append(1, "two-two").second.ok());
  ASSERT_TRUE(log.Append(2, "three-three-three").second.ok());

  ASSERT_TRUE(log.TruncateSuffix(3).ok());
  auto [entry_count, estimated_bytes] = log.GetLogStats();
  EXPECT_EQ(entry_count, 2);
  EXPECT_EQ(estimated_bytes,
            2 * kEntryMetadataBytes + std::string("one").size() + std::string("two-two").size());

  ASSERT_TRUE(log.TruncateSuffix(1).ok());
  const std::pair<size_t, size_t> empty_stats{0, 0};
  EXPECT_EQ(log.GetLogStats(), empty_stats);
}

TEST(RaftLogTest, GetLogStatsUnchangedWhenTruncationIsPastEnd) {
  RaftLog log;
  ASSERT_TRUE(log.Append(1, "payload").second.ok());
  auto stats_before = log.GetLogStats();

  ASSERT_TRUE(log.TruncateSuffix(3).ok());

  EXPECT_EQ(log.GetLogStats(), stats_before);
}

TEST(RaftLogTest, SetStartIndexResetsLogStats) {
  RaftLog log;
  ASSERT_TRUE(log.Append(1, "old payload").second.ok());

  log.SetStartIndex(42);

  const std::pair<size_t, size_t> empty_stats{0, 0};
  EXPECT_EQ(log.GetLogStats(), empty_stats);
  auto [index, status] = log.Append(2, "new");
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(index, 42);
  const std::pair<size_t, size_t> expected_stats{1,
                                                 kEntryMetadataBytes + std::string("new").size()};
  EXPECT_EQ(log.GetLogStats(), expected_stats);
}

}  // namespace
}  // namespace rollingraft
