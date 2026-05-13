#include <gtest/gtest.h>
#include <memory>
#include <thread>

#include "rollingraft/log_persister.h"
#include "rollingraft/persister.h"
#include "rollingraft/raft_log.h"

#if defined(__has_include)
#if __has_include(<asio.hpp>)
#include <asio.hpp>
#define ROLLINGRAFT_HAS_ASIO
#endif
#endif

using namespace rollingraft;

class MockPersister : public Persister {
 public:
  Status Open(const std::string&) override { return Status::OK(); }
  void Close() override {}

  Status SaveState(const PersistentState&) override { return Status::OK(); }
  Status LoadState(PersistentState&) override { return Status::OK(); }

  Status AppendEntries(const std::vector<RaftLogEntry>& entries) override {
    for (const auto& e : entries) {
      entries_[e.index_] = e;
      if (e.index_ > last_index_) {
        last_index_ = e.index_;
      }
    }
    return Status::OK();
  }

  Status GetEntries(uint64_t start, uint64_t end,
                    std::vector<RaftLogEntry>* out) override {
    for (uint64_t i = start; i < end && i <= last_index_; ++i) {
      auto it = entries_.find(i);
      if (it != entries_.end()) {
        out->push_back(it->second);
      }
    }
    return Status::OK();
  }

  Status GetEntry(uint64_t index, RaftLogEntry& entry) override {
    auto it = entries_.find(index);
    if (it == entries_.end()) {
      return Status::Error("Not found");
    }
    entry = it->second;
    return Status::OK();
  }

  Status TruncateSuffix(uint64_t from_index) override {
    auto it = entries_.lower_bound(from_index);
    entries_.erase(it, entries_.end());
    if (!entries_.empty()) {
      last_index_ = entries_.rbegin()->first;
    } else {
      last_index_ = 0;
    }
    return Status::OK();
  }

  Status TruncatePrefix(uint64_t before_index) override {
    auto it = entries_.lower_bound(before_index);
    entries_.erase(entries_.begin(), it);
    if (!entries_.empty()) {
      last_index_ = entries_.rbegin()->first;
    } else {
      last_index_ = 0;
    }
    return Status::OK();
  }

  std::pair<uint64_t, uint64_t> GetLastLogInfo() override {
    if (entries_.empty()) {
      return {0, 0};
    }
    const auto& last = entries_.rbegin()->second;
    return {last.index_, last.term_};
  }

  std::map<uint64_t, RaftLogEntry> entries_;
  uint64_t last_index_ = 0;
};

TEST(LogCompactionTest, TruncatePrefixDeletesOldEntries) {
  auto mock = std::make_shared<MockPersister>();
  auto* mock_ptr = mock.get();
  LogPersister lp(mock);
  lp.Start();

  // Append 10 entries
  for (int i = 1; i <= 10; ++i) {
    RaftLogEntry entry;
    entry.index_ = i;
    entry.term_ = 1;
    lp.Append(entry);
  }

  lp.FlushSync();
  EXPECT_EQ(mock_ptr->entries_.size(), 10);

  // Truncate prefix at 5 (delete entries 1-4)
  auto status = lp.TruncatePrefix(5);
  EXPECT_TRUE(status.ok());

  EXPECT_EQ(mock_ptr->entries_.size(), 6);
  EXPECT_EQ(mock_ptr->entries_.count(1), 0);
  EXPECT_EQ(mock_ptr->entries_.count(4), 0);
  EXPECT_EQ(mock_ptr->entries_.count(5), 1);
  EXPECT_EQ(mock_ptr->entries_.count(10), 1);

  lp.Stop();
}

TEST(LogCompactionTest, TruncatePrefixWithBufferedEntries) {
  auto mock = std::make_shared<MockPersister>();
  auto* mock_ptr = mock.get();
  LogPersister lp(mock);
  lp.Start();

  // Append entries without flushing
  for (int i = 1; i <= 5; ++i) {
    RaftLogEntry entry;
    entry.index_ = i;
    entry.term_ = 1;
    lp.Append(entry);
  }

  // TruncatePrefix should drain buffer first
  auto status = lp.TruncatePrefix(3);
  EXPECT_TRUE(status.ok());

  // Entries 1-2 should be gone, 3-5 should remain
  EXPECT_EQ(mock_ptr->entries_.count(1), 0);
  EXPECT_EQ(mock_ptr->entries_.count(2), 0);
  EXPECT_EQ(mock_ptr->entries_.count(3), 1);
  EXPECT_EQ(mock_ptr->entries_.count(5), 1);

  lp.Stop();
}

TEST(LogCompactionTest, RetentionMath) {
  // retention=0: compact_before = snapshot_index + 1
  uint64_t snapshot_index = 1000;
  uint64_t retention = 0;
  uint64_t compact_before = 1;
  if (snapshot_index + 1 > retention) {
    compact_before = snapshot_index + 1 - retention;
  }
  EXPECT_EQ(compact_before, 1001);

  // retention=100: compact_before = 1001 - 100 = 901
  retention = 100;
  compact_before = 1;
  if (snapshot_index + 1 > retention) {
    compact_before = snapshot_index + 1 - retention;
  }
  EXPECT_EQ(compact_before, 901);

  // snapshot < retention: compact_before = 1
  snapshot_index = 50;
  retention = 100;
  compact_before = 1;
  if (snapshot_index + 1 > retention) {
    compact_before = snapshot_index + 1 - retention;
  }
  EXPECT_EQ(compact_before, 1);
}

TEST(LogCompactionTest, TruncatePrefixAsyncSyncFallback) {
  auto mock = std::make_shared<MockPersister>();
  auto* mock_ptr = mock.get();
  LogPersister lp(mock);
  lp.Start();

  // Append and flush entries
  for (int i = 1; i <= 10; ++i) {
    RaftLogEntry entry;
    entry.index_ = i;
    entry.term_ = 1;
    lp.Append(entry);
  }
  lp.FlushSync();

  // No executor configured → sync fallback
  std::promise<Status> promise;
  lp.TruncatePrefixAsync(5, [&promise](Status s) { promise.set_value(s); });

  auto future = promise.get_future();
  ASSERT_EQ(future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  EXPECT_TRUE(future.get().ok());
  EXPECT_EQ(mock_ptr->entries_.size(), 6);

  lp.Stop();
}

#ifdef ROLLINGRAFT_HAS_ASIO
TEST(LogCompactionTest, TruncatePrefixAsyncWithExecutor) {
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  std::thread io_thread([&]() { io.run(); });

  LogPersistenceConfig config;
  config.executor = [&io](std::function<void()> fn) {
    asio::post(io, std::move(fn));
  };

  auto mock = std::make_shared<MockPersister>();
  auto* mock_ptr = mock.get();
  LogPersister lp(mock, config);
  lp.Start();

  // Append and flush entries
  for (int i = 1; i <= 10; ++i) {
    RaftLogEntry entry;
    entry.index_ = i;
    entry.term_ = 1;
    lp.Append(entry);
  }
  lp.FlushSync();

  std::promise<Status> promise;
  lp.TruncatePrefixAsync(5, [&promise](Status s) { promise.set_value(s); });

  // Must pump io_context to execute posted work
  work.reset();
  io_thread.join();

  auto future = promise.get_future();
  ASSERT_EQ(future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  EXPECT_TRUE(future.get().ok());
  EXPECT_EQ(mock_ptr->entries_.size(), 6);

  lp.Stop();
}

TEST(LogCompactionTest, TruncatePrefixAsyncShutdownSafety) {
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  std::thread io_thread([&]() { io.run(); });

  LogPersistenceConfig config;
  config.executor = [&io](std::function<void()> fn) {
    asio::post(io, std::move(fn));
  };

  {
    auto mock = std::make_shared<MockPersister>();
    LogPersister lp(mock, config);
    lp.Start();

    // Fire async truncation then immediately destroy
    lp.TruncatePrefixAsync(5, [](Status s) {
      // Should either succeed or return shutdown error
      (void)s;
    });

    // Destructor must not crash or UAF
  }  // lp destroyed here

  work.reset();
  io_thread.join();
}
#endif  // ROLLINGRAFT_HAS_ASIO
