/**
 * @file group_commit_benchmark.cpp
 * @brief Benchmark durable throughput with and without group commit.
 *
 * Uses a synthetic persister that sleeps a fixed amount per Sync() to simulate
 * fsync latency. This isolates the group-commit effect from disk variance and
 * from WAL implementation details.
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "rollingraft/log_persister.h"
#include "rollingraft/persister.h"
#include "rollingraft/raft_log.h"

namespace rollingraft {

/**
 * Synthetic persister with configurable per-Sync() latency.
 */
class SyntheticPersister : public Persister {
 public:
  explicit SyntheticPersister(std::chrono::microseconds sync_latency)
      : sync_latency_(sync_latency) {}

  Status Open(const std::string&) override { return Status::OK(); }
  void Close() override {}

  Status SaveState(const PersistentState&) override { return Status::OK(); }
  Status LoadState(PersistentState&) override { return Status::OK(); }

  Status AppendEntries(const std::vector<RaftLogEntry>& entries) override {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& e : entries) {
      logs_[e.index_] = e;
    }
    return Status::OK();
  }

  Status Sync() override {
    std::this_thread::sleep_for(sync_latency_);
    std::lock_guard<std::mutex> lock(mutex_);
    ++sync_count_;
    return Status::OK();
  }

  Status GetEntries(uint64_t start, uint64_t end, std::vector<RaftLogEntry>* out) override {
    std::lock_guard<std::mutex> lock(mutex_);
    out->clear();
    for (uint64_t i = start; i < end; ++i) {
      auto it = logs_.find(i);
      if (it != logs_.end()) {
        out->push_back(it->second);
      }
    }
    return Status::OK();
  }

  Status GetEntry(uint64_t index, RaftLogEntry& entry) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = logs_.find(index);
    if (it == logs_.end()) {
      return Status::Error("not found");
    }
    entry = it->second;
    return Status::OK();
  }

  Status TruncateSuffix(uint64_t) override { return Status::OK(); }
  Status TruncatePrefix(uint64_t) override { return Status::OK(); }

  std::pair<uint64_t, uint64_t> GetLastLogInfo() override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (logs_.empty()) {
      return {0, 0};
    }
    const auto& last = logs_.rbegin()->second;
    return {last.index_, last.term_};
  }

  uint64_t SyncCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return sync_count_;
  }

 private:
  std::chrono::microseconds sync_latency_;
  mutable std::mutex mutex_;
  std::map<uint64_t, RaftLogEntry> logs_;
  uint64_t sync_count_ = 0;
};

static RaftLogEntry MakeEntry(uint64_t index, const std::string& data) {
  RaftLogEntry entry;
  entry.index_ = index;
  entry.term_ = 1;
  entry.data_ = data;
  return entry;
}

static double Bench(LogPersistenceConfig::SyncPolicy policy, int entries,
                    std::chrono::microseconds sync_latency, uint32_t interval_ms,
                    size_t max_entries_per_sync, size_t batch_size, uint32_t batch_interval_ms) {
  auto raw_synthetic = std::make_shared<SyntheticPersister>(sync_latency);
  std::shared_ptr<Persister> synthetic = raw_synthetic;

  LogPersistenceConfig config;
  config.batch_size = batch_size;
  config.batch_interval_ms = batch_interval_ms;
  config.sync_policy = policy;
  config.group_commit_interval_ms = interval_ms;
  config.group_commit_max_entries = max_entries_per_sync;

  LogPersister persister(synthetic, config);
  persister.Start();

  std::string payload(128, 'x');
  std::atomic<int> acked{0};
  std::mutex mutex;
  std::condition_variable cv;

  auto t0 = std::chrono::steady_clock::now();
  if (policy == LogPersistenceConfig::SyncPolicy::kSyncEveryWrite) {
    // Synchronous durable append: each entry waits for its own fsync.
    for (int i = 1; i <= entries; ++i) {
      persister.AppendSync(MakeEntry(i, payload), std::chrono::seconds(5));
    }
  } else {
    // Asynchronous durable append: entries are flushed and fsynced in groups.
    for (int i = 1; i <= entries; ++i) {
      persister.Append(MakeEntry(i, payload), [&acked, &mutex, &cv](Status) {
        acked.fetch_add(1, std::memory_order_acq_rel);
        {
          std::lock_guard<std::mutex> lock(mutex);
          cv.notify_one();
        }
      });
    }

    // Wait for all entries to be durably acknowledged.
    {
      std::unique_lock<std::mutex> lock(mutex);
      cv.wait(lock, [&acked, entries] { return acked.load(std::memory_order_acquire) >= entries; });
    }
  }
  auto t1 = std::chrono::steady_clock::now();

  persister.Stop();

  auto elapsed_ms = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() / 1000.0;
  double ops_per_sec = entries / (elapsed_ms / 1000.0);

  std::cout << "| "
            << (policy == LogPersistenceConfig::SyncPolicy::kSyncEveryWrite ? "sync-every-write"
                                                                            : "group-commit")
            << " | " << entries << " | " << sync_latency.count() << " us"
            << " | " << interval_ms << " ms"
            << " | " << max_entries_per_sync << " | " << batch_size << " | " << batch_interval_ms
            << " ms"
            << " | " << raw_synthetic->SyncCount() << " | " << std::fixed << elapsed_ms << " ms"
            << " | " << static_cast<int>(ops_per_sec) << " ops/s |\n";

  return ops_per_sec;
}

}  // namespace rollingraft

int main() {
  using namespace rollingraft;
  using std::chrono::microseconds;

  std::cout << "# Group Commit Durable Throughput Benchmark\n\n";
  std::cout << "Simulated fsync latency = 3700 us (matches profiling report on macOS).\n\n";
  std::cout << "| Policy | Entries | fsync latency | group interval | max entries/sync | "
               "flush batch | flush interval | Sync calls | Total time | Throughput |\n";
  std::cout << "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n";

  const int kEntries = 100;
  const auto kSyncLatency = microseconds(3700);

  // To amortize fsync, group commit batches many small flushes into one sync.
  // Sync-every-write pays a full fsync for every flushed batch.
  const size_t kFlushBatchSize = 1;
  const uint32_t kFlushIntervalMs = 1;
  const size_t kGroupCommitMaxEntries = static_cast<size_t>(kEntries);
  const uint32_t kGroupCommitIntervalMs = 1000;  // rely on entry threshold

  double every_write_ops = Bench(LogPersistenceConfig::SyncPolicy::kSyncEveryWrite, kEntries,
                                 kSyncLatency, 0, 1, kFlushBatchSize, kFlushIntervalMs);
  double group_commit_ops =
      Bench(LogPersistenceConfig::SyncPolicy::kSyncAdaptive, kEntries, kSyncLatency,
            kGroupCommitIntervalMs, kGroupCommitMaxEntries, kFlushBatchSize, kFlushIntervalMs);

  std::cout << "\n";
  if (every_write_ops > 0) {
    double ratio = group_commit_ops / every_write_ops;
    std::cout << "Group commit throughput improvement: " << std::fixed << std::setprecision(1)
              << ratio << "x\n";
    if (ratio >= 10.0) {
      std::cout << "✅ Meets >=10× target.\n";
    } else {
      std::cout << "⚠️ Below >=10× target (expected with more entries or smaller "
                   "interval).\n";
    }
  }

  return 0;
}
