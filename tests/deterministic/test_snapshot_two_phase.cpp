/**
 * @file test_snapshot_two_phase.cpp
 * @brief Deterministic regression tests for the two-phase snapshot refactor
 *
 * The two-phase snapshot refactor (unlocked creation, locked apply) opened a
 * window in which the log can advance past the snapshot index between
 * CreateSnapshot and ApplySnapshotLocked. Applying such a stale snapshot
 * wipes committed entries (SetStartIndex clears the whole in-memory log) and
 * reuses their indices. These tests hold the window open deterministically
 * and assert committed entries survive.
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "deterministic/test_cluster.h"
#include <gtest/gtest.h>

using namespace rollingraft;

namespace {

// Captures the mock snapshot FIRST (at the log index when TriggerSnapshot is
// called), then blocks until the gate opens. This holds the two-phase window
// open with a stale snapshot while the test advances the log past its index.
class BlockingSnapshotMachine : public MockStateMachine {
 public:
  static void CloseGate() {
    std::lock_guard<std::mutex> lock(gate_mtx_);
    started_.store(false, std::memory_order_release);
    gate_closed_.store(true, std::memory_order_release);
  }

  static void OpenGate() {
    std::lock_guard<std::mutex> lock(gate_mtx_);
    gate_closed_.store(false, std::memory_order_release);
    gate_cv_.notify_all();
  }

  static bool Started() { return started_.load(std::memory_order_acquire); }

  std::shared_ptr<Snapshot> CreateSnapshot() override {
    auto snapshot = MockStateMachine::CreateSnapshot();  // Captured at the trigger index
    std::unique_lock<std::mutex> lock(gate_mtx_);
    started_.store(true, std::memory_order_release);
    gate_cv_.wait(lock, [] { return !gate_closed_.load(std::memory_order_acquire); });
    return snapshot;
  }

 private:
  static std::mutex gate_mtx_;
  static std::condition_variable gate_cv_;
  static std::atomic<bool> gate_closed_;
  static std::atomic<bool> started_;
};

std::mutex BlockingSnapshotMachine::gate_mtx_;
std::condition_variable BlockingSnapshotMachine::gate_cv_;
std::atomic<bool> BlockingSnapshotMachine::gate_closed_{false};
std::atomic<bool> BlockingSnapshotMachine::started_{false};

}  // namespace

// One behavior: entries committed while a snapshot is being created must not
// be wiped by the snapshot apply (no index reuse, no lost commits).
TEST(SnapshotTwoPhaseTest, CommittedEntriesSurviveSlowSnapshotCreation) {
  TestCluster::Options options;
  options.election_timeout_ms = 300;
  options.heartbeat_interval_ms = 20;
  options.state_machine_factory = [] { return std::make_shared<BlockingSnapshotMachine>(); };
  TestCluster cluster(options);
  cluster.StartAll();
  cluster.RunUntilLeaderElected();
  cluster.AssertSingleLeader();

  // Baseline: 5 committed entries.
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(cluster.ProposeToLeader("base_" + std::to_string(i)).ok());
    cluster.RunUntilCommit(i + 1);
  }
  cluster.RunUntilIdle();

  // Start a manual snapshot. CreateSnapshot captures at index 5, then blocks
  // inside the gate — the two-phase window is now open with a stale snapshot.
  BlockingSnapshotMachine::CloseGate();
  std::thread trigger_thread([&cluster]() {
    NodeId leader = cluster.GetLeaderId();
    if (leader >= 0) {
      (void)cluster.GetNode(leader)->TriggerSnapshot();
    }
  });

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!BlockingSnapshotMachine::Started() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(BlockingSnapshotMachine::Started()) << "snapshot creation never started";

  // Commit 5 more entries while the window is open: log advances to 10 with
  // the pending snapshot covering only 5.
  for (int i = 5; i < 10; ++i) {
    ASSERT_TRUE(cluster.ProposeToLeader("late_" + std::to_string(i)).ok());
    cluster.RunUntilCommit(i + 1);
  }
  cluster.RunUntilIdle();

  // Release the gate. The stale snapshot@5 must be skipped (log advanced);
  // applying it would wipe entries 6-10 and reuse their indices.
  BlockingSnapshotMachine::OpenGate();
  trigger_thread.join();
  cluster.RunUntilIdle();

  cluster.AssertAllApplied(10);
  cluster.AssertStateMachineEqual();

  // The leader must remain able to commit after the skipped apply — a new
  // entry lands at 11, never reusing a wiped index.
  ASSERT_TRUE(cluster.ProposeToLeader("after_snapshot").ok());
  cluster.RunUntilCommit(11);
  cluster.AssertAllApplied(11);
  cluster.AssertStateMachineEqual();
}
