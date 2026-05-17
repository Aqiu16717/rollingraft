# RollingRaft Deterministic Testing Mode Design

> Version: 0.1.0-draft
> Status: Design Phase
> Target: Task #22 Deterministic Testing Mode

## Overview

Provide a fully deterministic simulation environment so AI agents can:

1. **Test scenarios reproducibly** — same seed → same execution
2. **Control time** — pause, advance, or skip timers deterministically
3. **Control network** — drop, delay, duplicate, reorder messages
4. **Inject faults** — partition networks, crash nodes, corrupt packets
5. **Run chaos tests** — automated fault injection with invariants checking

## Architecture

```
┌─────────────────────────────────────────────┐
│            Agent Test Orchestrator          │
│  (controls time, network, fault injection)  │
└──────────────┬──────────────────┬───────────┘
               │                  │
    ┌──────────▼──────────┐      ▼
    │   SimulatedClock    │  SimulatedNetwork
    │   (deterministic)   │  (message queue)
    └──────────┬──────────┘      ▲
               │                  │
    ┌──────────▼──────────────────┴───────────┐
    │         RollingRaft Cluster              │
    │  (N nodes using SimulatedClock+Network)  │
    └──────────────────────────────────────────┘
```

## Components

### 1. SimulatedClock

Replaces `std::chrono::steady_clock` with a manual-advance clock:

```cpp
class SimulatedClock {
 public:
  using TimePoint = uint64_t;  // milliseconds

  TimePoint Now() const { return current_time_ms_; }

  // Advance time deterministically
  void Advance(uint64_t delta_ms);

  // Run until no pending timers or network messages
  void RunUntilIdle();

  // Run until a specific time
  void RunUntil(TimePoint target);

  // Register a callback to be called when time reaches a point
  void At(TimePoint when, std::function<void()> callback);

 private:
  std::atomic<uint64_t> current_time_ms_{0};
  std::map<uint64_t, std::vector<std::function<void()>>> scheduled_callbacks_;
};
```

**Integration with TimerService:**

Create `SimulatedTimerService` implementing the `TimerService` interface:

```cpp
class SimulatedTimerService : public TimerService {
 public:
  explicit SimulatedTimerService(SimulatedClock* clock);

  TimerId StartTimer(uint64_t delay_ms,
                     std::function<void()> callback) override;
  void CancelTimer(TimerId id) override;

 private:
  SimulatedClock* clock_;
  std::unordered_map<TimerId, std::function<void()>> timers_;
};
```

### 2. SimulatedNetworkTransport

Replaces `AsioNetworkTransport` with an in-process message router:

```cpp
struct SimulatedMessage {
  NodeId from;
  NodeId to;
  std::string payload;
  uint64_t send_time_ms;
  uint64_t deliver_time_ms;  // Can be delayed
};

class SimulatedNetwork {
 public:
  void RegisterEndpoint(NodeId id, NetworkTransport* transport);
  void UnregisterEndpoint(NodeId id);

  // Send message (goes through network rules before delivery)
  void Send(NodeId from, NodeId to, const std::string& payload);

  // Delivery control
  void DeliverAll();           // Deliver all pending messages
  void DeliverOne();           // Deliver exactly one message
  void DeliverUntil(uint64_t time_ms);  // Deliver messages up to time

  // Fault injection
  void Partition(NodeId a, NodeId b);        // Drop all messages between a,b
  void HealPartition(NodeId a, NodeId b);    // Restore connectivity
  void DropNext(float probability);          // Random drop (with seed)
  void DelayAll(uint64_t delay_ms);          // Add fixed delay
  void DelayNext(uint64_t delay_ms);         // Delay single next message
  void DuplicateNext(float probability);     // Duplicate messages
  void ReorderMessages(bool enable);         // Shuffle delivery order

  // Network state query
  bool IsConnected(NodeId a, NodeId b) const;
  size_t PendingMessageCount() const;

 private:
  SimulatedClock* clock_;
  std::unordered_map<NodeId, NetworkTransport*> endpoints_;
  std::vector<SimulatedMessage> pending_messages_;
  std::set<std::pair<NodeId, NodeId>> partitions_;
  std::mt19937 rng_;  // Seeded RNG for deterministic behavior
};
```

### 3. TestCluster Fixture

High-level fixture for writing deterministic tests:

```cpp
class TestCluster {
 public:
  struct Options {
    size_t num_nodes = 3;
    uint64_t seed = 42;
    uint32_t election_timeout_ms = 300;
    uint32_t heartbeat_interval_ms = 50;
  };

  explicit TestCluster(const Options& options);

  // Lifecycle
  void StartAll();
  void StopAll();
  void StartNode(NodeId id);
  void StopNode(NodeId id);
  void CrashNode(NodeId id);   // Immediate stop, no graceful shutdown
  void RestartNode(NodeId id); // Restart with same state

  // Time control
  void AdvanceTime(uint64_t ms);
  void RunUntilLeaderElected();
  void RunUntilCommit(Index index);
  void RunFor(uint64_t ms);

  // Network control
  void Partition(std::vector<NodeId> partition_a,
                 std::vector<NodeId> partition_b);
  void HealAllPartitions();
  void DropMessages(float probability);

  // Assertions
  void AssertNoLeader();                    // All followers
  void AssertSingleLeader();                // Exactly one leader
  void AssertCommitted(Index index);        // Index committed by all
  void AssertAllApplied(Index index);       // Index applied by all
  void AssertStateMachineEqual();           // All state machines match

  // Queries
  RaftNodeRole GetRole(NodeId id) const;
  NodeId GetLeaderId() const;
  Index GetCommitIndex(NodeId id) const;
  std::vector<NodeId> GetLeaderIds() const; // Should be 0 or 1

 private:
  SimulatedClock clock_;
  SimulatedNetwork network_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
  std::vector<std::shared_ptr<MockStateMachine>> state_machines_;
};
```

## Example: Deterministic Leader Election Test

```cpp
TEST(DeterministicRaft, LeaderElectionAfterPartition) {
  TestCluster cluster({.num_nodes = 5, .seed = 12345});
  cluster.StartAll();

  // Run until initial leader is elected
  cluster.RunUntilLeaderElected();
  NodeId leader = cluster.GetLeaderId();
  ASSERT_NE(leader, -1);

  // Partition: isolate leader from majority
  cluster.Partition({leader}, {1, 2, 3, 4});

  // Advance time past election timeout
  cluster.AdvanceTime(400);

  // Majority partition should elect new leader
  cluster.RunUntilLeaderElected();
  NodeId new_leader = cluster.GetLeaderId();
  ASSERT_NE(new_leader, leader);

  // Heal partition
  cluster.HealAllPartitions();
  cluster.AdvanceTime(100);

  // Old leader should step down
  cluster.AssertSingleLeader();
}
```

## Example: Chaos Test with Invariants

```cpp
TEST(DeterministicRaft, ChaosTest) {
  TestCluster cluster({.num_nodes = 5, .seed = 42});
  cluster.StartAll();
  cluster.RunUntilLeaderElected();

  // Propose 100 commands
  for (int i = 0; i < 100; ++i) {
    cluster.ProposeToLeader(fmt::format("cmd_{}", i));
  }

  // Randomly inject faults during execution
  for (int round = 0; round < 50; ++round) {
    cluster.AdvanceTime(10);
    cluster.RandomFaultInjection(/*probability=*/0.1);
  }

  // Heal everything and let cluster stabilize
  cluster.HealAllPartitions();
  cluster.DropMessages(0.0f);
  cluster.RunUntilCommit(100);

  // Invariants
  cluster.AssertSingleLeader();
  cluster.AssertStateMachineEqual();
  cluster.AssertCommitted(100);
}
```

## Integration with Existing Code

The existing `RaftNodeConfig` already supports factory injection:

```cpp
struct RaftNodeConfig {
  // ... existing fields ...

  // Factory functions for dependency injection (testing)
  std::function<std::unique_ptr<NetworkTransport>()> network_factory = nullptr;
  std::function<std::unique_ptr<TimerService>()> timer_factory = nullptr;
  std::function<std::unique_ptr<Persister>()> persister_factory = nullptr;
  std::function<std::unique_ptr<Protocol>()> protocol_factory = nullptr;
};
```

**Minimal changes needed:**
1. Create `SimulatedTimerService` implementing `TimerService`
2. Create `SimulatedNetworkTransport` implementing `NetworkTransport`
3. Create `TestCluster` fixture tying everything together
4. Use `MockStateMachine`, `MockPersister` from existing test code

## File Layout

```
tests/deterministic/
├── simulated_clock.h
├── simulated_clock.cpp
├── simulated_timer_service.h
├── simulated_timer_service.cpp
├── simulated_network.h
├── simulated_network.cpp
├── test_cluster.h
├── test_cluster.cpp
└── deterministic_test.cpp       # Example tests
```

## Benefits for Agent Development

1. **Reproducible bugs** — deterministic execution means agents can reproduce race conditions
2. **Fast tests** — no real network or timers, tests run in milliseconds
3. **Extensive coverage** — easy to simulate network partitions, crashes, delays
4. **Property-based testing** — agents can generate random fault sequences and verify invariants
5. **CI friendly** — no external dependencies, runs in any environment
