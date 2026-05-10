# RollingRaft 测试策略

## 目标

* 验证 Raft 核心逻辑正确性（选举、日志复制、快照）
* 确保日志持久化可靠（崩溃恢复场景）
* 提供快速反馈的自动化测试

---

## 测试分层

```
┌─────────────────────────────────────┐
│  E2E 测试 (5%)                      │
│  * 3 节点集群功能验证               │
│  * Counter 示例端到端测试           │
├─────────────────────────────────────┤
│  集成测试 (15%)                     │
│  * 网络分区处理                     │
│  * Leader 故障恢复                  │
│  * 日志持久化恢复                   │
├─────────────────────────────────────┤
│  单元测试 (80%)                     │
│  * RaftNode 逻辑（Mock 依赖）       │
│  * LogPersister 批量写入            │
│  * 序列化/反序列化                  │
└─────────────────────────────────────┘
```

---

## Mock 组件

| Mock 类 | 用途 | 位置 |
|---------|------|------|
| MockNetworkTransport | 模拟网络延迟/丢包/分区 | tests/mock/mock_network.h |
| MockTimerService | 手动控制时间推进 | tests/mock/mock_timer.h |
| MockPersister | 模拟磁盘故障 | tests/mock/mock_persister.h |
| MockStateMachine | 验证 Apply 调用 | tests/mock/mock_state_machine.h |

---

## 核心测试用例

### 1. 选举测试

```cpp
TEST_F(RaftNodeTest, ElectionTimeout_BecomesCandidate) {
  auto node = CreateNode(1, {2, 3});
  node->Start();
  
  // 推进选举超时
  timer_->Advance(std::chrono::milliseconds(350));
  
  EXPECT_EQ(node->GetRole(), RaftNodeRole::CANDIDATE);
  EXPECT_EQ(node->CurrentTerm(), 1);
}

TEST_F(RaftNodeTest, MajorityVotes_BecomesLeader) {
  auto node = CreateNode(1, {2, 3});
  node->Start();
  
  timer_->Advance(std::chrono::milliseconds(350));
  
  // 模拟收到两张选票
  ReceiveVoteResponse(2, /*term=*/1, /*granted=*/true);
  ReceiveVoteResponse(3, /*term=*/1, /*granted=*/true);
  
  EXPECT_EQ(node->GetRole(), RaftNodeRole::LEADER);
}
```

### 2. 日志复制测试

```cpp
TEST_F(RaftNodeTest, Propose_AppendsToLog) {
  BecomeLeader();
  
  node_->Propose("cmd1", [](auto){});
  
  EXPECT_EQ(GetLogSize(), 1);
  EXPECT_EQ(GetLastLogTerm(), CurrentTerm());
}

TEST_F(RaftNodeTest, ReplicateToMajority_BeforeCommit) {
  BecomeLeader();
  
  bool committed = false;
  node_->Propose("cmd1", [&](auto result) {
    committed = result.success;
  });
  
  // 未复制到多数前不应提交
  EXPECT_FALSE(committed);
  
  // 模拟两个 follower 确认
  ReceiveAppendResponse(2, /*success=*/true);
  ReceiveAppendResponse(3, /*success=*/true);
  
  EXPECT_TRUE(committed);
}
```

### 3. 日志持久化测试

```cpp
TEST_F(LogPersisterTest, BatchFlush_WritesToDisk) {
  LogPersister persister(CreateMockPersister());
  persister.Start();
  
  // 追加 50 条日志
  for (int i = 1; i <= 50; ++i) {
    persister.Append(MakeEntry(i));
  }
  
  // 等待批量写入
  persister.FlushSync();
  
  EXPECT_EQ(mock_persister_->GetWriteCount(), 1);
  EXPECT_EQ(mock_persister_->GetEntryCount(), 50);
}

TEST_F(LogPersisterTest, CrashRecovery_RestoresLogs) {
  // Phase 1: 写入日志
  {
    LogPersister persister(CreateLevelDBPersister());
    persister.Start();
    for (int i = 1; i <= 100; ++i) {
      persister.Append(MakeEntry(i));
    }
    persister.Stop();  // 正常刷盘
  }
  
  // Phase 2: 重启恢复
  {
    LogPersister persister(CreateLevelDBPersister());
    auto entries = persister.Restore(1);
    
    EXPECT_EQ(entries.size(), 100);
    EXPECT_EQ(entries.back().index_, 100);
  }
}
```

### 4. 集成测试

```cpp
TEST_F(ClusterTest, NetworkPartition_ElectsNewLeader) {
  StartCluster(5);  // 5 节点集群
  
  auto* leader = GetLeader();
  ASSERT_NE(leader, nullptr);
  
  // 网络分区: Leader 与 2 个 Follower 隔离
  network_->Partition({leader->Id()}, {2, 3, 4, 5});
  
  // 等待新 Leader 选举（旧 Leader 所在分区不足多数）
  WaitForLeader(timeout_sec: 5, excluded: leader->Id());
  
  // 恢复网络
  network_->HealPartition();
  
  // 旧 Leader 应退位
  WaitFor([&] { return !leader->IsLeader(); });
}
```

---

## 目录结构

```
tests/
├── CMakeLists.txt
├── mock/
│   ├── mock_network.h/cpp
│   ├── mock_timer.h/cpp
│   ├── mock_persister.h/cpp
│   └── mock_state_machine.h
├── unit/
│   ├── test_raft_election.cpp
│   ├── test_raft_log_replication.cpp
│   ├── test_raft_snapshot.cpp
│   ├── test_log_persister.cpp
│   └── test_json_protocol.cpp
└── integration/
    ├── test_cluster_3nodes.cpp
    ├── test_leader_failure.cpp
    └── test_log_recovery.cpp
```

---

## 实施计划

| 阶段 | 内容 | 时间 |
|------|------|------|
| 1 | Mock 组件实现 | 1 day |
| 2 | 选举/日志单元测试 | 2 days |
| 3 | 持久化测试 | 1 day |
| 4 | 集成测试 | 2 days |
| 5 | CI 集成 | 1 day |

---

## 运行测试

```bash
# 构建测试
cmake -B build -DBUILD_TESTING=ON
cmake --build build

# 运行所有测试
ctest --test-dir build --output-on-failure

# 运行特定测试
./build/tests/unit_tests --gtest_filter="RaftNodeTest.Election*"
```
