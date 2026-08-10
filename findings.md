# Findings

## 偶发竞态：TcpConnection pending_callbacks_（2026-08-10 记录）

两阶段快照改造后 TSan 全量首跑报 1 个竞态（MultiRaft2GroupsTest.BothGroupsElectIndependentLeaders 触发，Subprocess aborted + LeaderCanProposeInEachGroup timeout）。单跑不触发；第二次 TSan 全量 0 竞态全绿。

竞态栈：Read 在 `TcpConnection::ExtractCallbackLocked`（HandleMessage，io 线程），Previous write 在 `Send` 的 `timer->async_wait` 注册（io 线程）。代码审查未发现 pending_callbacks_ 的无锁访问（所有读写都在 mutex_ 内）；TSan 报 size-1 字段疑似 asio 库内部状态与 TcpConnection 布局的边界情况。

**判断**：网络层 pre-existing 偶发竞态，两阶段快照改变的调度时序使其暴露一次。无法稳定复现，代码审查无无锁访问 → 记录待查，不阻塞。

## 已解决：确定性 Chaos 测试（2026-08-09）

## 已解决：确定性 Chaos 测试（2026-08-09）

**根因**：`OnStoreTick`/`CheckHeartbeatTimeoutLocked`/`CheckSnapshotTimeoutLocked` 的 deadline 比较用 `std::chrono::steady_clock::now()`（真实时钟），而 tick 由 `SimulatedTimerService::SetInterval`（模拟时钟）驱动。模拟测试中模拟时间快速推进（10000ms 模拟时间 = 几毫秒真实时间），真实时钟从未走过 election_timeout → 选举超时永不触发 → 无 leader。CI 机器慢，循环超过 300ms 真实时间 → 碰巧通过。

**证据**：失败运行日志 0 次 "became Candidate"。

**修复**：`TimerService` 增加虚方法 `Now()`（默认真实时钟）；`SimulatedTimerService` 覆写返回模拟时间；RaftNodeImpl 6 处超时 deadline 设置/比较改用 `timer_->Now()`（election ×2、heartbeat ×2、snapshot ×2）。`log_replicator.cpp:39` 的 `+= interval` 基于同一原点无需改。

**验证**：6 个 chaos 测试全过；全量 364/364 首次零失败。

## 已知信息（来自 review 历史）
- 9 个测试本地不稳，CI 全绿
- 共同模式：单跑通过，全量并行跑挂 → 疑似时序/环境敏感
- 之前验证过的 flake 证据：
  - `SyncByBatchSizeThreshold`：TSan 全量挂、单跑 3/3 过（2026-08-01, 08-04, 08-06 多次）
  - `ReElectsAfterRestart`：TSan 全量挂、单跑 3/3 过（2026-08-02）
  - `MetricsShowHeartbeatCoalescing`：TSan 全量挂、单跑 2/2 过（2026-08-07）——这个是心跳时序窗口，曾与 TransferLeadership 混淆
- 本地 7 个混沌测试必挂但 CI 全绿 → 本地 vs CI 环境差异（2026-07-29 起记录）
