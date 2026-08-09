# Task Plan: 修复遗留 flaky 测试

## Goal

让 9 个本地不稳定的测试在本地 Release/TSan 下稳定通过（CI 全绿但本地时序敏感）。全部已知 flaky：
- `deterministic.ChaosTest.PartitionRecovery` / `DelayStorm` / `DuplicateAndReorder`
- `deterministic.ChaosScenariosTest.PartitionRecovery` / `DelayStorm` / `DuplicateMessages`
- `integration.MetricsEndpointTest.TransferLeadershipFromLeader`
- `unit.LogPersisterGroupCommitTest.SyncByBatchSizeThreshold`
- `integration.MultiRaft2GroupsTest.ReElectsAfterRestart`

## Phases

### Phase 1: 复现与分类
- [x] 逐个测试确认本地复现率（Release + TSan 各跑 N 次）
- [x] 分类：确定性问题（逻辑 bug）vs 时序问题（等待/轮询不足）vs 环境问题
- [x] 记录到 findings.md

### Phase 2: 确定性 Chaos 测试（6 个）
- [x] 通读 `tests/deterministic/`（simulated clock/network/timer）
- [x] 定位失败断言与根因
- [x] 修复 → **根因：Raft 超时 deadline 用真实时钟，模拟 tick 用模拟时钟，本地机器快导致选举从不触发。修复：TimerService::Now() 可注入 + 6 处超时检查改用 timer_->Now()。6/6 通过，全量 364/364 首次零失败**

### Phase 3: 其他 flaky（3 个）
- [ ] `TransferLeadershipFromLeader` — 领导权转移时序
- [ ] `SyncByBatchSizeThreshold` — group commit 并行时序
- [ ] `ReElectsAfterRestart` — multi-raft 重启竞态

### Phase 4: 验证
- [ ] Release 全量 ×3 连续通过
- [ ] TSan 全量 ×1 通过
- [ ] 提交 + push

## Next Step
Phase 1: 跑一轮全量确认当前 flaky 名单（今天的环境下哪些还挂）

## Decisions Made
| Date | Decision |
|------|----------|
| 2026-08-09 | 用 planning-with-files 流程跟踪本次任务 |
