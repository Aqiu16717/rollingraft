# Task Plan: 锁内 I/O 移出改造

## Goal

将 manager 锁内的大块磁盘/用户 I/O 移出，解除吞吐上限。review 记录的设计级遗留。

## 锁内 I/O 清单（已梳理）

| # | 位置 | I/O | 持有锁 | 量级 |
|---|------|-----|--------|------|
| A | `DoSnapshotLocked:58,110` | CreateSnapshot + SaveSnapshotStream | election+replication+snapshot | 100MB+ |
| B | `SendInstallSnapshotToPeerLocked:231` | CreateSnapshot（每次发送） | election+replication+snapshot | 大 |
| C | `HandleInstallSnapshot:602,677` | RestoreStream + SaveSnapshotStream | replication+snapshot+applier | 100MB+ |
| D | `HandleAppendEntries:387` | TruncateSuffix → FlushSync（1s） | replication | 小-中 |

## Phases

### Phase 1: 快照创建两阶段（A）
- [x] 设计：锁内检查触发点 → 解锁 → 锁外 CreateSnapshot+SaveSnapshotStream → 重新加锁应用
- [x] 正确性：ApplySnapshotLocked 验证（下台/过期快照跳过）；snapshot_in_progress_ 防重入；StateMachine 线程安全（Apply/Query 已并发）
- [x] 实现：ShouldTriggerSnapshotLocked / CreateAndPersistSnapshot / ApplySnapshotLocked / RunAutoSnapshotIfNeeded；TriggerSnapshot + CheckSnapshotTimeoutLocked 改两阶段

### Phase 2: 快照接收两阶段（C）
- [x] 设计：锁内捕获 done 信息 + 移交临时文件所有权 → 解锁 → 锁外 RestoreFromSnapshotFile + PersistSnapshotFile → 重锁 ApplySnapshot（done_index 过期检查）
- [x] 实现：RestoreFromSnapshotFile / PersistSnapshotFile helpers；HandleInstallSnapshot done 分支拆三阶段

### Phase 3: 截断移出 replication 锁（D）
- [x] 评估 → **保留现状（设计决策）**：TruncateSuffix 的 FlushSync 必须在 replication 锁内同步执行以保证顺序性（锁外执行时并发 AppendEntries 可能把截断点后的新条目 flush 落盘后被误删）；1s 超时是防御性上限，正常路径毫秒级

### Phase 4: 验证 + 提交
- [x] Release 全量 365/365 通过（Phase 1+2 各验证一轮）
- [x] TSan ×2：首轮 1 偶发竞态（网络层，见 findings.md，二次全绿 0 竞态）
- [x] 提交（bed8ccb）+ push（待 push）

## Next Step
Phase 1: 设计快照创建两阶段（读 DoSnapshotLocked 现状 + 快照阈值触发路径）

## Decisions Made
| Date | Decision |
|------|----------|
| 2026-08-10 | 从快照创建（A）开始，最大收益 |
| 2026-08-10 | B（发送侧 CreateSnapshot）并入 A 一并处理 |
