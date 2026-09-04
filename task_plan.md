# Task Plan: 稳定心跳合并指标测试

## Goal

消除 `MetricsShowHeartbeatCoalescing` 对周期心跳相位的偶然依赖，完成
Release/TSan 验证并安全发布节点 mTLS 提交。

## Phases

### Phase 1: 根因与 RED
- [x] 获取完整断言：未出现 `raft_heartbeat_coalesced_total`
- [x] 定向重复 20 次，第 19 次失败，确认时序波动
- [x] 确认 `d925046` 未修改失败测试的生产路径

### Phase 2: 确定性测试修复
- [x] 让测试在有界时间内持续发起 ReadIndex，直到观测到合并计数器
- [x] 定向压力验证（30/30）

### Phase 3: 全量验证与发布
- [x] `make format-check` 和 `git diff --check`
- [x] Release 396/396
- [x] TSan 396/396
- [x] 将节点测试证书改为构建时生成，避免发布私钥夹具历史
- [ ] 创建功能分支并提交 PR，不触碰 `third_party/leveldb` 和 `.codex/`

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| Release 中 `MetricsShowHeartbeatCoalescing` 失败 | 1 | 定向重复确认为心跳相位依赖，进入确定性等待修复 |

---

# 历史：Store 级 endpoints（已完成）

## Goal

补齐 multi-raft 模式下的控制平面端点（review #1 遗留）：/v1/status 聚合 + admin 端点按 group_id 路由。

## Phases

### Phase 1: 基础设施
- [x] MetricsHttpServer admin handler 签名加 group_id（POST body / DELETE ?group_id=）
- [x] RaftStoreConfig 加 admin_token 透传
- [x] RaftNodeImpl 加 GetLeaderId()

### Phase 2: Store 级 providers
- [x] RaftStore::Start 创建 store 级 metrics server + RegisterStoreProviders
- [x] /v1/status 聚合所有 group（public getter，不持 store 锁构建 JSON）
- [x] AddMember/RemoveMember/TriggerSnapshot/TransferLeadership 按 group_id 路由；config GET/PATCH store 全局

### Phase 3: 验证
- [x] Release 全量 366/366（1 已知 flaky 单跑通过）
- [x] TSan：0 竞态；BothGroupsElectIndependentLeaders 并行偶发（单跑 3/3 过，与已知网络层偶发竞态同族）
- [x] 提交 787969e + push

---
# 历史：锁内 I/O 移出改造

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

### Phase 1b: 发送侧快照准备移出锁（B）
- [x] 设计：SendInstallSnapshotToPeerLocked 检测需要快照 → 异步锁外 PrepareSnapshotForPeer（CreateSnapshot）→ 重锁装填 + 发送第一块
- [x] 防护：weak 守卫 timer；重锁后检查 role/peer 存在/in_progress/快照仍需要（next_index < first_index）
- [x] 实现 + 验证：365/365 通过（提交待 push）

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
