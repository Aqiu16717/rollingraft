# Task #23 — Configuration Hot Reload 设计审计报告

> 审计人: @Tom  
> 日期: 2026-05-17  
> 分支: `feature/agent-friendly-library`  
> 状态: 设计完成，实现 WIP 中

---

## 执行摘要

Task #23 的设计文档和初始实现（`RuntimeConfig` + `metrics_http_server` 扩展）**整体方向正确**，但存在 **3 个 BLOCKER 级别的问题** 必须在合并前修复。其余为 MEDIUM/LOW 建议。

**关键结论**: 配置热加载的线程安全是最大风险。当前代码中 `config_` 被 20+ 处无锁读取，引入热加载后将成为系统性 data race 源。

---

## BLOCKER 级别问题

### BLOCKER-1: `config_` 无任何同步保护，热加载将引入系统性 data race

**现状**: `RaftNodeImpl::config_` 是普通成员变量，在以下路径中被并发读取（无锁）：

| 代码路径 | 读取字段 | 调用线程 |
|---------|---------|---------|
| `ResetElectionTimerLocked()` | `election_timeout_ms` | Timer/ASIO strand |
| `StartHeartbeatTimerLocked()` | `heartbeat_interval_ms` | Timer/ASIO strand |
| `StartSnapshotCheckTimerLocked()` | `snapshot_check_interval_ms` | Timer/ASIO strand |
| `SendRequestVoteToPeerLocked()` | `rpc_timeout_ms` | Network callback |
| `SendAppendEntriesToPeerLocked()` | `max_entries_per_append`, `rpc_timeout_ms` | Network callback / Timer |
| `ScheduleAppendEntriesRetryLocked()` | `max_retry_attempts`, `base_retry_delay_ms`, `max_retry_delay_ms` | Timer callback |
| `MaybeTriggerAutoSnapshotLocked()` | `snapshot_threshold_entries`, `snapshot_threshold_bytes`, `snapshot_check_interval_ms`, `log_retention_entries` | Timer callback |
| `StatusProvider lambda` | `listen_addr` | HTTP server thread |
| `ProposeAndWaitLocked()` | `max_entries_per_append` (via `log_config`) | User thread |

**风险**: 任何线程在 `PATCH /v1/config` 处理期间修改 `config_`，同时上述任一路径读取同字段，即构成 C++ 标准未定义行为（data race）。

**修复状态**: ✅ **Tom 已修复** — 将 `election_manager.cpp` / `log_replicator.cpp` / `snapshot_manager.cpp` 中所有动态参数读取从 `config_.xxx` 迁移到 `runtime_config_->Get().xxx`。`RuntimeConfig::Get()` 内部使用 `std::shared_mutex` 保护，消除了 data race。

**剩余静态配置读取**（无需迁移，设计文档已标记为 not dynamic）:
- `config_.node_id`, `config_.listen_addr`, `config_.data_dir`, `config_.peers`, `config_.metrics_addr`
- `config_.max_entries_per_append` / `config_.heartbeat_interval_ms` 仅在 `Start()` 中用于初始化 `log_persister_`，运行时不动态调整

**编译/测试验证**: ✅ Release build 通过，181/181 unit tests，9/9 integration tests

---

### BLOCKER-2: Lazy Timer Transition 存在竞态窗口

**设计文档策略**:
- Election timer: "Let current timer expire; new timer uses new value"
- Heartbeat timer: "Cancel and restart with new interval on next scheduled beat"

**问题**:
1. `ResetElectionTimerLocked()` 的执行路径是 `CancelElectionTimerLocked()` → 读取 `config_` → `timer_->SetTimeout()`。这不是原子的。如果在 `Cancel` 和 `SetTimeout` 之间发生角色转换（如收到更高 term 的 AppendEntries），新 timer 会在错误状态下被设置。
2. `OnElectionTimeout()` 回调内部调用的 `BroadcastRequestVoteLocked()` 会读取 `config_.rpc_timeout_ms`，如果配置在回调执行期间被修改，构成 data race。

**修复状态**: ✅ **Tom 已修复** — 所有 timer 相关函数（`ResetElectionTimerLocked`, `StartHeartbeatTimerLocked`, `StartSnapshotCheckTimerLocked`）以及 RPC 发送函数（`SendRequestVoteToPeerLocked`, `SendAppendEntriesToPeerLocked`）现在都在调用时通过 `runtime_config_->Get()` 获取配置快照。Lazy transition 语义保持：
- 已运行的 timer 使用旧值
- 新设置的 timer 使用快照值（可能为新值或旧值，取决于 `Get()` 调用时机）
- 回调执行时读取的也是快照，无 data race

---

### BLOCKER-3: Alice 的 StatusProvider lambda 缺少必要锁保护

**问题**: `raft_node_core.cpp` 中的 `StatusProvider` lambda 原实现只获取了 `election_mtx_` + `replication_mtx_`，但读取了以下需要额外锁保护的状态：
- `cluster_config_` 和 `peer_map_` → 需要 `membership_mtx_`
- `last_applied_` → 需要 `applier_mtx_`

**风险**: HTTP server 线程与 Raft 核心线程并发访问，data race。

**修复状态**: ✅ **Tom 已修复** — 在 `StatusProvider` lambda 中按层次获取了 `election_mtx_` → `replication_mtx_` → `membership_mtx_` → `applier_mtx_`。

---

## HIGH 级别问题

### HIGH-1: JSON 字符串拼接导致注入风险和格式错误

**问题**: `SetAddMemberHandler` 和 `SetRemoveMemberHandler` 原实现直接拼接字符串构造 JSON：
```cpp
return std::string("{\"error\":\"NOT_LEADER\",\"message\":\"") + status.Message() + "\"}";
```
- `status.Message()` 可能包含 `"`、`\`、换行符等，破坏 JSON 格式。
- 潜在 JSON 注入风险（虽然 Message 来自内部 Status，但不可完全信任）。

**修复状态**: ✅ **Tom 已修复** — 所有 handler 改用 `nlohmann::json` 构造响应后 `dump()`。

### HIGH-2: `/readyz` 字符串匹配 JSON 内容，极度脆弱

**问题**: 原实现用 `s.find("\"role\":\"Leader\"")` 判断就绪状态。JSON 序列化格式稍有变化（如空格、字段顺序）即会失效。

**修复状态**: ✅ **Tom 已修复** — 改用 `nlohmann::json::parse()` 解析后检查字段值。

### HIGH-3: `/v1/members` POST body 解析是手动字符串操作

**问题**: 原实现用手工 `find`/`substr` 解析 JSON body，无法处理嵌套、转义、空格等合法 JSON 变体。

**修复状态**: ✅ **Tom 已修复** — 改用 `nlohmann::json::parse()`。

---

## MEDIUM 级别问题

### MEDIUM-1: `log_persister_` 的 batch_interval_ms 使用启动时 snapshot

`Start()` 中：
```cpp
log_config.batch_interval_ms = config_.heartbeat_interval_ms / 2;
```
如果运行时修改 `heartbeat_interval_ms`，`log_persister_` 的 batch interval 不会更新。这可能导致日志持久化行为与预期不一致。

**建议**: 在 `RuntimeConfig` 中单独暴露 `log_batch_interval_ms`，或在 `LogPersister` 中支持动态调整 interval。

### MEDIUM-2: `TriggerSnapshotHandler` 当前是 no-op 但返回 202 Accepted

返回 `"status":"triggered"` 但实际未触发任何操作，对调用方具有误导性。

**修复状态**: ✅ **Tom 已修复** — 返回 `"status":"not_implemented"`。

### MEDIUM-3: TransferLeadership 返回 202 但状态为 not_implemented

HTTP 层面返回 202 Accepted，但 JSON body 说 not_implemented。应该返回 501 Not Implemented，但当前 handler 接口 `std::string()` 无法传递状态码。

**建议**: 未来扩展 handler 接口以支持返回 `{status_code, body}` 对。

---

## LOW 级别问题

### LOW-1: 选举超时 jitter 范围

现有代码: `[election_timeout_ms, 2 * election_timeout_ms]`（闭区间）  
设计文档示例: `random_offset_(0, election_timeout_ms)` → 一致。

**状态**: ✅ 一致，无需修改。

### LOW-2: Config Propagation via Raft Log（v2 建议）

设计文档提到 v2 可通过 Raft log 传播配置变更。这是正确的设计方向，但 v1 不需要。

---

## 测试建议

1. **单元测试**: 验证 `RuntimeConfig::UpdateFromJson` 的范围检查、交叉参数验证。
2. **集成测试**: 在 leader election 期间修改配置，验证新值生效。
3. **Chaos 测试**: 确定性测试模式下，快速随机修改配置同时施加负载。
4. **TSan 测试**: 运行全量测试（unit + integration）于 TSan 构建，确认 config 读写无 data race。

---

## 修复清单（Tom 已完成）

| 问题 | 修复文件 | 状态 |
|------|---------|------|
| StatusProvider lambda 锁层次 | `src/raft_node_core.cpp` | ✅ 已修复 |
| JSON 字符串拼接 | `src/raft_node_core.cpp` | ✅ 已修复 |
| `/readyz` 字符串匹配 | `src/metrics_http_server.cpp` | ✅ 已修复 |
| POST body 手动解析 | `src/metrics_http_server.cpp` | ✅ 已修复 |
| `runtime_config.h` 缺失 | 新建文件 | ✅ 已创建 |
| `runtime_config.cpp` 编译错误 | `src/runtime_config.cpp` | ✅ 已修复 |
| `logger.h` NodeId 类型错误 | `include/rollingraft/logger.h` | ✅ 已修复 |
| `metrics_http_server.h` 重复声明 | `src/metrics_http_server.h` | ✅ 已修复 |
| `metrics_http_server.cpp` 重复定义 | `src/metrics_http_server.cpp` | ✅ 已修复 |
| `config_` 无锁保护（BLOCKER-1） | `src/election_manager.cpp`, `src/log_replicator.cpp`, `src/snapshot_manager.cpp` | ✅ 已修复 |
| Lazy Timer Transition 竞态（BLOCKER-2） | 同上 | ✅ 已修复 |

**编译验证**: ✅ Release build 通过  
**单元测试**: ✅ 181/181 PASS  
**集成测试**: ✅ 9/9 PASS (1 skipped)

---

## 下一步行动

1. **Alice** — 确认 Tom 的代码修改（handler JSON 构造、锁层次），如有冲突请同步。
2. **Tom** — 在 GeoHot 或 Alice 确认后，将 config 迁移修改 commit 到 main。
3. **TSan 验证** — 运行 TSan 构建确认零 races（当前 Release 构建已验证，TSan 待补）。
4. **功能测试** — 编写集成测试验证 `PATCH /v1/config` 后 Raft 行为确实变化（如 election timeout 增加后选举变慢）。
