# RollingRaft Snapshot 路径深度审查报告

**审查日期**: 2025-06-09  
**审查范围**: HandleInstallSnapshot、SaveSnapshotStream/RestoreStream、TriggerSnapshot、Leader-side snapshot send  
**审查人**: Tom  

---

## 执行摘要

对 Snapshot 全链路进行深度审查，发现 **2 个 Medium Severity** 问题、**1 个 Low Severity** 建议。**无 P0/P1** 级别问题。

| # | 问题 | 位置 | 严重度 |
|---|------|------|--------|
| 1 | `snapshot_sends_` 在 `BecomeLeaderLocked` 中未清理 | `election_manager.cpp:135-165` | 🟡 Medium |
| 2 | `HandleInstallSnapshot` 异常路径未清理 temp 文件 | `rpc_handlers.cpp:551-559` | 🟡 Medium |
| 3 | `SaveSnapshotStream` 在 snapshot 进行中时新的 `DeleteSnapshotDataLocked` | `state_persister.cpp:330` | 🟢 Low |

---

## 🟡 Medium Severity

### SR1. `snapshot_sends_` 在 `BecomeLeaderLocked` 中未清理

**位置**: `src/election_manager.cpp:135-165`  
**代码**:
```cpp
void RaftNode::RaftNodeImpl::BecomeLeaderLocked() {
  // ...
  next_index_.clear();
  match_index_.clear();
  retry_state_.clear();
  inflight_.clear();
  client_sessions_.clear();
  quorum_acks_.clear();
  last_contact_time_.clear();
  // ❌ snapshot_sends_.clear() 缺失！
}
```

**问题**: 旧 leader 在发送 snapshot 给 peer 时 step down，`snapshot_sends_[peer]` 中 `in_progress = true`。重新当选后：

1. `log_replicator.cpp:203` 调用 `SendInstallSnapshotToPeerLocked(peer)`
2. `snapshot_manager.cpp:215` 检查 `if (state.in_progress) return;` — 跳过发送！
3. peer 永远收不到 snapshot，导致 log 无法同步

**影响**: Leader 重新当选后，之前正在接收 snapshot 的 peer 可能永远停滞在落后状态。

**建议**: 在 `BecomeLeaderLocked` 中添加 `snapshot_sends_.clear();`，与 `inflight_.clear()` 保持一致。

---

### SR2. `HandleInstallSnapshot` 异常路径未清理 temp 文件

**位置**: `src/rpc_handlers.cpp:517-559`  
**代码**:
```cpp
if (req.done_) {
    auto restore_provider = [&](std::string& chunk) -> bool {
        // ... 可能抛异常（std::bad_alloc、文件 I/O 错误）
    };
    if (!state_machine_->RestoreStream(restore_provider)) {
        // ✅ 正常失败路径：清理 temp 文件
        std::remove(snapshot_temp_path_.c_str());
        snapshot_temp_path_.clear();
        return;
    }
    // ...
}
```

**问题**: `RestoreStream` 内部调用 `restore_provider` lambda。如果 lambda 抛异常（如 `std::bad_alloc`、`std::ios_base::failure`）：
- `restore_ifs`（`shared_ptr<std::ifstream>`）会 RAII 析构 ✅
- 但 `snapshot_temp_path_` 的显式清理在异常路径中**不会执行** ❌
- temp 文件永久残留在 `/tmp`

**影响**: 高频 snapshot 场景（如大 state machine + 网络抖动）可能导致 `/tmp` 磁盘空间泄漏。

**建议**: 使用 RAII wrapper 管理 temp 文件生命周期：
```cpp
struct TempFileGuard {
    std::string& path;
    ~TempFileGuard() {
        if (!path.empty()) {
            std::remove(path.c_str());
            path.clear();
        }
    }
};
```

或在 `HandleInstallSnapshot` 外层包裹 try-catch：
```cpp
if (req.done_) {
    try {
        // ... restore and persist ...
    } catch (...) {
        if (!snapshot_temp_path_.empty()) {
            std::remove(snapshot_temp_path_.c_str());
            snapshot_temp_path_.clear();
        }
        throw;
    }
    // cleanup
}
```

---

## 🟢 Low Severity

### SR3. `SaveSnapshotStream` 在 snapshot 进行中时新的 `DeleteSnapshotDataLocked`

**位置**: `src/state_persister.cpp:330`  
**代码**:
```cpp
Status StatePersister::SaveSnapshotStream(...) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  DeleteSnapshotDataLocked();  // ← 先删除旧 snapshot
  // ... 写入新 snapshot chunks ...
}
```

**问题**: 如果 `SaveSnapshotStream` 在写入新 snapshot 的过程中失败（如磁盘满），旧 snapshot 已经被删除，导致：
- 新 snapshot 不完整
- 旧 snapshot 丢失
- 节点重启后无有效 snapshot

**影响**: Low。`SaveSnapshotStream` 是 leader 调用的（`DoSnapshotLocked`），如果失败，leader 会在下次触发 snapshot 时重试。Follower 的 snapshot 是从 leader 接收的，如果 follower 端 persist 失败，旧 snapshot 仍然存在（follower 使用 `persister_->SaveSnapshotStream`，而 `persister_` 在 follower 端当前仍然是 `LevelDBPersister` 或 `HybridPersister`）。

**建议**: 使用写时复制（write-to-temp-then-rename）策略，或至少确保 `DeleteSnapshotDataLocked` 在新 snapshot 完全写入后才执行。但考虑到 LevelDB 的 WriteBatch 原子性和当前使用场景，此问题优先级较低。

---

## ✅ 验证通过的项

| # | 检查项 | 结论 |
|---|--------|------|
| 1 | Snapshot 过程中 leader 切换 | ✅ 安全。`HandleInstallSnapshot` Phase 1 的 term 检查拒绝旧 leader 的 chunk |
| 2 | `TriggerSnapshot` 与 `AppendEntries` 并发 | ✅ 安全。两者持有相同的锁层次（election → replication → snapshot） |
| 3 | SaveSnapshotStream chunk 大小边界 | ✅ 安全。Chunk 大小由调用者控制为 64KB |
| 4 | Snapshot temp 文件命名唯一性 | ✅ 安全。`<server_id>_<index>_<term>` 三元组确保唯一 |
| 5 | Leader-side snapshot chunk 顺序 | ✅ 安全。`SendNextSnapshotChunkLocked` 按 offset 递增发送，TCP 保证顺序 |

---

## 总结

| # | 问题 | 严重度 | 建议修复 |
|---|------|--------|----------|
| 1 | `BecomeLeaderLocked` 未清理 `snapshot_sends_` | 🟡 Medium | 添加 `snapshot_sends_.clear()` |
| 2 | `HandleInstallSnapshot` 异常路径 temp 文件泄漏 | 🟡 Medium | 添加 try-catch 或 RAII wrapper |
| 3 | `SaveSnapshotStream` 非原子替换 | 🟢 Low | 考虑写时复制策略（后续优化） |

**无 P0/P1 级别问题发现。**

---

*Report by Tom, 2025-06-09*
