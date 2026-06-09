# RollingRaft 竞态条件审查报告

**审查日期**: 2025-06-09  
**审查范围**: P1-P2 新增核心功能  
**审查人**: Tom  

---

## 执行摘要

对 5 个核心功能进行了深度竞态条件审查，发现 **2 个 Medium Severity** 问题、**2 个 Low Severity** 问题、**1 个建议项**。无 P0/P1 级别竞态条件或死锁。

| 功能 | 严重度 | 发现数 | 状态 |
|------|--------|--------|------|
| Pipeline replication | 🟡 Medium | 1 | inflight_ 在 step-down 时未清理 |
| Async Apply | 🟢 Low | 1 | 无竞态，但 callback 语义待明确 |
| Client Session | 🟢 Low | 1 | 无竞态，但 session_manager_ 清理顺序建议 |
| Transport Batching | 🟡 Medium | 1 | Close() 时 write_queue_ 未调用 callbacks |
| WALPersister | 🟢 Low | 1 | Close() 异常时 trailer 可能不完整 |

---

## 🟡 Medium Severity

### MR1. Pipeline replication: `BecomeFollowerLocked` 未清空 `inflight_`

**位置**: `src/election_manager.cpp:5-63`  
**代码**:
```cpp
void RaftNode::RaftNodeImpl::BecomeFollowerLocked(Term term) {
  // ... 设置 role_ = FOLLOWER ...
  // 停止 leader timers ...
  // 但没有清空 inflight_！
}
```

**问题**: 当 leader 因为收到更高 term 的 AppendEntries/RequestVote 而 step down 时，`BecomeFollowerLocked` 没有清空 `inflight_` 映射。虽然：
- `BecomeLeaderLocked` 在重新当选时会清空 `inflight_`
- 所有回调路径（`HandleAppendEntriesResponse`、`ScheduleAppendEntriesRetry`）都检查了 `role_ == LEADER`

但旧 term 的 inflight 条目会在内存中残留到下一次当选，造成不必要的内存占用。在长时间无法重新当选的场景（如网络分区导致少数派隔离）中，这部分内存会持续累积。

**建议**: 在 `BecomeFollowerLocked` 中添加 `inflight_.clear()`，与 `BecomeLeaderLocked` 保持一致：
```cpp
void RaftNode::RaftNodeImpl::BecomeFollowerLocked(Term term) {
  // ... existing code ...
  inflight_.clear();  // 清理旧 leader 的 pipeline 状态
  next_index_.clear();
  match_index_.clear();
}
```

---

### MR2. Transport Batching: `Close()` 时 `write_queue_` 未调用 callbacks

**位置**: `src/asio_network_transport.cpp`  
**代码**:
```cpp
void Close() {
  asio::post(strand_, [self = shared_from_this()]() {
    self->write_queue_.clear();  // ← 直接丢弃，未通知 callbacks
    self->write_in_progress_ = false;
  });
}
```

**问题**: 当 `TcpConnection::Close()` 被调用时，`write_queue_` 中未发送的消息被直接 `clear()` 丢弃，但对应的 RPC callbacks 不会被调用。这导致：
- Client 发起的 proposal 可能永远等待 response
- RPC timeout 机制（由 `pending_callbacks_` 中的 timer 处理）最终会触发 timeout，但这取决于 timer 的超时时间

**建议**: 在 `Close()` 中遍历 `write_queue_`，提取 correlation_ids，然后在 strand 中调用对应的 callbacks：
```cpp
void Close() {
  auto cids = ... // extract correlation_ids from write_queue_
  asio::post(strand_, [self = shared_from_this(), cids = std::move(cids)]() {
    self->write_queue_.clear();
    self->write_in_progress_ = false;
    for (uint64_t cid : cids) {
      self->NotifyCallback(cid, false, "Connection closed");
    }
  });
}
```

---

## 🟢 Low Severity

### LR1. Async Apply: apply thread 退出语义正确

**位置**: `src/raft_node_core.cpp:499-553`, `src/state_machine_applier.cpp:96-167`  

**审查结论**: ✅ 正确。

1. `DoGracefulShutdown()` 设置 `apply_running_ = false` 并 `notify_all()`
2. `ApplyLoop` 从 wait 返回后检查 `!apply_running_` 并 break
3. `join()` 等待 `ApplyLoop` 完成当前 batch
4. 然后 drain `apply_queue_`，对未处理的 task 调用 callback 返回 `"Node stopped"`
5. 最后清理 `pending_proposals_`

无竞态，无 callback 丢失。

---

### LR2. Client Session: `session_manager_` 清理顺序正确

**位置**: `src/raft_node_core.cpp:82`  

**审查结论**: ✅ 正确。

`session_manager_` 是 `RaftNodeImpl` 的 `std::unique_ptr<ClientSessionManager>` 成员。`DoGracefulShutdown()` 在 `RaftNodeImpl` 析构之前调用，因此 session_manager_ 在整个 shutdown 过程中始终可用。

**建议（非阻塞）**: 在 `DoGracefulShutdown` 的末尾显式 `session_manager_.reset()`，确保在 pending callbacks 被调用后、析构前释放 session 内存。当前行为正确，但这是防御式编程的增强。

---

### LR3. WALPersister: `Close()` 异常时 trailer 可能不完整

**位置**: `src/wal_persister.cpp`  
**代码**:
```cpp
void WALPersister::Close() {
  std::lock_guard<std::mutex> lock(mtx_);
  if (active_segment_.fd >= 0) {
    lseek(active_segment_.fd, active_segment_.end_offset, SEEK_SET);
    WriteTrailer(active_segment_.fd, active_segment_.end_offset);  // ← 未检查返回值
    close(active_segment_.fd);
    active_segment_ = Segment{};
  }
  // ...
}
```

**问题**: `WriteTrailer` 的返回值被忽略。如果写入失败（如磁盘满），segment 文件可能缺少有效的 trailer。下次 `Open()` 时，`ReadTrailer` 会检测到损坏并可能将文件视为空 segment。

**影响**: 低。`Open()` 的 CRC 验证和 trailer 检查会检测到问题，可能丢失该 segment 中的最后一个记录，但不会导致数据静默损坏。

**建议**: 在 `WriteTrailer` 失败时记录 WARN 日志，或考虑使用 `fdatasync` 确保 trailer 落盘。

---

## 总结

| # | 问题 | 严重度 | 建议修复 |
|---|------|--------|----------|
| 1 | `BecomeFollowerLocked` 未清空 `inflight_` | 🟡 Medium | 添加 `inflight_.clear()` |
| 2 | `TcpConnection::Close()` 未通知 write_queue_ callbacks | 🟡 Medium | 遍历 queue 调用 callbacks |
| 3 | WALPersister `Close()` 忽略 `WriteTrailer` 返回值 | 🟢 Low | 检查返回值并记录日志 |

**无 P0/P1 级别竞态条件或死锁发现。**

---

*Report by Tom, 2025-06-09*
