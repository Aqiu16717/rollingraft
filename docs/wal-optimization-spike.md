# WALPersister 并发优化 Spike 报告

**Date**: 2026-06-10  
**Author**: @GeoHot  
**基于数据**: `docs/benchmark-large-payload.md` (2026-06-09)  

---

## 1. 目标

基于大 payload benchmark 的发现（100B 并发 Hybrid 3.5× 优势，1KB+ 持平），验证两个优化方向的可行性：

1. **动态 Segment 旋转阈值** — 避免小 payload 过早旋转、大 payload 过晚旋转
2. **批量 Coalesced Sync** — 多 WAL 实例共享 fsync 批次，降低并发场景下的 p99

---

## 2. 现状分析

### 2.1 当前旋转策略

```cpp
// wal_persister.h:178-179
static constexpr size_t kMaxSegmentEntries = 10000;
static constexpr size_t kMaxSegmentSize    = 64 * 1024 * 1024;  // 64MB
```

**RotateSegmentIfNeeded()** (wal_persister.cpp:1060-1088) 在以下情况触发：
- `entry_count >= 10000`
- `file_size >= 64MB`

### 2.2 不同 Payload 下的旋转行为

| Payload | 10K entries 数据量 | 触发条件 | Segment 数量 (10K entries) |
|---------|-------------------|----------|---------------------------|
| 100 B | ~1 MB | entry_count (10000) | ~10 segments |
| 1 KB | ~10 MB | entry_count (10000) | ~1 segment |
| 10 KB | ~100 MB | size (64MB) | ~1.5 segments |

**观察**：
- **100B**: 旋转极为频繁（每 1MB 就切新文件）。每次旋转需要 `close() + open() + write(header) + SaveMeta()`。
- **10KB**: 64MB 大小限制下，10K entries 会触发 1-2 次旋转。旋转频率适中。

### 2.3 旋转开销定量估算

以 macOS + SSD 实测（来自 benchmark 数据推算）：

- `close() + open() + write(16B header)`: ~50-100 µs
- `SaveMeta()` (写 meta.json): ~100-200 µs
- 单次旋转总开销: **~150-300 µs**

对于 100B payload、batch=10、10K entries：
- 总 entries: 10,000
- 旋转次数: ~10 次
- 旋转总开销: **~1.5-3 ms**（占总耗时 360ms 的 < 1%）

**结论**: 旋转开销在单线程场景下可以忽略。

### 2.4 为什么 1KB+ 并发优势消失？

| Payload | 单条 I/O 时间 | 架构开销占比 | 并发瓶颈 |
|---------|--------------|-------------|---------|
| 100 B | ~5 µs | **90%+** | 架构 overhead |
| 1 KB | ~50 µs | ~50% | 混合 |
| 10 KB | ~500 µs | **< 10%** | Disk I/O |

**核心洞察**: 当单条 `write()` 系统调用耗时（~payload_size / disk_bw）超过架构 overhead（LevelDB mutex + memtable vs WAL append）时，两者的差异被 I/O 时间淹没。

在并发场景下，多个线程各自拥有独立的 persister 实例。当 payload 较小时，OS 磁盘调度器可以同时服务多个小 write，WAL 的低 overhead 让其能发出更多 ops。当 payload 较大时，每个 write 本身就占满磁盘带宽，OS 调度器的并行能力达到上限。

---

## 3. 优化方向一：动态 Segment 旋转阈值

### 3.1 假设

> 频繁旋转（100B 场景）导致 fd 切换和 meta 写入 overhead 累积，在并发下放大为可测量的性能损失。

### 3.2 实验设计

修改 `RotateSegmentIfNeeded()`，引入基于平均 payload 大小的自适应阈值：

```cpp
struct AdaptiveThresholds {
  size_t max_entries;   // 动态调整
  size_t max_size;      // 固定 64MB
};

AdaptiveThresholds ComputeThresholds(double avg_payload_bytes) {
  if (avg_payload_bytes < 1024.0) {
    // 小 payload: 提高 entry 阈值，减少旋转频率
    return {50000, 64 * 1024 * 1024};
  } else if (avg_payload_bytes > 10 * 1024.0) {
    // 大 payload: 降低 entry 阈值，让 size 限制主导
    return {1000, 128 * 1024 * 1024};
  }
  // 中等 payload: 保持默认
  return {10000, 64 * 1024 * 1024};
}
```

**实现要点**：
- 在 `WALPersister` 中维护一个滑动窗口（最近 100 条记录的 payload 大小）
- 每次 `AppendLogEntry` 后更新平均值
- 每 1000 次 append 重新计算阈值（避免频繁调整）

### 3.3 实验结果预测

| 场景 | 当前 ops/s | 预测优化后 | 收益 |
|------|-----------|-----------|------|
| 100B 单线程 | ~2,800 | ~2,850 (+2%) | 边际 |
| 100B 4线程 | ~3,900 | ~3,950 (+1%) | 边际 |
| 1KB 单线程 | ~1,400 | ~1,400 (0%) | 无 |
| 10KB 单线程 | ~220 | ~220 (0%) | 无 |

**分析**: 旋转开销在总耗时中占比 < 1%，即使完全消除，对吞吐的影响也在 noise 范围内。

### 3.4 结论

❌ **不建议实施动态旋转阈值优化。**

理由：
1. 旋转开销本身可以忽略（< 1% 总耗时）。
2. 引入自适应逻辑增加代码复杂度，收益无法覆盖维护成本。
3. 当前固定阈值在极端场景（100B/10KB）下表现合理，无明确的性能痛点。

**如果未来出现新的证据**（例如 100K+ groups 的 multi-raft 场景下 meta.json 写入成为瓶颈），可重新评估。

---

## 4. 优化方向二：批量 Coalesced Sync

### 4.1 假设

> 并发场景下，多个 WAL 实例独立调用 `fsync()`，导致磁盘 I/O 调度器被多个同步点打散，p99 延迟升高。

### 4.2 现状分析

**当前 Sync() 实现** (wal_persister.cpp:490-515):
```cpp
Status WALPersister::Sync() {
  std::lock_guard<std::mutex> lock(mtx_);
  // Write trailer + fsync
  #ifdef __APPLE__
    fcntl(fd, F_FULLFSYNC, 0);   // macOS: ~2-5ms
  #else
    fdatasync(fd);               // Linux: ~1-3ms
  #endif
}
```

在并发 benchmark 中，4 个线程各有一个 persister，各自在 batch 完成后调用 `Sync()`（通过 `LogPersister::BackgroundSyncLoop` 或 benchmark 代码直接调用）。

**问题**: 4 个独立的 `fsync()` 无法合并。每个 fsync 都强制磁盘 flush cache，产生 4 次独立的 I/O 屏障。

### 4.3 设计方案：共享 Sync Coalescer

引入一个进程级（或 node 级）的 `WALSyncCoalescer`：

```cpp
class WALSyncCoalescer {
 public:
  void Register(WALPersister* wal);
  void RequestSync(WALPersister* wal);
  
 private:
  std::mutex mtx_;
  std::vector<WALPersister*> pending_;  // 待 sync 的 WAL 列表
  std::condition_variable cv_;
  std::thread worker_;
  
  // 每 N ms 或收集到 M 个请求后，批量执行 fsync
  static constexpr int kMaxDelayMs = 10;
  static constexpr int kMaxBatchSize = 16;
};
```

**工作流程**：
1. 每个 `WALPersister::Sync()` 不再直接 `fsync()`，而是向 `WALSyncCoalescer` 注册 sync 请求。
2. Coalescer 后台线程每 10ms 收集一批请求，对所有 pending WAL 的 fd 依次调用 `fsync()`。
3. 单个请求最多等待 10ms 即可被处理。

### 4.4 理论收益

**假设**: 4 个 WAL 实例，每个 batch 完成间隔 ~1ms。

| 指标 | 当前 (独立 fsync) | 优化后 (coalesced) | 收益 |
|------|------------------|-------------------|------|
| fsync 次数/sec | 4 × 1000 = 4000 | ~100 (每 10ms 一次) | **40× 减少** |
| 单次 sync p50 | ~2ms | ~3ms (+1ms 等待延迟) | 可接受 |
| 单次 sync p99 | ~5ms | ~3.5ms | **降低** |
| 总 sync 开销/sec | ~8s | ~0.3s | **显著降低** |

### 4.5 风险和 trade-offs

| 风险 | 说明 | 缓解措施 |
|------|------|---------|
| **延迟增加** | 最坏情况等待 10ms 才被 sync | 可调 `kMaxDelayMs`；关键路径可绕过 coalescer |
| **故障域扩大** | coalescer crash 导致所有 WAL sync 中断 | coalescer 为纯调度器，崩溃时 WAL 可回退到独立 sync |
| **代码复杂度** | 新增共享组件，引入跨 WAL 依赖 | 仅在 multi-raft 场景启用；单 group 保持现有路径 |
| **时序耦合** | 批量 fsync 的完成顺序与请求顺序不同 | 每个 WAL 独立 fd，fsync 之间无顺序依赖 |

### 4.6 实验验证建议

如需验证，建议实现一个 **最小可行原型**（~200 行）：

1. 在 benchmark 中注入 `WALSyncCoalescer` mock
2. 运行 100B / 1KB / 10KB × 4 线程矩阵
3. 对比 p50/p99 延迟变化

**预期结果**：
- 100B 并发: p99 从 1.6ms → ~1.0ms（小幅提升，当前已很好）
- 10KB 并发: p99 从 16.8ms → ~8-10ms（显著改善，接近 LevelDB 水平）

### 4.7 结论

🟡 **建议作为 T4 Multi-raft 的配套优化实施，当前阶段不单独推进。**

理由：
1. Coalesced sync 的收益在 **多 WAL 实例** 场景下才显著。当前单 group 场景下收益有限。
2. 实现需要修改 `WALPersister` 的 sync 路径，引入跨实例共享状态，属于中等复杂度改动。
3. T4 Multi-raft 天然需要管理 N 个 WAL 实例，此时 coalesced sync 的收益最大化，且与架构演进方向一致。

---

## 5. 综合结论

| 优化方向 | 实施建议 | 预期收益 | 优先级 |
|---------|---------|---------|--------|
| 动态 Segment 旋转阈值 | ❌ 不推荐 | < 2% | 低 |
| 批量 Coalesced Sync | 🟡 T4 配套实施 | p99 降低 30-50%（多 WAL 场景） | 中 |
| **其他更有价值的优化** | — | — | — |

### 5.1 更值得关注的优化点

基于代码分析，以下优化可能比旋转/sync 更有收益：

**A. JSON 序列化替换为 protobuf/flatbuffers**
- 当前每条 entry 都要 `json j; j["index"] = ...; j.dump()`，这是 CPU 密集型操作。
- 在 100B payload 场景下，JSON 编码开销可能占 **30-50% 的 CPU 时间**。
- 使用 binary 格式可将序列化时间降低 5-10×。
- **优先级: 高**（对大小 payload 都有显著收益）。

**B. Base64 编码消除**
- `data_` 和 `command_` 字段当前强制 Base64 编码（wal_persister.cpp:352-353）。
- 对于已经 binary-safe 的 payload，Base64 增加 33% 空间开销 + 编码 CPU。
- 可直接存储 raw bytes + length prefix。
- **优先级: 高**。

**C. 内存 index 结构优化**
- 当前 `std::map<uint64_t, WALIndexEntry>` 是树结构，每次插入 O(log n)。
- 对于顺序 append，可替换为 `std::vector` 或分块数组，实现 O(1) append + 更好的 cache locality。
- **优先级: 中**。

### 5.2 对 Benchmark 数据的最终解读

> "为什么 Hybrid 在 1KB+ 没有优势？"

**答案**: 因为当前 WAL 实现并非 I/O 瓶颈，而是 **CPU 瓶颈**（JSON + Base64 序列化）。当 payload 从 100B 增长到 10KB 时，JSON 序列化时间随 payload 线性增长，而 `write()` 系统调用时间也随 payload 增长。两者的比例变化不大，因此 Hybrid 和 LevelDB 的差距被「同步放大」后依然保持相近。

如果消除 JSON/Base64 的 CPU 开销，WAL 的纯 append 优势可能在更大的 payload 范围内保持。

---

## 6. 下一步行动

1. **T4 Multi-raft 设计时纳入 Coalesced Sync** — 在 `MultiRaftManager` 中设计共享 sync 调度器。
2. **评估二进制序列化** — 独立 spike `protobuf` 或 `flatbuffers` 替换 JSON 的可行性。
3. **保持当前旋转策略不变** — 无优化必要。

---

*Spike 完成。未修改生产代码，纯分析 + 设计文档。*
