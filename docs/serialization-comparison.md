# 二进制序列化选型报告：protobuf / flatbuffers / msgpack / capnp

**Date**: 2026-06-10  
**Author**: @GeoHot  
**Context**: WAL 优化 spike 结论（JSON/Base64 CPU 瓶颈）→ 寻找替代序列化方案

---

## 1. 选型约束

### 1.1 硬性约束

| 约束 | 说明 | 影响 |
|------|------|------|
| **Schema 演进** | `RaftLogEntry` 未来可能扩展字段 | 需要 forward/backward 兼容 |
| **WAL 嵌入** | 需嵌入现有 WAL 文件结构（magic + version + segment_id + records + trailer） | 序列化后的字节流需作为 WAL Record 的 payload 嵌入 |
| **C++20** | 编译器为 Apple Clang / GCC 11+ | 需完整支持 C++17/20 特性 |
| **跨平台** | 目标 Linux x86_64 / ARM64 + macOS ARM64 | 需 little-endian 统一、对齐规则一致 |

### 1.2 偏好约束

| 约束 | 说明 |
|------|------|
| **零拷贝 deserialization** | WAL recovery 时需顺序读取大量 entry，零拷贝可显著降低 CPU |
| **小依赖** | 不引入过重构建依赖（如 Java 工具链、复杂 CMake） |
| **调试友好** | 能方便地 dump 二进制内容为人类可读格式 |

---

## 2. 候选方案对比

### 2.1 概览

| 维度 | protobuf | flatbuffers | msgpack | capnp |
|------|----------|-------------|---------|-------|
| **Schema 定义** | `.proto` 文件 | `.fbs` 文件 | 无（schema-less） | `.capnp` 文件 |
| **序列化方式** | 编码为紧凑二进制 | 内存布局即 wire 格式 | 编码为紧凑二进制 | 内存布局即 wire 格式 |
| **零拷贝读取** | ❌ 需要 parse | ✅ 直接偏移量访问 | ❌ 需要 parse | ✅ 直接偏移量访问 |
| **Schema 演进** | ✅ 优秀（field number） | ⚠️ 有限（只能加末尾） | ❌ 无保障 | ✅ 优秀（field number） |
| **序列化速度** | 快（~300ns/op） | 中等（需构建 buffer） | 快（~200ns/op） | 快（~50ns/op） |
| **反序列化速度** | 中等（~500ns/op） | **极快**（~80ns/op） | 中等（~400ns/op） | **极快**（~80ns/op） |
| **消息大小** | **最小**（varint + TLV） | 较大（vtable + padding） | 较小 | 较大（固定 struct + pointer） |
| **C++20 支持** | ✅ 完整 | ✅ 完整 | ✅ 完整 | ✅ 完整（C++11+） |
| **构建依赖** | 中（protoc + libprotobuf） | 小（flatc + 头文件-only 可选） | **极小**（头文件-only） | 中（capnp tool + lib） |
| **调试友好** | ✅ protoc --decode | ✅ flatc -t（JSON 输出） | ⚠️ 需工具 | ✅ capnp decode |
| **社区/生态** | **最大** | 中 | 中 | 小 |

> 速度数据参考：1KB payload，单字段 string，C++，Release 模式（来源：flatbuffers 官方 benchmark + 学术研究论文）。

---

### 2.2 protobuf（Protocol Buffers）

**设计哲学**：编码时序列化为紧凑二进制，解码时反序列化为内存对象。

**Wire format**：varint + tag-length-value (TLV)。小整数只占 1-2 字节，字符串带长度前缀。

**Schema 演进**：
- ✅ 添加字段：安全。新字段有默认值，旧代码忽略。
- ✅ 删除字段：安全。用 `reserved` 标记避免字段号复用。
- ⚠️ 改字段类型：部分安全（int32→int64 可以，string→int 不行）。
- ✅ 重排字段：不影响（按 field number 定位）。

**对 RollingRaft 的适用性**：
- **优点**：生态最成熟；schema 演进最安全；消息体积最小（对网络传输有利）。
- **缺点**：反序列化需要 parse + 内存分配，WAL recovery 时无法零拷贝。
- **WAL 嵌入**：序列化后的 `std::string` 直接作为 `WriteRecord()` 的 payload，完全兼容。

**依赖评估**：
- `libprotobuf` 约 2MB 静态库。
- `protoc` 代码生成器需构建时可用。
- 无运行时外部依赖。

---

### 2.3 flatbuffers

**设计哲学**：数据的序列化格式就是内存格式。不需要编码/解码，直接通过偏移量读取。

**Wire format**：vtable（字段偏移量表）+ 内联数据。访问一个字段 = 查 vtable 拿偏移量 + 按偏移量读数据。

**Schema 演进**：
- ✅ 添加字段：安全，但**只能加在 table 末尾**。
- ⚠️ 删除字段：标记为 deprecated，不能移除（vtable 偏移量不能变）。
- ❌ 改字段类型：**极度危险**。zero-copy 意味着数据直接按偏移量和类型解释。
- ❌ 重排字段：**破坏兼容性**。

**对 RollingRaft 的适用性**：
- **优点**：零拷贝 deserialization 对 WAL recovery 极具吸引力；构建 buffer 后可直接 `write()` 到 fd。
- **缺点**：schema 演进比 protobuf 严格；消息体积较大（vtable + padding 开销）；**Base64 字符串字段在 flatbuffers 中仍是 string 类型，仍需编码**（若保持现有接口不变）。
- **WAL 嵌入**：完全兼容。flatbuffer 构建完成后得到 `uint8_t*` + size，直接嵌入 WAL Record payload。

**关键限制**：
- `RaftLogEntry.data_` 和 `command_` 当前是 `std::string`（可能含二进制数据）。若继续 Base64 编码后再存入 flatbuffers string，则**零拷贝优势被 Base64 抵消**。
- 若改为直接存 raw bytes（`[uint8]` vector），则 flatbuffers 的零拷贝价值显现。

---

### 2.4 msgpack

**设计哲学**：schema-less 的二进制 JSON。无预定义 schema，按类型标记 + 长度编码。

**Wire format**：type tag + length + payload。例如 string = `0xda` (fixstr16) + 2-byte length + bytes。

**Schema 演进**：
- ❌ **无内置支持**。字段顺序变更、新增/删除字段都可能导致旧数据无法正确解析。
- 需自行实现版本号 + 条件解析逻辑（类似手动 TLV）。

**对 RollingRaft 的适用性**：
- **优点**：依赖极小（单头文件即可）；序列化/反序列化速度快。
- **缺点**：无 schema 验证，演化风险高；无法零拷贝；debug 需要额外工具。
- **WAL 嵌入**：兼容。

**结论**：schema-less 与 RollingRaft 的长期演进需求冲突。**不推荐**。

---

### 2.5 capnp（Cap'n Proto）

**设计哲学**：与 flatbuffers 类似——zero-copy，内存布局即 wire 格式。但使用固定大小 struct + pointer 段，而非 vtable。

**Wire format**：struct 定长（标量内联）+ 变长数据（string/list）通过 pointer 引用。小端序 + 对齐。

**Schema 演进**：
- ✅ 添加字段：安全（按 field number 定位，类似 protobuf）。
- ✅ 删除字段：安全（旧编号保留不复用）。
- ⚠️ 改字段类型：部分安全。
- ✅ 重排字段：不影响。

**对 RollingRaft 的适用性**：
- **优点**：零拷贝 + schema 演进比 flatbuffers 更灵活。Kenton Varda（protobuf v2 作者）设计，弥补了 flatbuffers 的演进缺陷。
- **缺点**：社区/生态比 protobuf/flatbuffers 小；C++ 库较复杂；消息体积与 flatbuffers 相近（大于 protobuf）。
- **WAL 嵌入**：兼容。

---

## 3. 场景化分析

### 3.1 RollingRaft 当前瓶颈场景

| 场景 | 当前格式 | 瓶颈 | 最适配方案 |
|------|----------|------|-----------|
| WAL 写入（100B payload） | JSON + Base64 | CPU（序列化 + 编码） | protobuf / msgpack |
| WAL 写入（10KB payload） | JSON + Base64 | CPU（Base64 膨胀 33%） | protobuf / flatbuffers（若消除 Base64） |
| WAL recovery（100K entries） | JSON + Base64 | CPU（parse + Base64 解码） | **flatbuffers / capnp**（零拷贝优势最大） |
| RPC 传输（node-to-node） | JSON Protocol 4 | 网络带宽 + parse CPU | protobuf（体积最小） |
| Snapshot 存储 | JSON + Base64 | 体积 + CPU | protobuf（紧凑）或 raw bytes |

### 3.2 零拷贝的价值量化

以 WAL recovery 100K entries、1KB payload 为例：

| 方案 | 反序列化动作 | 预估耗时 |
|------|-------------|---------|
| JSON + Base64 | parse JSON + 分配对象 + Base64 解码 | ~4.5s |
| protobuf | parse TLV + 分配对象 + 拷贝 string | ~1.5s |
| flatbuffers | 验证 vtable + 偏移量访问 | **~0.3s** |
| capnp | 验证 struct + 偏移量访问 | **~0.3s** |

> 零拷贝方案可将 recovery 时间降低 **3-5×**。

---

## 4. 选型建议

### 4.1 推荐方案：分阶段混合策略

| 阶段 | 目标 | 方案 | 理由 |
|------|------|------|------|
| **Phase 1（v0.2.1）** | 最小改动，最大收益 | **protobuf** | 替换 JSON 即可，Base64 保留。生态成熟，风险最低。 |
| **Phase 2（v0.3.0）** | 消除 Base64，启用零拷贝 | **flatbuffers** 或 **capnp** | 需将 `data_`/`command_` 从 Base64 string 改为 raw bytes vector。零拷贝 recovery 收益显著。 |
| **Phase 3（v0.4.0+）** | RPC 协议统一 | **protobuf**（若选 pb Phase 1）或 **capnp**（若选 capnp Phase 2） | 统一 storage + transport 序列化格式，减少格式转换。 |

### 4.2 单方案推荐：protobuf

如果只能选一个方案覆盖所有场景，推荐 **protobuf**：

| 理由 | 说明 |
|------|------|
| **生态** | 最大社区，工具链完善，长期维护有保障 |
| **Schema 演进** | 最安全。RaftLogEntry 未来扩展字段无风险 |
| **体积** | 最小。对 RPC 传输和 WAL 存储都有利 |
| **速度** | 序列化极快，反序列化可接受 |
| **构建** | protoc 代码生成，CMake 集成成熟 |
| **WAL 嵌入** | 完全兼容。`std::string payload = protobuf.SerializeAsString()` → `WriteRecord()` |

**protobuf 的零拷贝劣势可被其他优化抵消**：
- 若同步实施 "消除 Base64"（raw bytes + length prefix），protobuf 的 parse CPU  overhead 可被抵消。
- recovery 场景可用 mmap + lazy parsing 部分缓解。

### 4.3 备选方案：capnp

如果团队愿意接受较小社区，**capnp 是 flatbuffers 的上位替代**：

| capnp 优势 | flatbuffers 劣势 |
|-----------|-----------------|
| Schema 演进与 protobuf 同级 | flatbuffers 只能末尾加字段 |
| 零拷贝 deserialization | flatbuffers 也有，但演进受限 |
| struct 定长布局，cache friendly | vtable 间接访问有 overhead |

**capnp 的劣势**：
- 社区小，文档分散。
- C++ 库较复杂，构建依赖多。
- 消息体积比 protobuf 大 20-30%。

---

## 5. 实施路径建议

### 5.1 最小可行实验（1 天）

验证 protobuf 在 RollingRaft 场景下的实际收益：

1. 定义 `raft_log_entry.proto`：
   ```protobuf
   syntax = "proto3";
   message RaftLogEntry {
     uint64 index = 1;
     uint64 term = 2;
     bytes data = 3;      // raw bytes，替代 Base64
     bytes command = 4;   // raw bytes，替代 Base64
     uint64 checksum = 5;
   }
   ```

2. 修改 `WALPersister::AppendLogEntry()`：
   - 用 protobuf 替代 `nlohmann::json` + `Base64Encode()`
   - `data_`/`command_` 直接存 raw bytes（不再 Base64）

3. 运行 benchmark（100B / 1KB / 10KB）：
   - 对比 JSON vs protobuf 的序列化/反序列化耗时
   - 对比 Base64 vs raw bytes 的体积变化

### 5.2 预期收益

| 指标 | JSON+Base64 | protobuf+raw bytes | 收益 |
|------|------------|-------------------|------|
| 序列化 CPU | ~500ns | ~100ns | **5× 降低** |
| 消息体积（100B payload） | ~400B | ~150B | **2.5× 缩小** |
| 消息体积（10KB payload） | ~13.3KB | ~10.2KB | **1.3× 缩小** |
| 反序列化 CPU | ~800ns | ~300ns | **2.5× 降低** |

### 5.3 风险与回退

| 风险 | 缓解措施 |
|------|---------|
| protobuf 依赖引入构建复杂度 | 使用 bundled 源码（~2MB），不依赖系统包 |
| raw bytes 导致接口 breaking change | 新加 `bytes` 字段，保留旧 `string` 字段过渡期 |
| 跨版本 WAL 兼容性 | WAL 格式版本号（当前 version=1）→ bump 到 version=2，Open() 时根据 version 选择 parser |

---

## 6. 结论

| 问题 | 答案 |
|------|------|
| **最佳单方案** | **protobuf** — 生态 + 演进安全 + 体积 |
| **最佳零拷贝方案** | **capnp** — 零拷贝 + 演进安全（flatbuffers 的上位替代） |
| **不推荐** | **msgpack** — 无 schema，演化风险高 |
| **最快落地路径** | v0.2.1 用 protobuf 替换 JSON，同步消除 Base64 |
| **最大收益场景** | WAL recovery（零拷贝方案可降低 3-5×） |

---

*报告完成。建议下一步：1 天 protobuf MVP 实验，验证实际收益后再决定 Phase 2 方向。*
