# RollingRaft 架构状态报告

> 日期: 2026-04-02  
> 版本: 基于代码审查的当前状态

---

## 1. 项目概述

RollingRaft 是一个基于 **Asio** 的 C++20 Raft 共识库，提供开箱即用的分布式共识解决方案。设计理念是**集成优于分离**：网络层、存储层、共识逻辑都由库管理，用户只需实现业务状态机。

---

## 2. 架构概览

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Application Layer                            │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              StateMachine (用户实现)                         │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │   │
│  │  │    Apply()   │  │CreateSnapshot│  │   Restore()  │      │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘      │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ Apply Committed Commands
┌─────────────────────────────────────────────────────────────────────┐
│                      RollingRaft Core Library                       │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                      RaftNode (PIMPL)                        │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │   │
│  │  │   Election   │  │   Log        │  │   Snapshot   │      │   │
│  │  │   Logic      │  │ Replication  │  │   Transfer   │      │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘      │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   Network    │  │    Timer     │  │  Persister   │              │
│  │  (Asio TCP)  │  │   Service    │  │  (LevelDB)   │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   Protocol   │  │  RaftLog     │  │    Logger    │              │
│  │   (JSON)     │  │  (Memory)    │  │   (spdlog)   │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. 组件实现状态

### 3.1 核心组件

| 组件 | 状态 | 文件位置 | 说明 |
|------|------|----------|------|
| **RaftNode** | ✅ 已完成 | `src/raft_node.cpp` | 完整实现选举、日志复制、快照 |
| **StateMachine** | ✅ 接口稳定 | `include/rollingraft/state_machine.h` | 用户需实现的5个纯虚函数 |
| **RPC 消息** | ✅ 已完成 | `include/rollingraft/rpc.h` | 8种消息类型完整定义 |
| **NetworkTransport** | ✅ 已完成 | `src/asio_network_transport.cpp` | Asio TCP 实现 |
| **TimerService** | ✅ 已完成 | `src/asio_timer_service.cpp` | Asio deadline_timer 实现 |
| **Persister** | ✅ 已完成 | `src/leveldb_persister.cpp` | LevelDB 持久化实现 |
| **Protocol** | ✅ 已完成 | `src/json_protocol.cpp` | JSON 序列化实现 |

### 3.2 功能模块

| 功能 | 状态 | 说明 |
|------|------|------|
| **Leader 选举** | ✅ 已完成 | 随机超时、投票计数、状态转换 |
| **日志复制** | ✅ 已完成 | AppendEntries、冲突处理、批量发送 |
| **Commit 推进** | ✅ 已完成 | 多数确认、应用到 StateMachine |
| **成员变更** | ❌ 未实现 | 动态添加/删除节点（P3） |
| **ReadIndex** | ⚠️ 占位 | 线性一致读接口待实现 |
| **Client 协议** | ✅ 已完成 | 请求去重、Leader 重定向 |

---

## 4. 核心 API

### 4.1 状态机接口 (用户实现)

```cpp
class StateMachine {
 public:
  // 应用已提交的命令（保证顺序和一致性）
  virtual ApplyResult Apply(std::span<const uint8_t> data, uint64_t index) = 0;

  // 查询最后应用的日志索引
  virtual uint64_t GetLastAppliedIndex() const = 0;

  // 创建快照句柄（轻量、快速、非阻塞）
  virtual std::shared_ptr<Snapshot> CreateSnapshot() = 0;

  // 从快照数据恢复
  virtual bool Restore(const std::vector<uint8_t>& snapshot) = 0;

  // 等待指定索引被应用（用于 Linearizable Read）
  virtual void WaitIndex(uint64_t index, std::function<void()> cb) = 0;
};
```

### 4.2 RaftNode API

```cpp
class RaftNode {
 public:
  RaftNode(const RaftNodeConfig& config, std::shared_ptr<StateMachine> sm);
  
  // 生命周期
  Status Start();
  Status Stop();
  
  // 状态查询
  bool IsLeader() const;
  RaftNodeRole GetRole() const;
  Term CurrentTerm() const;
  NodeAddr GetLeaderAddr() const;
  
  // 提议命令（仅 Leader 可调用）
  Status Propose(const std::string& command,
                 std::function<void(const ApplyResult&)> callback);
  
  // 回调设置（必须在 Start() 前设置）
  void SetRoleChangeCallback(std::function<void(RaftNodeRole, Term)> cb);
  void SetLeaderChangeCallback(std::function<void(NodeId, const NodeAddr&)> cb);
};
```

### 4.3 配置结构

```cpp
struct RaftNodeConfig {
  NodeId node_id;                          // 本节点 ID
  std::string listen_addr;                 // 监听地址 (e.g., "127.0.0.1:8001")
  std::vector<std::string> peers;          // 其他节点地址列表
  std::string data_dir;                    // 数据目录

  // 超时配置
  uint32_t election_timeout_ms = 300;      // 选举超时
  uint32_t heartbeat_interval_ms = 100;    // 心跳间隔
  uint32_t max_entries_per_append = 100;   // 每次 RPC 最大日志条目数
  uint32_t snapshot_threshold = 10000;     // 快照触发阈值
  uint32_t rpc_timeout_ms = 1000;          // RPC 超时

  // 工厂函数（用于测试注入 Mock）
  std::function<std::unique_ptr<NetworkTransport>()> network_factory = nullptr;
  std::function<std::unique_ptr<TimerService>()> timer_factory = nullptr;
  std::function<std::unique_ptr<Persister>()> persister_factory = nullptr;
  std::function<std::unique_ptr<Protocol>()> protocol_factory = nullptr;
};
```

---

## 5. RPC 消息类型

| 消息 | 类型 | 用途 |
|------|------|------|
| `RequestVoteRequest/Response` | 节点间 | Leader 选举 |
| `AppendEntriesRequest/Response` | 节点间 | 日志复制 + 心跳 |
| `InstallSnapshotRequest/Response` | 节点间 | 快照传输 |
| `ClientRequest/Response` | 客户端 | 业务命令提交 |

---

## 6. 项目结构

```
rollingraft/
├── include/rollingraft/           # 公共 API 头文件
│   ├── raft_node.h               # 核心 RaftNode 类
│   ├── state_machine.h           # 状态机接口
│   ├── rpc.h                     # RPC 消息定义
│   ├── raft_log.h                # 日志条目定义
│   ├── network_transport.h       # 网络传输接口
│   ├── timer_service.h           # 定时器服务接口
│   ├── persister.h               # 持久化接口
│   ├── protocol.h                # 序列化协议接口
│   ├── status.h                  # 错误处理
│   ├── logger.h                  # 日志接口
│   └── types.h                   # 类型定义
│
├── src/                           # 实现文件
│   ├── raft_node.cpp             # Raft 核心逻辑 (~1500 行)
│   ├── asio_network_transport.cpp # Asio TCP 实现
│   ├── asio_timer_service.cpp    # Asio 定时器实现
│   ├── leveldb_persister.cpp     # LevelDB 存储实现
│   ├── json_protocol.cpp         # JSON 序列化
│   ├── raft_log.cpp              # 内存日志管理
│   ├── logger*.cpp               # 日志实现
│   └── status.cpp                # Status 实现
│
├── example/                       # 示例程序
│   └── counter/                   # 分布式计数器示例
│       ├── counter_server.cpp    # 计数器服务器
│       └── counter_client.cpp    # 计数器客户端
│
├── tests/                         # 测试（待完善）
│   ├── CMakeLists.txt
│   └── test_server.cpp           # 空文件
│
└── third_party/                   # 第三方依赖
    ├── asio/                     # 网络库
    ├── spdlog/                   # 日志库
    ├── json/                     # JSON 库
    ├── leveldb/                  # 存储引擎
    └── googletest/               # 测试框架
```

---

## 7. 数据流

### 7.1 写请求流程

```
┌─────────┐     ┌─────────────┐     ┌──────────┐     ┌─────────────┐
│ Client  │────▶│  RaftNode   │────▶│  Leader  │────▶│   Followers │
└─────────┘     └─────────────┘     └──────────┘     └──────┬──────┘
     │                          │                          │
     │                          │                          ▼
     │                          │                     ┌─────────────┐
     │                          │◄────────────────────│  AppendEntries
     │                          │                        Response
     │                          │
     │◄─────────────────────────┘
     │      ApplyResult (callback)
```

### 7.2 选举流程

```
┌──────────┐                    ┌──────────┐
│  Node 1  │◀──────────────────▶│  Node 2  │
│(Follower)│   RequestVote      │(Follower)│
└────┬─────┘                    └────┬─────┘
     │                               │
     │◄──────── Election ────────────▶│
     │          Timeout               │
     │                               │
     ▼                               ▼
┌──────────┐                    ┌──────────┐
│(Candidate│◀── RequestVote ───▶│(Candidate│
│  Term+1) │     (投票)          │  Term+1) │
└────┬─────┘                    └────┬─────┘
     │                               │
     ▼                               ▼
┌──────────┐                    ┌──────────┐
│( Leader )│                    │(Follower)│
│  Term+1  │                    │  Term+1  │
└──────────┘                    └──────────┘
```

---

## 8. 测试状态

| 测试类型 | 状态 | 说明 |
|----------|------|------|
| 单元测试 | ❌ 未开始 | `tests/test_server.cpp` 为空 |
| 集成测试 | ❌ 未开始 | 需要编写集群测试 |
| Mock 组件 | ⚠️ 文档中 | `test.md` 有设计但无实现 |

---

## 9. 待办事项

### 高优先级
- [ ] **补充单元测试** - 使用 gtest 测试选举、日志复制逻辑
- [ ] **补充集成测试** - 3/5 节点集群测试
- [ ] **验证 Counter 示例** - 端到端功能验证

### 中优先级
- [ ] **实现 ReadIndex** - 线性一致读支持
- [ ] **成员变更** - 动态添加/删除节点
- [ ] **性能测试** - 吞吐量、延迟基准

### 低优先级
- [ ] **配置热加载** - 运行时更新配置
- [ ] **监控指标** - Prometheus 导出

---

## 10. 代码规范

| 项目 | 规范 |
|------|------|
| 语言标准 | C++20 |
| 代码风格 | Google C++ Style Guide |
| 行长限制 | 80 字符 |
| 缩进 | 2 空格 |
| 类名 | PascalCase (`RaftNode`) |
| 函数名 | PascalCase (`Start`, `Stop`) |
| 变量名 | snake_case (`current_term`) |
| 成员变量 | trailing underscore (`term_`) |

---

## 11. 构建与运行

```bash
# 构建
mkdir -p build && cd build
cmake ..
make -j4

# 运行 Counter 示例（3 节点集群）
mkdir -p data/node1 data/node2 data/node3

# Terminal 1
./example/example_counter_server 1 8001 8002 8003

# Terminal 2
./example/example_counter_server 2 8002 8001 8003

# Terminal 3
./example/example_counter_server 3 8003 8001 8002

# Terminal 4 - 客户端
./example/example_counter_client
```

---

## 12. 关键设计决策

1. **PIMPL 模式**: `RaftNode` 公开接口，`RaftNodeImpl` 隐藏实现细节
2. **回调驱动**: 异步回调风格，避免阻塞调用
3. **依赖注入**: 通过工厂函数支持 Mock 注入测试
4. **随机选举超时**: [election_timeout, 2*election_timeout) 避免活锁
5. **内存日志 + 持久化状态**: 日志在内存，term/voted_for 持久化到 LevelDB
6. **64KB 快照分块**: 大快照分块传输，避免阻塞网络

---

## 13. 参考文档

| 文档 | 内容 |
|------|------|
| `DESIGN.md` | 原始设计文档（部分已过时） |
| `DESIGNv2.md` - `DESIGNv6.md` | 设计迭代历史 |
| `USER_GUIDE.md` | 用户使用指南（部分 API 已变更） |
| `CLAUDE.md` | Claude Code 工作指引 |
| `AGENTS.md` | 开发规范和原则 |
| `test.md` | 测试策略设计文档 |
