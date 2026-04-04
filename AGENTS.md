# RollingRaft Development Guidelines

**项目**: 基于 Asio 的开箱即用 Raft 库  
**版本**: v0.1.0  
**语言**: C++20

---

## Core Principles

### KISS - Keep It Simple
* 能用 `std::vector` 就不用自定义容器
* 函数调用不超过 3-4 层
* 先跑起来，再优化
* 单一职责，每个类/函数只做一件事

### No "AI Smell" Code
* 显而易见的逻辑不需要注释
* 不过度设计，只解决当前问题
* 代码直接，能用一行不用三行
* 允许暂时的不完美代码

### Dependencies Are Acceptable
* Asio (网络), spdlog (日志), nlohmann/json (序列化), LevelDB (持久化)
* 目标：开箱即用，不是零依赖

---

## Code Style

| Item | Rule |
|------|------|
| Standard | C++20 |
| Style | Google C++ Style |
| Line Length | 80 chars |
| Indent | 2 spaces |
| Classes | PascalCase (`RaftNode`) |
| Functions | PascalCase (`Start`, `Stop`) |
| Variables | snake_case (`current_term`) |
| Members | trailing underscore (`term_`) |

### Comments
* **英文注释** - 所有注释使用英文
* **Doxygen 风格** - 公共 API 使用 `/** */`
* **解释"为什么"** - 不重复代码显而易见的内容

---

## Project Status

### Completed ✅
* [x] 项目基础结构
* [x] Asio TCP 服务器
* [x] **Raft 核心**（选举、日志复制、快照）
* [x] **JSON 协议**序列化
* [x] **LevelDB 持久化**
* [x] **日志持久化**（LogPersister 批量异步）
* [x] **ReadIndex** 线性一致读
* [x] **成员变更**（添加/删除节点）
* [x] **单元测试**（59 个）
* [x] **零编译警告**
* [x] **API 文档**（Doxygen 规范注释）

### TODO
* [ ] 性能基准测试
* [ ] 集成测试自动化（Docker）
* [ ] Metrics & Monitoring
* [ ] 自动快照触发
* [ ] Client Library（高层封装）

---

## Quick Start

### Build
```bash
# 使用 build.sh（推荐）
./build.sh              # Release 构建
./build.sh debug        # Debug 构建
./build.sh test         # 构建并运行测试
./build.sh clean        # 清理构建目录

# 或手动 CMake
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Run Tests
```bash
# 单元测试
./build/tests/unit_tests

# 特定测试
./build/tests/unit_tests --gtest_filter="*Snapshot*"
```

### Run Counter Example (3 nodes)
```bash
# Terminal 1 - Node 1 (becomes leader)
./build/example/counter/counter_server 1 8001 8002 8003

# Terminal 2 - Node 2
./build/example/counter/counter_server 2 8002 8001 8003

# Terminal 3 - Node 3
./build/example/counter/counter_server 3 8003 8001 8002

# Client
./build/example/counter/counter_client 127.0.0.1:8001
```

---

## Commit Style

遵循 [Conventional Commits](https://www.conventionalcommits.org/)：

```
type(scope): subject

body (use * for bullets)
```

### Types
* `feat` - 新功能
* `fix` - Bug 修复
* `docs` - 文档
* `style` - 代码格式
* `refactor` - 重构
* `test` - 测试
* `chore` - 构建/工具

### Example
```
feat: add log persistence support

* Add LogPersister class with batch async writes
* Integrate in RaftNode for propose and append
* Restore logs from disk on startup
```

---

## Directory Structure

```
rollingraft/
├── include/rollingraft/    # 公共 API 头文件
│   ├── raft_node.h        # 主 Raft 节点接口
│   ├── state_machine.h    # 状态机接口
│   ├── rpc.h              # RPC 消息定义
│   ├── status.h           # 状态码
│   └── types.h            # 类型定义
├── src/                   # 实现
│   ├── raft_node.cpp      # Raft 核心实现
│   ├── raft_log.cpp       # 日志管理
│   ├── asio_network_transport.cpp
│   ├── leveldb_persister.cpp
│   └── ...
├── tests/                 # 测试
│   ├── unit/              # 单元测试
│   └── integration/       # 集成测试
├── example/               # 示例
│   └── counter/           # 计数器示例
├── doc/                   # 设计文档（本地）
└── third_party/           # 依赖库
```

---

## Key Files

| 文件 | 说明 |
|------|------|
| `include/rollingraft/raft_node.h` | 主 API |
| `doc/todo.md` | 任务列表 |
| `doc/comment_style_guide.md` | 注释规范 |
| `CONTRIBUTING.md` | 贡献指南 |
| `build.sh` | 构建脚本 |

---

## Resources

* [Raft Paper](https://raft.github.io/raft.pdf)
* [Raft Visualization](https://raft.github.io/)
* [Asio Documentation](https://think-async.com/Asio/)
