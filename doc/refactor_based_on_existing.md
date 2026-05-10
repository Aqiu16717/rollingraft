# 基于现有代码的架构改造设计

## 1. 现有代码分析

### 1.1 当前架构状态

```cpp
// include/rollingraft/raft_node.h
class RaftNode {
  class RaftNodeImpl;
  std::unique_ptr<RaftNodeImpl> raft_node_impl_;
};

// src/raft_node.cpp
class RaftNode::RaftNodeImpl {
  // 已有成员（当前状态）
  using WorkGuard = asio::executor_work_guard<asio::io_context::executor_type>;
  std::unique_ptr<WorkGuard> work_guard_;
  std::unique_ptr<Server> server_;           // 已经存在！
  asio::io_context io_context_;              // RaftNode 管理 io_context
  std::thread io_thread_;
  std::atomic<bool> is_running_;
  
  RaftNodeConfig config_;
  std::shared_ptr<StateMachine> state_machine_;
  
  // Raft 状态...
};

// include/rollingraft/server.h
class Server {
  Server(uint32_t id, int port);  // 当前构造函数
  void Start();
};
```

### 1.2 当前问题

1. **Server 职责不清**：Server 只管监听，但收到消息后不知道交给谁处理
2. **缺少 CommandHandler 连接**：RaftNodeImpl 创建 Server，但没设置 handler
3. **RaftNode 直接依赖 Asio**：io_context_ 在 RaftNodeImpl 中暴露
4. **RPC 序列化没连接**：JsonProtocol 实现完整，但没被使用

### 1.3 现有可用资源

| 组件 | 状态 | 位置 |
|------|------|------|
| Server TCP 基础 | 可用 | `src/server.cpp` |
| JsonProtocol | 完整 | `src/json_protocol.cpp` |
| RaftCommandHandler | 框架 | `src/raft_command_handler.cpp` |
| RaftNode 框架 | 可用 | `src/raft_node.cpp` |
| 状态机接口 | 完整 | `include/rollingraft/state_machine.h` |
| RPC 消息定义 | 完整 | `include/rollingraft/rpc.h` |

## 2. 最小改造方案

### 2.1 核心原则

- **不推翻重来**：基于现有 Server、RaftNodeImpl 改造
- **渐进式**：先让基本流程跑通，再优化
- **解耦 Asio**：把 Asio 限制在 Server 内部，RaftNode 不直接 include asio

### 2.2 改造点 1：Server 支持外部 io_context（可选）

**现状**：Server 自己管理 io_context 还是外部传入？看代码：

```cpp
// 当前 ServerImpl 构造函数
ServerImpl(uint32_t id, uint16_t port, asio::io_context& io_ctx, ...)
// 已经是外部传入 io_context！
```

**现状 RaftNodeImpl**：
```cpp
RaftNodeImpl(...) : io_context_(), ...  // RaftNodeImpl 创建 io_context
                  server_(...)           // 传入 io_context_
```

**结论**：架构已经合理（RaftNode 管理 io_context，Server 使用它），只需完善连接。

### 2.3 改造点 2：让 Server 和 RaftNode 连接起来

**问题**：Server 收到消息后，如何调用 RaftNode 的方法？

**现状分析**：
```cpp
// Server::ServerImpl 构造函数需要 CommandHandler
ServerImpl(..., std::shared_ptr<CommandHandler> command_handler)

// 但 RaftNodeImpl 构造函数没有创建 handler：
RaftNodeImpl(...) 
    : server_(config.node_id, port, io_context_, ???)  // handler 怎么传？
```

**解决方案**：两阶段初始化（和之前说的一样，但是基于现有代码）

```cpp
// 修改 Server 头文件（最小改动）
class Server {
 public:
  Server() = default;
  Server(uint32_t id, int port, asio::io_context& io_ctx);  // 去掉 handler
  
  void SetCommandHandler(std::shared_ptr<CommandHandler> handler);  // 新增
  Status Start();  // 改为返回 Status
  Status Stop();   // 新增
};
```

**RaftNodeImpl 改造**：
```cpp
RaftNodeImpl(const RaftNodeConfig& config, std::shared_ptr<StateMachine> sm)
    : config_(config),
      state_machine_(sm),
      io_context_(),
      server_(std::make_unique<Server>()),  // 先默认构造
      ... {
  
  // 1. 创建 handler（需要 this，所以不能在初始化列表）
  // 但 handler 需要调用 RaftNode 的方法... 循环依赖
}
```

**循环依赖问题**：
- RaftCommandHandler 需要 RaftNode 来处理 RPC
- RaftNode 创建时需要 Server
- Server 需要 Handler
- Handler 需要 RaftNode

**解决方案 A**：前向声明 + shared_ptr（推荐，改动小）

```cpp
// raft_node_impl.h 或放在 raft_node.cpp 顶部
class RaftNodeRpcHandler : public CommandHandler {
 public:
  explicit RaftNodeRpcHandler(RaftNode::RaftNodeImpl* node) 
      : node_(node) {}
  
  Status HandleCommand(const std::string& request, 
                       std::string& response) override {
    return node_->HandleRpc(request, response);
  }
  
 private:
  RaftNode::RaftNodeImpl* node_;
};

// RaftNodeImpl 添加方法
class RaftNode::RaftNodeImpl {
 public:
  Status HandleRpc(const std::string& request, std::string& response) {
    // 1. 反序列化
    // 2. 根据类型分发到 RequestVote/AppendEntries
    // 3. 序列化响应
  }
};
```

### 2.4 改造点 3：RaftNodeImpl 实现 RPC 处理

**现状**：RaftNodeImpl 有 RequestVote/AppendEntries 方法，但它们是空的或用于发送

**改造后**：
```cpp
class RaftNode::RaftNodeImpl {
 public:
  // 发送 RPC（主动调用）
  Status RequestVote(const RequestVoteRequest&, RequestVoteResponse&);
  Status AppendEntries(const AppendEntriesRequest&, AppendEntriesResponse&);
  
  // 处理收到的 RPC（被 Handler 回调）
  void OnRequestVote(const RequestVoteRequest& req, RequestVoteResponse* resp);
  void OnAppendEntries(const AppendEntriesRequest& req, AppendEntriesResponse* resp);
  
  // Handler 入口
  Status HandleRpc(const std::string& request_data, std::string& response_data);
};

Status RaftNode::RaftNodeImpl::HandleRpc(
    const std::string& request_data, 
    std::string& response_data) {
  
  // 使用已有的 JsonProtocol
  static JsonProtocol protocol;
  
  // 反序列化
  RaftRequest* base_req = nullptr;  // 需要工厂方法创建具体类型
  Status status = protocol.DeserializeRequest(request_data, *base_req);
  if (!status.ok()) return status;
  
  // 分发
  switch (base_req->type_) {
    case RaftMessageType::KRequestVoteRequest: {
      auto& req = static_cast<RequestVoteRequest&>(*base_req);
      RequestVoteResponse resp;
      OnRequestVote(req, &resp);
      status = protocol.SerializeResponse(resp, response_data);
      break;
    }
    case RaftMessageType::KAppendEntriesRequest: {
      // 类似...
    }
    // ...
  }
  
  delete base_req;  // 工厂创建的，需要删除
  return status;
}
```

### 2.5 改造点 4：解决 JsonProtocol 的 Deserialize 问题

**现状**：JsonProtocol::DeserializeRequest 需要传入 RaftRequest&，但具体类型未知

**问题代码**：
```cpp
// json_protocol.cpp
Status DeserializeRequest(const std::string& input, RaftRequest& req) {
  // 根据 type 创建具体对象，但 req 是引用，不能重新赋值
  RequestVoteRequest* vote_req = new RequestVoteRequest(...);
  req = *vote_req;  // 切片！只复制了基类部分
  delete vote_req;
}
```

**解决方案 A**：使用智能指针（推荐）

```cpp
// protocol.h 修改接口
class Protocol {
 public:
  virtual Status DeserializeRequest(
      const std::string& input, 
      std::unique_ptr<RaftRequest>& req) = 0;  // 出参用智能指针
};

// json_protocol.cpp 实现
Status JsonProtocol::DeserializeRequest(
    const std::string& input,
    std::unique_ptr<RaftRequest>& req) {
  
  auto j = json::parse(input);
  int type_id = j["type"];
  
  switch (IntToMessageType(type_id)) {
    case RaftMessageType::KRequestVoteRequest: {
      auto vote_req = std::make_unique<RequestVoteRequest>(
          j["term"], j["candidate_id"], ...);
      req = std::move(vote_req);
      break;
    }
    // ...
  }
  return Status::OK();
}
```

### 2.6 改造点 5：完整构造流程

```cpp
// 基于现有代码的完整流程

RaftNode::RaftNode(const RaftNodeConfig& config, 
                   std::shared_ptr<StateMachine> sm)
    : raft_node_impl_(std::make_unique<RaftNodeImpl>(config, sm)) {}

RaftNodeImpl::RaftNodeImpl(const RaftNodeConfig& config,
                           std::shared_ptr<StateMachine> sm)
    : config_(config),
      state_machine_(sm),
      io_context_(),
      is_running_(false),
      // server_ 先不构造，等知道 port 后再构造
      {
  // 从 config.listen_addr 解析 port
  uint16_t port = ParsePort(config.listen_addr);
  
  // 构造 Server（传入 io_context，但不传 handler）
  server_ = std::make_unique<Server>(config.node_id, port, io_context_);
  
  // 创建 handler，传入 this
  auto handler = std::make_shared<RaftNodeRpcHandler>(this);
  
  // 设置 handler
  server_->SetCommandHandler(handler);
}

Status RaftNodeImpl::Start() {
  if (is_running_.exchange(true)) return Status::OK();
  
  // 1. 启动 Server（开始监听）
  auto status = server_->Start();
  if (!status.ok()) return status;
  
  // 2. 启动 io_context 线程（已有逻辑）
  work_guard_ = std::make_unique<WorkGuard>(
      asio::make_work_guard(io_context_));
  io_thread_ = std::thread([this]() { io_context_.run(); });
  
  // 3. 初始化 Raft 状态
  BecomeFollower();
  
  return Status::OK();
}
```

### 2.7 改造点 6：移除 raft_command_handler.h 的循环依赖

**现状**：`RaftCommandHandler` 在单独文件，依赖 `RaftNode`

```cpp
// src/raft_command_handler.h
#include "rollingraft/raft_node.h"  // 循环依赖风险

class RaftCommandHandler : public CommandHandler {
  std::shared_ptr<RaftNode> raft_node_;  // 持有 RaftNode
};
```

**问题**：RaftNode 包含 RaftNodeImpl，RaftNodeImpl 需要 Handler，Handler 包含 RaftNode...

**解决方案**：Handler 只依赖 RaftNodeImpl（前向声明）

```cpp
// 方案：把 Handler 定义移到 raft_node.cpp 内部

// src/raft_node.cpp
class RaftNode::RaftNodeImpl;  // 前向声明

// Handler 只依赖 Impl，不依赖 RaftNode
class RaftNodeRpcHandler : public CommandHandler {
 public:
  explicit RaftNodeRpcHandler(RaftNode::RaftNodeImpl* impl) : impl_(impl) {}
  
  Status HandleCommand(...) override {
    return impl_->HandleRpc(...);
  }
  
 private:
  RaftNode::RaftNodeImpl* impl_;
};

// 然后才是 RaftNodeImpl 定义
class RaftNode::RaftNodeImpl {
  // ...
};
```

## 3. 具体文件改动清单

### 3.1 include/rollingraft/server.h（小改）

```cpp
#pragma once

#include <cstdint>
#include <memory>
#include "rollingraft/status.h"

// 前向声明，不暴露 Asio
namespace asio {
class io_context;
}

namespace rollingraft {

class CommandHandler;

class Server {
 public:
  Server();
  // 新增：支持外部 io_context 的构造函数
  Server(uint32_t id, int port, asio::io_context& io_ctx);
  ~Server();
  
  // 新增：两阶段初始化
  void SetCommandHandler(std::shared_ptr<CommandHandler> handler);
  
  Status Start();  // 改为返回 Status
  Status Stop();   // 新增
  
 private:
  class ServerImpl;
  std::unique_ptr<ServerImpl> server_impl_;
};

}  // namespace rollingraft
```

### 3.2 src/server.cpp（中改）

- 修改构造函数支持外部 io_context
- 实现 SetCommandHandler
- Start/Stop 返回 Status
- Session 使用 weak_ptr 避免循环引用

### 3.3 include/rollingraft/protocol.h（小改）

```cpp
// 修改 Deserialize 接口使用智能指针
virtual Status DeserializeRequest(
    const std::string& input,
    std::unique_ptr<RaftRequest>& req) = 0;
```

### 3.4 src/json_protocol.cpp（中改）

- 实现新的 DeserializeRequest 接口
- 使用工厂模式创建具体请求对象

### 3.5 src/raft_node.cpp（大改）

- 添加 RaftNodeRpcHandler 类（内部类）
- 完善 RaftNodeImpl 构造函数
- 实现 HandleRpc 方法
- 完善 Start/Stop 错误处理

### 3.6 src/raft_command_handler.h（删除或废弃）

- 功能合并到 raft_node.cpp 内部

## 4. 不改造的部分（保持现状）

| 组件 | 理由 |
|------|------|
| RaftLog | 当前够用，后面再优化 |
| Persister | 先实现内存版本，持久化后加 |
| 定时器逻辑 | 先用简单实现，后续再抽象 TimerService |
| ClientRequest 处理 | 先跑通节点间 RPC，客户端协议后加 |

## 5. 实施步骤（基于现有代码）

### Step 1: Protocol 接口调整（2 小时）
- 修改 `protocol.h` 使用 `unique_ptr`
- 修改 `json_protocol.cpp` 实现
- 验证序列化/反序列化单元测试

### Step 2: Server 改造（3 小时）
- 修改 `server.h` 支持外部 io_context
- 实现 `SetCommandHandler`
- 修改 `Start/Stop` 返回 Status
- Session 使用 weak_ptr

### Step 3: RaftNode 连接（4 小时）
- 添加 `RaftNodeRpcHandler` 内部类
- 完善 `RaftNodeImpl` 构造函数
- 实现 `HandleRpc` 分发逻辑
- 测试编译通过

### Step 4: 联调（3 小时）
- 修复编译错误
- 单节点启动测试
- 两节点 RPC 通信测试

**总计：约 12 小时**（比重新设计少 3 小时）

## 6. 关键代码片段（基于现有）

### RaftNodeImpl 构造函数（最终）

```cpp
RaftNodeImpl::RaftNodeImpl(const RaftNodeConfig& config,
                           std::shared_ptr<StateMachine> sm)
    : config_(config),
      state_machine_(sm),
      io_context_(),
      is_running_(false),
      current_term_(0),
      voted_for_(-1),
      state_(RaftState::FOLLOWER) {
  
  // 解析端口
  uint16_t port = ParsePort(config.listen_addr);
  
  // 创建 Server（使用 RaftNodeImpl 的 io_context）
  server_ = std::make_unique<Server>(config.node_id, port, io_context_);
  
  // 创建并设置 Handler
  auto handler = std::make_shared<RaftNodeRpcHandler>(this);
  server_->SetCommandHandler(handler);
}
```

### HandleRpc 分发（最终）

```cpp
Status RaftNodeImpl::HandleRpc(const std::string& request_data,
                                std::string& response_data) {
  static JsonProtocol protocol;
  
  std::unique_ptr<RaftRequest> req;
  auto status = protocol.DeserializeRequest(request_data, req);
  if (!status.ok()) return status;
  
  switch (req->type_) {
    case RaftMessageType::KRequestVoteRequest: {
      auto& vote_req = static_cast<RequestVoteRequest&>(*req);
      RequestVoteResponse resp;
      OnRequestVote(vote_req, &resp);
      return protocol.SerializeResponse(resp, response_data);
    }
    case RaftMessageType::KAppendEntriesRequest: {
      // ...
    }
    default:
      return Status::ProtocolError("Unknown type");
  }
}
```

## 7. 总结

**基于现有代码的核心改动**：
1. Server 支持两阶段初始化（构造 + SetHandler）
2. Protocol 接口改用智能指针
3. RaftNodeImpl 添加 HandleRpc 方法
4. Handler 作为内部类，避免循环依赖

**保留的现有架构**：
- RaftNodeImpl 管理 io_context（合理）
- Server 使用外部 io_context（已经这样）
- PIMPL 模式（保持）
- 错误处理风格（保持）

**删除/合并**：
- `raft_command_handler.h`（功能内联到 raft_node.cpp）
