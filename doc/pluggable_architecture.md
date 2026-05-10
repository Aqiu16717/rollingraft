# RollingRaft 可插拔架构设计

## 1. 设计原则

### 1.1 核心原则

1. **依赖抽象，不依赖具体**
   - RaftNode 只依赖接口（NetworkTransport, TimerService, Persister, Logger）
   - 不依赖 Asio、spdlog、文件系统等具体实现

2. **开箱即用**
   - 用户只需实现 `StateMachine`（业务逻辑）
   - 其他组件提供默认实现（AsioNetwork, FilePersister, SpdlogLogger）
   - 一行代码启动：`RaftNode::Create(config, state_machine)->Start()`

3. **可插拔**
   - 高级用户可以替换任意组件
   - 通过依赖注入或工厂方法配置

### 1.2 架构分层

```
┌──────────────────────────────────────────────────────────────┐
│                      User Application                        │
│  ┌────────────────────────────────────────────────────────┐ │
│  │                  UserStateMachine                       │ │
│  │  (Counter / KV Store / SQL / ...)                      │ │
│  └────────────────────────────────────────────────────────┘ │
└───────────────────────────┬──────────────────────────────────┘
                            │ 只依赖 StateMachine 接口
┌───────────────────────────▼──────────────────────────────────┐
│                    RollingRaft Core                          │
│  ┌────────────────────────────────────────────────────────┐ │
│  │                    RaftNode                             │ │
│  │  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐ │ │
│  │  │   Election  │  │ Log Replication│  │  Snapshot    │ │ │
│  │  │   Logic     │  │   Logic        │  │  Manager     │ │ │
│  │  └─────────────┘  └──────────────┘  └───────────────┘ │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  依赖接口：                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │  Network     │  │    Timer     │  │   Persister      │   │
│  │  Transport   │  │   Service    │  │   (optional)     │   │
│  │  (required)  │  │  (required)  │  │                  │   │
│  └──────────────┘  └──────────────┘  └──────────────────┘   │
└───────────────────────────┬──────────────────────────────────┘
                            │ 抽象接口
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│   Default     │   │   Default     │   │   Default     │
│  AsioNetwork  │   │  AsioTimer    │   │ FilePersister │
│  (Production) │   │  (Production) │   │  (Production) │
└───────────────┘   └───────────────┘   └───────────────┘
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│     Mock      │   │     Mock      │   │     Mock      │
│   Network     │   │    Timer      │   │   Persister   │
│   (Testing)   │   │   (Testing)   │   │   (Testing)   │
└───────────────┘   └───────────────┘   └───────────────┘
```

## 2. 抽象接口定义

### 2.1 NetworkTransport（网络传输层）

```cpp
// include/rollingraft/network_transport.h
#pragma once

#include <functional>
#include <string>
#include <memory>
#include "rollingraft/status.h"

namespace rollingraft {

using NodeId = int32_t;
using NodeAddr = std::string;  // 如 "127.0.0.1:8001"

// RPC 回调类型
using RpcResponseCallback = std::function<void(const std::string& response_data)>;
using RpcRequestHandler = std::function<void(NodeId from,
                                              const std::string& request_data,
                                              std::string& response_data)>;

// 网络传输抽象接口
class NetworkTransport {
 public:
  virtual ~NetworkTransport() = default;

  // 初始化：设置监听地址和请求处理器
  virtual Status Initialize(const NodeAddr& listen_addr,
                            RpcRequestHandler request_handler) = 0;

  // 启动/停止
  virtual Status Start() = 0;
  virtual Status Stop() = 0;

  // 发送 RPC（异步）
  virtual void SendRpc(NodeId to,
                       const NodeAddr& addr,
                       const std::string& request_data,
                       RpcResponseCallback callback) = 0;

  // 获取本地地址
  virtual NodeAddr GetLocalAddr() const = 0;
};

// 工厂函数类型
using NetworkTransportFactory = std::function<std::unique_ptr<NetworkTransport>()>;

}  // namespace rollingraft
```

### 2.2 TimerService（定时器服务）

```cpp
// include/rollingraft/timer_service.h
#pragma once

#include <functional>
#include <chrono>
#include <cstdint>

namespace rollingraft {

using TimerCallback = std::function<void()>;
using TimerId = uint64_t;

// 定时器服务抽象接口
class TimerService {
 public:
  virtual ~TimerService() = default;

  virtual void Start() = 0;
  virtual void Stop() = 0;

  // 一次性定时器
  virtual TimerId SetTimeout(std::chrono::milliseconds delay,
                             TimerCallback callback) = 0;

  // 周期性定时器
  virtual TimerId SetInterval(std::chrono::milliseconds interval,
                              TimerCallback callback) = 0;

  virtual void CancelTimer(TimerId timer_id) = 0;
  virtual void ResetTimer(TimerId timer_id, std::chrono::milliseconds new_delay) = 0;
};

// 工厂函数类型
using TimerServiceFactory = std::function<std::unique_ptr<TimerService>()>;

}  // namespace rollingraft
```

### 2.3 Persister（持久化层，可选）

```cpp
// include/rollingraft/persister.h
#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "rollingraft/raft_log.h"
#include "rollingraft/status.h"

namespace rollingraft {

// 持久化元数据
struct PersistentState {
  uint64_t current_term = 0;
  int32_t voted_for = -1;
};

// 持久化抽象接口
class Persister {
 public:
  virtual ~Persister() = default;

  // 初始化数据目录
  virtual Status Init(const std::string& data_dir) = 0;

  // 保存/加载元数据
  virtual Status SaveState(const PersistentState& state) = 0;
  virtual Status LoadState(PersistentState& state) = 0;

  // 保存/加载日志
  virtual Status AppendLog(const RaftLogEntry& entry) = 0;
  virtual Status TruncateLog(uint64_t from_index) = 0;
  virtual std::vector<RaftLogEntry> LoadLog(uint64_t start, uint64_t end) = 0;

  // 快照
  virtual Status SaveSnapshot(const std::string& snapshot_data, uint64_t last_index) = 0;
  virtual Status LoadSnapshot(std::string& snapshot_data, uint64_t& last_index) = 0;
};

// 工厂函数类型
using PersisterFactory = std::function<std::unique_ptr<Persister>()>;

}  // namespace rollingraft
```

### 2.4 Protocol（序列化协议）

```cpp
// include/rollingraft/protocol.h
#pragma once

#include <memory>
#include <string>
#include "rollingraft/status.h"

namespace rollingraft {

// 前向声明
struct RaftRequest;
struct RaftResponse;

// 序列化协议抽象接口
class Protocol {
 public:
  virtual ~Protocol() = default;

  virtual Status SerializeRequest(const RaftRequest& req,
                                  std::string& output) const = 0;
  virtual Status DeserializeRequest(const std::string& input,
                                    std::unique_ptr<RaftRequest>& req) = 0;

  virtual Status SerializeResponse(const RaftResponse& res,
                                   std::string& output) const = 0;
  virtual Status DeserializeResponse(const std::string& input,
                                     std::unique_ptr<RaftResponse>& res) = 0;
};

// 工厂函数类型
using ProtocolFactory = std::function<std::unique_ptr<Protocol>()>;

}  // namespace rollingraft
```

## 3. RaftNode 配置与工厂

### 3.1 配置结构

```cpp
// include/rollingraft/raft_node.h

struct RaftNodeConfig {
  // 基础配置
  NodeId node_id;
  NodeAddr listen_addr;
  std::vector<NodeAddr> peers;
  std::string data_dir;

  // 超时配置
  uint32_t election_timeout_ms = 300;
  uint32_t heartbeat_interval_ms = 100;

  // 可选：自定义组件工厂（nullptr 使用默认）
  NetworkTransportFactory network_factory = nullptr;
  TimerServiceFactory timer_factory = nullptr;
  PersisterFactory persister_factory = nullptr;
  ProtocolFactory protocol_factory = nullptr;
};
```

### 3.2 RaftNode 接口

```cpp
class RaftNode {
 public:
  // 高级用法：完全自定义所有组件
  RaftNode(const RaftNodeConfig& config,
           std::shared_ptr<StateMachine> state_machine,
           std::unique_ptr<NetworkTransport> network,
           std::unique_ptr<TimerService> timer,
           std::unique_ptr<Persister> persister = nullptr,
           std::unique_ptr<Protocol> protocol = nullptr);

  // 工厂方法：开箱即用（使用默认组件）
  static std::unique_ptr<RaftNode> Create(
      const RaftNodeConfig& config,
      std::shared_ptr<StateMachine> state_machine);

  // 工厂方法：部分自定义
  static std::unique_ptr<RaftNode> CreateWithNetwork(
      const RaftNodeConfig& config,
      std::shared_ptr<StateMachine> state_machine,
      std::unique_ptr<NetworkTransport> network);

  ~RaftNode();

  Status Start();
  Status Stop();

  bool IsLeader() const;

  // 提交命令
  Status Propose(const std::string& command,
                 std::function<void(const ApplyResult&)> callback);

 private:
  class RaftNodeImpl;
  std::unique_ptr<RaftNodeImpl> impl_;
};
```

## 4. 默认实现（src/ 目录）

### 4.1 默认组件注册表

```cpp
// src/default_components.h
#pragma once

#include "rollingraft/raft_node.h"

namespace rollingraft {

// 注册/获取默认组件工厂
class DefaultComponents {
 public:
  // 设置自定义默认工厂（全局）
  static void SetNetworkFactory(NetworkTransportFactory factory);
  static void SetTimerFactory(TimerServiceFactory factory);
  static void SetPersisterFactory(PersisterFactory factory);
  static void SetProtocolFactory(ProtocolFactory factory);

  // 获取默认工厂（如果没有设置，返回 Asio 实现）
  static NetworkTransportFactory GetNetworkFactory();
  static TimerServiceFactory GetTimerFactory();
  static PersisterFactory GetPersisterFactory();
  static ProtocolFactory GetProtocolFactory();
};

}  // namespace rollingraft
```

### 4.2 默认 Asio 实现

```cpp
// src/asio/asio_network_transport.h
#pragma once

#include "rollingraft/network_transport.h"
#include <asio.hpp>

namespace rollingraft {

// 默认网络实现：基于 Asio
class AsioNetworkTransport : public NetworkTransport {
 public:
  AsioNetworkTransport();
  ~AsioNetworkTransport() override;

  Status Initialize(const NodeAddr& listen_addr,
                    RpcRequestHandler request_handler) override;
  Status Start() override;
  Status Stop() override;
  void SendRpc(NodeId to, const NodeAddr& addr,
               const std::string& request_data,
               RpcResponseCallback callback) override;
  NodeAddr GetLocalAddr() const override;

  // 允许外部传入 io_context（用于和 Timer 共享）
  void SetIoContext(asio::io_context& io_context);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rollingraft
```

```cpp
// src/asio/asio_timer_service.h
#pragma once

#include "rollingraft/timer_service.h"
#include <asio.hpp>

namespace rollingraft {

// 默认定时器实现：基于 Asio
class AsioTimerService : public TimerService {
 public:
  AsioTimerService();
  explicit AsioTimerService(asio::io_context& io_context);
  ~AsioTimerService() override;

  void Start() override;
  void Stop() override;
  TimerId SetTimeout(std::chrono::milliseconds delay, TimerCallback callback) override;
  TimerId SetInterval(std::chrono::milliseconds interval, TimerCallback callback) override;
  void CancelTimer(TimerId timer_id) override;
  void ResetTimer(TimerId timer_id, std::chrono::milliseconds new_delay) override;

  // 获取内部 io_context（用于和 Network 共享）
  asio::io_context& GetIoContext();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rollingraft
```

```cpp
// src/file_persister.h
#pragma once

#include "rollingraft/persister.h"

namespace rollingraft {

// 默认持久化实现：基于文件
class FilePersister : public Persister {
 public:
  FilePersister();
  ~FilePersister() override;

  Status Init(const std::string& data_dir) override;
  Status SaveState(const PersistentState& state) override;
  Status LoadState(PersistentState& state) override;
  Status AppendLog(const RaftLogEntry& entry) override;
  Status TruncateLog(uint64_t from_index) override;
  std::vector<RaftLogEntry> LoadLog(uint64_t start, uint64_t end) override;
  Status SaveSnapshot(const std::string& snapshot_data, uint64_t last_index) override;
  Status LoadSnapshot(std::string& snapshot_data, uint64_t& last_index) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rollingraft
```

```cpp
// src/json_protocol.h
#pragma once

#include "rollingraft/protocol.h"

namespace rollingraft {

// 默认协议实现：JSON
class JsonProtocol : public Protocol {
 public:
  JsonProtocol();
  ~JsonProtocol() override;

  Status SerializeRequest(const RaftRequest& req, std::string& output) const override;
  Status DeserializeRequest(const std::string& input,
                            std::unique_ptr<RaftRequest>& req) override;
  Status SerializeResponse(const RaftResponse& res, std::string& output) const override;
  Status DeserializeResponse(const std::string& input,
                             std::unique_ptr<RaftResponse>& res) override;
};

}  // namespace rollingraft
```

## 5. 用户使用示例

### 5.1 开箱即用（最简单）

```cpp
// example/counter/counter_server.cpp

#include <rollingraft/raft_node.h>
#include <rollingraft/state_machine.h>

int main(int argc, char** argv) {
  // 1. 实现 StateMachine（用户唯一需要做的）
  auto counter_sm = std::make_shared<CounterMachine>();

  // 2. 配置
  RaftNodeConfig config;
  config.node_id = 1;
  config.listen_addr = "127.0.0.1:8001";
  config.peers = {"127.0.0.1:8002", "127.0.0.1:8003"};
  config.data_dir = "./data/node1";

  // 3. 创建节点（使用所有默认组件：Asio网络、Asio定时器、文件持久化、JSON协议）
  auto node = RaftNode::Create(config, counter_sm);

  // 4. 启动
  auto status = node->Start();
  if (!status.ok()) {
    std::cerr << "Failed to start: " << status.ToString() << std::endl;
    return 1;
  }

  // 5. 运行...
  std::this_thread::sleep_for(std::chrono::hours(24));

  // 6. 停止
  node->Stop();
  return 0;
}
```

### 5.2 自定义网络层（高级用法）

```cpp
// 用户自定义网络层实现
class MyCustomNetwork : public rollingraft::NetworkTransport {
  // 实现自己的网络逻辑（如基于 libuv、RDMA 等）
};

int main() {
  auto sm = std::make_shared<MyStateMachine>();
  
  RaftNodeConfig config{...};
  
  // 方式1：通过配置工厂
  config.network_factory = []() {
    return std::make_unique<MyCustomNetwork>();
  };
  auto node = RaftNode::Create(config, sm);
  
  // 方式2：直接传入实例
  auto network = std::make_unique<MyCustomNetwork>();
  auto node = RaftNode::CreateWithNetwork(config, sm, std::move(network));
  
  node->Start();
  return 0;
}
```

### 5.3 全局替换默认实现

```cpp
// 在程序入口设置全局默认
int main() {
  // 所有后续 Create 调用都会使用自定义实现
  rollingraft::DefaultComponents::SetNetworkFactory([]() {
    return std::make_unique<MyOptimizedNetwork>();
  });
  
  rollingraft::DefaultComponents::SetPersisterFactory([]() {
    return std::make_unique<MyRocksDBPersister>();
  });
  
  // 现在所有 RaftNode::Create 都会使用自定义实现
  auto node1 = RaftNode::Create(config1, sm1);
  auto node2 = RaftNode::Create(config2, sm2);
  // ...
}
```

### 5.4 单元测试（Mock 组件）

```cpp
// test/test_raft.cpp

class MockNetwork : public NetworkTransport {
 public:
  std::vector<std::pair<NodeId, std::string>> sent_messages;
  
  void SendRpc(NodeId to, const NodeAddr&, const std::string& data,
               RpcResponseCallback callback) override {
    sent_messages.push_back({to, data});
    // 模拟响应
    callback("{\"success\": true}");
  }
  // ... 其他方法
};

class MockTimer : public TimerService {
 public:
  std::unordered_map<TimerId, TimerCallback> timers;
  
  TimerId SetTimeout(std::chrono::milliseconds, TimerCallback cb) override {
    TimerId id = next_id_++;
    timers[id] = cb;
    return id;
  }
  
  void TriggerTimer(TimerId id) {
    if (timers.count(id)) timers[id]();
  }
  // ... 其他方法
  
 private:
  TimerId next_id_ = 1;
};

TEST(RaftNode, Election) {
  auto sm = std::make_shared<MockStateMachine>();
  auto network = std::make_unique<MockNetwork>();
  auto timer = std::make_unique<MockTimer>();
  
  RaftNodeConfig config{1, "127.0.0.1:1", {2, 3}};
  
  RaftNode node(config, sm, std::move(network), std::move(timer));
  
  node.Start();
  
  // 触发选举超时
  static_cast<MockTimer*>(timer.get())->TriggerTimer(election_timer_id);
  
  // 验证发送了 RequestVote
  EXPECT_EQ(network->sent_messages.size(), 2);
}
```

## 6. 内部实现策略

### 6.1 RaftNodeImpl 持有抽象接口

```cpp
// src/raft_node_impl.h（内部使用）

class RaftNode::RaftNodeImpl {
 public:
  RaftNodeImpl(const RaftNodeConfig& config,
               std::shared_ptr<StateMachine> sm,
               std::unique_ptr<NetworkTransport> network,
               std::unique_ptr<TimerService> timer,
               std::unique_ptr<Persister> persister,
               std::unique_ptr<Protocol> protocol)
      : config_(config),
        state_machine_(sm),
        network_(std::move(network)),
        timer_(std::move(timer)),
        persister_(std::move(persister)),
        protocol_(std::move(protocol)) {}

  Status Start() {
    // 1. 初始化网络层
    network_->Initialize(config_.listen_addr,
        [this](NodeId from, const std::string& req, std::string& resp) {
          return HandleRpc(from, req, resp);
        });
    
    auto status = network_->Start();
    if (!status.ok()) return status;

    // 2. 启动定时器
    timer_->Start();

    // 3. 加载持久化状态
    if (persister_) {
      PersistentState state;
      persister_->Init(config_.data_dir);
      persister_->LoadState(state);
      current_term_ = state.current_term;
      voted_for_ = state.voted_for;
    }

    // 4. 进入 Follower 状态
    BecomeFollower();

    return Status::OK();
  }

 private:
  void BecomeFollower() {
    state_ = RaftState::FOLLOWER;
    
    // 使用抽象定时器
    election_timer_ = timer_->SetTimeout(
        RandomTimeout(config_.election_timeout_ms),
        [this]() { OnElectionTimeout(); });
  }

  void OnElectionTimeout() {
    BecomeCandidate();
    BroadcastRequestVote();
  }

  void BroadcastRequestVote() {
    RequestVoteRequest req(current_term_, config_.node_id,
                           log_.LastIndex(), log_.LastTerm());
    
    std::string data;
    protocol_->SerializeRequest(req, data);
    
    for (const auto& peer : config_.peers) {
      NodeId peer_id = ParseNodeId(peer);
      network_->SendRpc(peer_id, peer, data,
          [this, peer_id](const std::string& resp_data) {
            HandleRequestVoteResponse(peer_id, resp_data);
          });
    }
  }

  Status HandleRpc(NodeId from, const std::string& request_data,
                   std::string& response_data) {
    std::unique_ptr<RaftRequest> req;
    auto status = protocol_->DeserializeRequest(request_data, req);
    if (!status.ok()) return status;

    switch (req->type_) {
      case RaftMessageType::KRequestVoteRequest: {
        auto& vote_req = static_cast<RequestVoteRequest&>(*req);
        RequestVoteResponse resp;
        OnRequestVote(vote_req, &resp);
        return protocol_->SerializeResponse(resp, response_data);
      }
      // ...
    }
  }

 private:
  RaftNodeConfig config_;
  std::shared_ptr<StateMachine> state_machine_;
  
  // 抽象组件（不依赖具体实现）
  std::unique_ptr<NetworkTransport> network_;
  std::unique_ptr<TimerService> timer_;
  std::unique_ptr<Persister> persister_;
  std::unique_ptr<Protocol> protocol_;
  
  // Raft 状态
  uint64_t current_term_ = 0;
  int32_t voted_for_ = -1;
  RaftState state_;
  TimerId election_timer_ = 0;
  // ...
};
```

### 6.2 RaftNode::Create 工厂实现

```cpp
std::unique_ptr<RaftNode> RaftNode::Create(
    const RaftNodeConfig& config,
    std::shared_ptr<StateMachine> state_machine) {
  
  // 使用配置中的工厂或全局默认工厂
  auto network_factory = config.network_factory 
      ? config.network_factory 
      : DefaultComponents::GetNetworkFactory();
  
  auto timer_factory = config.timer_factory
      ? config.timer_factory
      : DefaultComponents::GetTimerFactory();
  
  auto persister_factory = config.persister_factory
      ? config.persister_factory
      : DefaultComponents::GetPersisterFactory();
  
  auto protocol_factory = config.protocol_factory
      ? config.protocol_factory
      : DefaultComponents::GetProtocolFactory();
  
  // 共享 io_context 优化（网络层和定时器使用同一个 io_context）
  auto timer = timer_factory();
  auto network = network_factory();
  
  // 如果都是 Asio 实现，共享 io_context
  if (auto* asio_timer = dynamic_cast<AsioTimerService*>(timer.get())) {
    if (auto* asio_network = dynamic_cast<AsioNetworkTransport*>(network.get())) {
      asio_network->SetIoContext(asio_timer->GetIoContext());
    }
  }
  
  return std::make_unique<RaftNode>(
      config, state_machine,
      std::move(network),
      std::move(timer),
      persister_factory(),
      protocol_factory());
}
```

## 7. 目录结构

```
rollingraft/
├── include/rollingraft/          # 公共接口（不依赖任何实现）
│   ├── raft_node.h              # 核心接口
│   ├── state_machine.h          # 状态机接口
│   ├── network_transport.h      # 网络抽象
│   ├── timer_service.h          # 定时器抽象
│   ├── persister.h              # 持久化抽象
│   ├── protocol.h               # 协议抽象
│   ├── rpc.h                    # RPC 消息定义
│   ├── status.h                 # 错误处理
│   └── ...
│
├── src/                          # 实现
│   ├── raft_node.cpp            # RaftNode 实现
│   ├── raft_node_impl.h         # PIMPL 实现（内部）
│   ├── default_components.cpp   # 默认组件注册表
│   │
│   ├── asio/                    # Asio 默认实现
│   │   ├── asio_network_transport.h/.cpp
│   │   ├── asio_timer_service.h/.cpp
│   │   └── asio_utils.h
│   │
│   ├── persist/                 # 持久化实现
│   │   ├── file_persister.h/.cpp
│   │   └── memory_persister.h/.cpp  # 用于测试
│   │
│   └── protocol/                # 协议实现
│       ├── json_protocol.h/.cpp
│       └── protobuf_protocol.h/.cpp  # 可选
│
├── example/                     # 示例
│   └── counter/
│       ├── counter_server.cpp   # 开箱即用示例
│       └── counter_client.cpp
│
└── tests/                       # 测试
    ├── mock/                    # Mock 组件
    │   ├── mock_network.h
    │   └── mock_timer.h
    └── test_raft_node.cpp       # 使用 Mock 的单元测试
```

## 8. 总结

### 8.1 设计达成目标

| 目标 | 实现方式 |
|------|----------|
| **依赖抽象** | RaftNodeImpl 持有 `unique_ptr<NetworkTransport>` 等抽象接口 |
| **开箱即用** | `RaftNode::Create(config, state_machine)` 自动使用默认 Asio 实现 |
| **可插拔** | 通过工厂函数或构造函数注入自定义实现 |
| **向后兼容** | 基于现有代码逐步迁移，不推翻重来 |

### 8.2 实施路径

1. **Phase 1**: 定义抽象接口（network_transport.h, timer_service.h 等）
2. **Phase 2**: 将现有 Asio 实现包装成默认实现
3. **Phase 3**: 修改 RaftNodeImpl 使用抽象接口
4. **Phase 4**: 实现 DefaultComponents 和 Create 工厂方法
5. **Phase 5**: 编写 Mock 实现和单元测试
6. **Phase 6**: Counter 示例验证

### 8.3 时间估算

| Phase | 内容 | 时间 |
|-------|------|------|
| 1 | 接口定义 | 4h |
| 2 | 现有代码包装 | 6h |
| 3 | RaftNodeImpl 改造 | 8h |
| 4 | 工厂方法 | 4h |
| 5 | Mock + 测试 | 6h |
| 6 | 验证 | 4h |
| **总计** | | **32h** |
