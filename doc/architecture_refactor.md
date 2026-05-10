# RollingRaft 架构重构设计

## 1. 当前问题

### 1.1 架构耦合

当前设计的问题：`RaftNode` 直接依赖 `asio::io_context`

```cpp
// 当前设计（耦合）
class RaftNode::RaftNodeImpl {
  asio::io_context io_context_;      // 直接依赖 Asio
  Server server_;                     // Server 也依赖 Asio
  std::thread io_thread_;
};
```

**问题**：
- Raft 核心逻辑与 Asio 网络层强耦合
- 无法单元测试（必须启动 Asio 事件循环）
- 无法替换网络实现（如改用 libuv、裸 socket 等）
- 编译依赖重（所有使用 RaftNode 的地方都要包含 Asio）

### 1.2 目标

```
┌─────────────────────────────────────────────────────────────┐
│                    Application                              │
│                 (Counter/KV Store)                          │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                   RaftNode (核心)                            │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │  状态机     │  │   日志管理   │  │    选举逻辑      │   │
│  │  (State)    │  │   (RaftLog)  │  │  (Election)      │   │
│  └─────────────┘  └──────────────┘  └──────────────────┘   │
│                                                              │
│  依赖接口：NetworkTransport, TimerService                    │
│  不依赖具体实现（Asio/ Libuv / 裸 socket）                   │
└───────────────────────┬─────────────────────────────────────┘
                        │ 通过抽象接口
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│ AsioTransport │ │ MockTransport │ │  RawSocket    │
│  (生产环境)    │ │  (单元测试)    │ │  (嵌入式)      │
└───────────────┘ └───────────────┘ └───────────────┘
```

## 2. 抽象层设计

### 2.1 NetworkTransport 接口

```cpp
// include/rollingraft/network_transport.h
#pragma once

#include <functional>
#include <string>
#include <vector>
#include "rollingraft/status.h"
#include "rollingraft/rpc.h"

namespace rollingraft {

// 节点地址标识
using NodeId = int32_t;
using NodeAddr = std::string;  // 如 "127.0.0.1:8001"

// 网络传输回调
using RpcResponseCallback = std::function<void(const std::string& response_data)>;
using RpcRequestHandler = std::function<void(NodeId from, 
                                              const std::string& request_data,
                                              std::string& response_data)>;

// 网络传输抽象接口
// RaftNode 只依赖此接口，不依赖具体实现
class NetworkTransport {
 public:
  virtual ~NetworkTransport() = default;

  // 初始化：绑定本地地址，准备接收连接
  virtual Status Initialize(const NodeAddr& listen_addr,
                            RpcRequestHandler request_handler) = 0;

  // 启动：开始监听和发送
  virtual Status Start() = 0;

  // 停止：关闭所有连接
  virtual Status Stop() = 0;

  // 发送 RPC 到指定节点（异步）
  // RaftNode 调用此方法发送 RequestVote/AppendEntries
  virtual void SendRpc(NodeId to, 
                       const NodeAddr& addr,
                       const std::string& request_data,
                       RpcResponseCallback callback) = 0;

  // 获取本地节点地址
  virtual NodeAddr GetLocalAddr() const = 0;
};

}  // namespace rollingraft
```

### 2.2 TimerService 接口

```cpp
// include/rollingraft/timer_service.h
#pragma once

#include <functional>
#include <chrono>

namespace rollingraft {

using TimerCallback = std::function<void()>;
using TimerId = uint64_t;

// 定时器服务抽象
// RaftNode 使用此接口管理选举定时器和心跳定时器
class TimerService {
 public:
  virtual ~TimerService() = default;

  // 启动定时器服务
  virtual void Start() = 0;

  // 停止所有定时器
  virtual void Stop() = 0;

  // 设置一次性定时器（返回 timer_id 用于取消）
  virtual TimerId SetTimeout(std::chrono::milliseconds delay, 
                             TimerCallback callback) = 0;

  // 设置周期性定时器
  virtual TimerId SetInterval(std::chrono::milliseconds interval,
                              TimerCallback callback) = 0;

  // 取消定时器
  virtual void CancelTimer(TimerId timer_id) = 0;

  // 重置定时器（重新计时）
  virtual void ResetTimer(TimerId timer_id, std::chrono::milliseconds new_delay) = 0;
};

}  // namespace rollingraft
```

### 2.3 RaftNode 改造后接口

```cpp
// include/rollingraft/raft_node.h
#pragma once

#include <memory>
#include "rollingraft/state_machine.h"
#include "rollingraft/status.h"
#include "rollingraft/network_transport.h"
#include "rollingraft/timer_service.h"

namespace rollingraft {

struct RaftNodeConfig {
  NodeId node_id;
  NodeAddr listen_addr;
  std::vector<NodeAddr> peers;
  std::string data_dir;
  
  uint32_t election_timeout_ms = 300;
  uint32_t heartbeat_interval_ms = 100;
};

class RaftNode {
 public:
  // 构造函数：注入依赖（依赖注入模式）
  RaftNode(const RaftNodeConfig& config,
           std::shared_ptr<StateMachine> state_machine,
           std::unique_ptr<NetworkTransport> transport,      // 注入网络层
           std::unique_ptr<TimerService> timer_service);     // 注入定时器
  
  // 或使用工厂方法，提供默认 Asio 实现
  static std::unique_ptr<RaftNode> Create(
      const RaftNodeConfig& config,
      std::shared_ptr<StateMachine> state_machine);

  ~RaftNode();

  Status Start();
  Status Stop();

  bool IsLeader() const;
  
  // 提交命令（给客户端使用）
  Status Propose(const std::string& command, 
                 std::function<void(const ApplyResult&)> callback);

 private:
  class RaftNodeImpl;
  std::unique_ptr<RaftNodeImpl> impl_;
};

}  // namespace rollingraft
```

## 3. Asio 实现（在 src/ 中）

### 3.1 AsioNetworkTransport

```cpp
// src/asio_network_transport.h
#pragma once

#include "rollingraft/network_transport.h"
#include <asio.hpp>
#include <memory>
#include <thread>
#include <unordered_map>

namespace rollingraft {

// Asio 实现的网络传输层
class AsioNetworkTransport : public NetworkTransport {
 public:
  AsioNetworkTransport();
  ~AsioNetworkTransport() override;

  Status Initialize(const NodeAddr& listen_addr,
                    RpcRequestHandler request_handler) override;
  Status Start() override;
  Status Stop() override;
  
  void SendRpc(NodeId to, 
               const NodeAddr& addr,
               const std::string& request_data,
               RpcResponseCallback callback) override;

  NodeAddr GetLocalAddr() const override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rollingraft
```

### 3.2 AsioTimerService

```cpp
// src/asio_timer_service.h
#pragma once

#include "rollingraft/timer_service.h"
#include <asio.hpp>
#include <memory>
#include <unordered_map>

namespace rollingraft {

// Asio 实现的定时器服务
class AsioTimerService : public TimerService {
 public:
  explicit AsioTimerService(asio::io_context& io_context);
  ~AsioTimerService() override;

  void Start() override;
  void Stop() override;
  
  TimerId SetTimeout(std::chrono::milliseconds delay, 
                     TimerCallback callback) override;
  TimerId SetInterval(std::chrono::milliseconds interval,
                      TimerCallback callback) override;
  void CancelTimer(TimerId timer_id) override;
  void ResetTimer(TimerId timer_id, std::chrono::milliseconds new_delay) override;

 private:
  asio::io_context& io_context_;
  std::atomic<TimerId> next_timer_id_{1};
  std::unordered_map<TimerId, std::shared_ptr<asio::steady_timer>> timers_;
  std::mutex timers_mutex_;
};

}  // namespace rollingraft
```

## 4. RaftNode 内部实现调整

### 4.1 改造后的 RaftNodeImpl

```cpp
// src/raft_node.cpp（改造后）

class RaftNode::RaftNodeImpl {
 public:
  RaftNodeImpl(const RaftNodeConfig& config,
               std::shared_ptr<StateMachine> sm,
               std::unique_ptr<NetworkTransport> transport,
               std::unique_ptr<TimerService> timer)
      : config_(config),
        state_machine_(sm),
        transport_(std::move(transport)),
        timer_service_(std::move(timer)),
        current_term_(0),
        voted_for_(-1),
        state_(RaftState::FOLLOWER) {}

  Status Start() {
    // 1. 设置 RPC 请求处理器
    transport_->Initialize(config_.listen_addr,
        [this](NodeId from, const std::string& data, std::string& resp) {
          return HandleIncomingRpc(from, data, resp);
        });

    // 2. 启动网络层
    auto status = transport_->Start();
    if (!status.ok()) return status;

    // 3. 启动定时器服务
    timer_service_->Start();

    // 4. 进入 Follower 状态，启动选举定时器
    BecomeFollower();

    return Status::OK();
  }

  Status Stop() {
    timer_service_->Stop();
    transport_->Stop();
    return Status::OK();
  }

  // 发送 RequestVote 给所有 peers
  void BroadcastRequestVote() {
    RequestVoteRequest req(current_term_, config_.node_id, 
                           log_.LastIndex(), log_.LastTerm());
    
    std::string data = Serialize(req);
    
    for (const auto& [peer_id, peer_addr] : peers_) {
      transport_->SendRpc(peer_id, peer_addr, data,
          [this, peer_id](const std::string& response_data) {
            HandleRequestVoteResponse(peer_id, Deserialize(response_data));
          });
    }
  }

  // Leader 发送心跳/日志
  void SendAppendEntries(NodeId peer_id, const NodeAddr& peer_addr) {
    // 构造请求...
    std::string data = Serialize(req);
    
    transport_->SendRpc(peer_id, peer_addr, data,
        [this, peer_id](const std::string& response_data) {
          HandleAppendEntriesResponse(peer_id, Deserialize(response_data));
        });
  }

 private:
  void BecomeFollower() {
    state_ = RaftState::FOLLOWER;
    
    // 使用抽象的定时器服务
    election_timer_id_ = timer_service_->SetTimeout(
        RandomTimeout(config_.election_timeout_ms),
        [this]() { OnElectionTimeout(); });
  }

  void OnElectionTimeout() {
    // 转为 Candidate，发起选举
    BecomeCandidate();
    BroadcastRequestVote();
  }

  void BecomeLeader() {
    state_ = RaftState::LEADER;
    
    // 启动心跳定时器
    heartbeat_timer_id_ = timer_service_->SetInterval(
        std::chrono::milliseconds(config_.heartbeat_interval_ms),
        [this]() { BroadcastHeartbeat(); });
  }

  std::string HandleIncomingRpc(NodeId from, 
                                const std::string& data,
                                std::string& response) {
    // 反序列化，根据类型分发
    auto msg_type = GetMsgType(data);
    
    switch (msg_type) {
      case MsgType::RequestVote:
        return HandleRequestVote(Deserialize<RequestVoteRequest>(data));
      case MsgType::AppendEntries:
        return HandleAppendEntries(Deserialize<AppendEntriesRequest>(data));
      // ...
    }
  }

 private:
  RaftNodeConfig config_;
  std::shared_ptr<StateMachine> state_machine_;
  
  // 依赖注入的组件（不依赖 Asio）
  std::unique_ptr<NetworkTransport> transport_;
  std::unique_ptr<TimerService> timer_service_;
  
  // Raft 状态
  uint64_t current_term_;
  NodeId voted_for_;
  RaftState state_;
  RaftLog log_;
  
  // 定时器 ID（用于取消）
  TimerId election_timer_id_ = 0;
  TimerId heartbeat_timer_id_ = 0;
};
```

## 5. 改造后的 Counter 示例

```cpp
// example/counter/counter_server.cpp（改造后）

#include <rollingraft/raft_node.h>
#include <rollingraft/state_machine.h>
// 注意：不再直接包含 asio 头文件

int main(int argc, char** argv) {
  // 解析参数...
  
  RaftNodeConfig config;
  config.node_id = node_id;
  config.listen_addr = "127.0.0.1:8001";
  config.peers = {...};
  
  auto sm = std::make_shared<CounterMachine>();
  
  // 方式1：使用工厂方法（自动创建 Asio 实现）
  auto node = RaftNode::Create(config, sm);
  
  // 方式2：手动注入依赖（适合测试）
  // auto transport = std::make_unique<AsioNetworkTransport>();
  // auto timer = std::make_unique<AsioTimerService>(io_context);
  // auto node = std::make_unique<RaftNode>(config, sm, 
  //                                        std::move(transport), 
  //                                        std::move(timer));
  
  node->Start();
  // ...
  node->Stop();
  
  return 0;
}
```

## 6. 好处

### 6.1 可测试性

```cpp
// test/test_raft_node.cpp
TEST(RaftNode, Election) {
  // 使用 Mock 网络层，不需要真实网络
  auto mock_transport = std::make_unique<MockNetworkTransport>();
  auto mock_timer = std::make_unique<MockTimerService>();
  
  RaftNodeConfig config{1, "127.0.0.1:1", {2, 3}};
  auto sm = std::make_shared<MockStateMachine>();
  
  RaftNode node(config, sm, 
                std::move(mock_transport), 
                std::move(mock_timer));
  
  node.Start();
  
  // 手动触发选举超时
  mock_timer->TriggerTimeout(election_timer_id);
  
  // 验证是否发送了 RequestVote 给所有节点
  EXPECT_EQ(mock_transport->GetSentMessages().size(), 2);
}
```

### 6.2 可替换性

```cpp
// 嵌入式环境使用裸 socket
class RawSocketTransport : public NetworkTransport { ... };

// 测试环境使用内存队列
class InMemoryTransport : public NetworkTransport { ... };

// 协程环境使用协程网络库
class CoroNetworkTransport : public NetworkTransport { ... };
```

### 6.3 编译隔离

```
include/rollingraft/     # 公共接口，不依赖 Asio
  ├── raft_node.h        # 只依赖 NetworkTransport 接口
  ├── network_transport.h # 纯虚接口
  └── timer_service.h     # 纯虚接口

src/                     # 实现，可以依赖 Asio
  ├── asio_network_transport.cpp
  ├── asio_timer_service.cpp
  └── raft_node.cpp      # 实现也看不到 Asio
```

## 7. 实施步骤

### Phase 1: 接口定义（2-3 小时）
1. 创建 `network_transport.h`
2. 创建 `timer_service.h`
3. 修改 `raft_node.h`，使用新接口

### Phase 2: Asio 实现（4-6 小时）
1. 实现 `AsioNetworkTransport`
2. 实现 `AsioTimerService`
3. 实现 `RaftNode::Create()` 工厂方法

### Phase 3: RaftNode 适配（3-4 小时）
1. 修改 `RaftNodeImpl` 使用抽象接口
2. 移除所有 `asio::` 直接引用
3. 调整 Start/Stop 逻辑

### Phase 4: 验证（2-3 小时）
1. 编译通过
2. Counter 示例运行正常
3. 编写 Mock 测试验证可测试性

**总时间：约 15 小时**

## 8. 风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| Asio 线程模型复杂 | 调试困难 | 保持 io_context 在 Transport 内部管理 |
| 性能开销（虚函数） | 延迟增加 | 使用 final 优化，实际影响小 |
| 定时器精度 | 选举不稳定 | AsioTimerService 使用 steady_timer |
| 回调生命周期 | 空指针 | 使用 shared_ptr/weak_ptr 管理 |
