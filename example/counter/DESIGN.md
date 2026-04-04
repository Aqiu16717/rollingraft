# Counter Example 设计文档

## 1. 概述

Counter 是一个基于 RollingRaft 的分布式计数器示例，演示如何使用 RollingRaft 构建一个简单的分布式应用。

### 功能特性

- **inc**: 计数器 +1
- **dec**: 计数器 -1  
- **add N**: 计数器 +N
- **sub N**: 计数器 -N
- **get**: 获取当前值（读操作，待实现）

### 架构特点

```
┌─────────────────────────────────────────────────────────────┐
│                         Client                              │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  CounterClient                                        │ │
│  │  - 维护 server 列表                                   │ │
│  │  - 自动 Leader 发现/切换                              │ │
│  │  - 支持重定向                                         │ │
│  └────────────────────┬──────────────────────────────────┘ │
└───────────────────────┼─────────────────────────────────────┘
                        │ RPC (待实现)
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│   Node 1      │ │   Node 2      │ │   Node 3      │
│  (Leader?)    │ │  (Follower)   │ │  (Follower)   │
├───────────────┤ ├───────────────┤ ├───────────────┤
│  RaftNode     │ │  RaftNode     │ │  RaftNode     │
│ ├─Server      │ │ ├─Server      │ │ ├─Server      │
│ └─CounterMachine│ │ └─CounterMachine│ │ └─CounterMachine│
└───────────────┘ └───────────────┘ └───────────────┘
```

---

## 2. 组件设计

### 2.1 CounterSnapshot

快照实现，用于状态机快照和恢复。

```cpp
class CounterSnapshot : public rollingraft::Snapshot {
 public:
  CounterSnapshot(int64_t value, uint64_t index, uint64_t term);
  
  const rollingraft::SnapshotMeta& GetMeta() const override;
  size_t Read(uint64_t offset, uint8_t* dest, size_t length) override;
  std::string GetPath() const override;

 private:
  rollingraft::SnapshotMeta meta_;
  int64_t value_;
  std::vector<uint8_t> data_;
};
```

**数据格式**: 简单的二进制 `int64_t` 值

### 2.2 CounterMachine（状态机）

用户实现的状态机，处理计数器逻辑。

```cpp
class CounterMachine : public rollingraft::StateMachine {
 public:
  CounterMachine();
  
  // 应用已提交的命令
  rollingraft::ApplyResult Apply(std::span<const uint8_t> data, 
                                  uint64_t index) override;
  
  // 获取最后应用的索引
  uint64_t GetLastAppliedIndex() const override;
  
  // 创建快照
  std::shared_ptr<rollingraft::Snapshot> CreateSnapshot() override;
  
  // 从快照恢复
  bool Restore(const std::vector<uint8_t>& snapshot) override;
  
  // 等待指定索引
  void WaitIndex(uint64_t index, std::function<void()> cb) override;
  
  // 获取当前值（用于演示输出）
  int64_t GetValue() const;

 private:
  void NotifyWaiters(uint64_t index);
  
  mutable std::mutex mtx_;
  int64_t value_;
  std::atomic<uint64_t> last_applied_index_;
  std::multimap<uint64_t, std::function<void()>> waiters_;
};
```

**命令解析**:

| 命令 | 操作 | 实现 |
|------|------|------|
| `inc` | value++ | `++value_` |
| `dec` | value-- | `--value_` |
| `add N` | value += N | `value_ += stoll(cmd.substr(4))` |
| `sub N` | value -= N | `value_ -= stoll(cmd.substr(4))` |

**Apply 返回结果**:
```cpp
rollingraft::ApplyResult result;
result.success_ = true;
result.response_ = std::to_string(value_);  // 返回当前值
result.applied_index_ = index;
```

### 2.3 服务端主流程

```cpp
int main(int argc, char** argv) {
  // 1. 解析命令行参数
  //    argv[1]: node_id
  //    argv[2]: listen_port  
  //    argv[3+]: peer_ports
  
  // 2. 构造配置
  rollingraft::RaftNodeConfig config;
  config.node_id = node_id;
  config.listen_addr = "127.0.0.1:" + listen_port;
  config.data_dir = "./data/node" + std::to_string(node_id);
  config.peers = {...};
  
  // 3. 创建 StateMachine
  auto sm = std::make_unique<CounterMachine>();
  
  // 4. 创建并启动 RaftNode
  rollingraft::RaftNode node(config, sm);
  node.Start();
  
  // 5. 主循环：如果是 Leader，定期打印当前值
  while (g_running) {
    sleep(3s);
    if (node.IsLeader()) {
      std::cout << "Current count value: " << sm->GetValue() << std::endl;
    }
  }
  
  // 6. 关闭
  node.Stop();
}
```

### 2.4 CounterClient（客户端）

```cpp
class CounterClient {
 public:
  explicit CounterClient(const std::vector<std::string>& servers);
  
  void SendCommand(const std::string& cmd);

 private:
  std::vector<std::string> servers_;
  size_t connect_idx_;      // 当前连接的节点索引
  uint64_t client_id_;      // 客户端唯一标识
  uint64_t seq_;            // 请求序列号
};
```

**发送逻辑**:

```cpp
void SendCommand(const std::string& cmd) {
  // 构造请求
  rollingraft::ClientRequest req;
  req.command = cmd;
  req.client_id = client_id_;
  req.seq = seq_++;
  
  while (!success) {
    // 尝试向当前节点发送
    rollingraft::Status status = rollingraft::RpcCall(addr, req, resp);
    
    if (status.ok()) {
      if (resp.success) {
        // 成功，打印响应
        success = true;
      } else {
        // 被重定向到 Leader
        current_addr_ = resp.leader_addr;
      }
    } else {
      // 连接失败，尝试下一个节点
      connect_idx_ = (connect_idx_ + 1) % servers_.size();
      sleep(500ms);
    }
  }
}
```

---

## 3. 客户端协议

### 3.1 消息结构（预期实现）

```cpp
struct ClientRequest {
  uint64_t client_id;       // 客户端唯一标识
  uint64_t seq;             // 序列号（用于幂等）
  std::string command;      // 命令（如 "inc", "add 100"）
};

struct ClientResponse {
  bool success;             // 是否成功处理
  std::string response;     // 响应内容（如当前计数值）
  std::string error;        // 错误信息
  
  // 重定向信息
  uint64_t leader_id;       // Leader 节点 ID
  std::string leader_addr;  // Leader 地址
};
```

### 3.2 交互流程

**成功场景**:
```
Client -> Leader: ClientRequest {cmd="inc"}
Leader -> Client: ClientResponse {success=true, response="42"}
```

**重定向场景**:
```
Client -> Follower: ClientRequest {cmd="inc"}
Follower -> Client: ClientResponse {success=false, leader_addr="127.0.0.1:8001"}
Client -> Leader: ClientRequest {cmd="inc"}
Leader -> Client: ClientResponse {success=true, response="42"}
```

**节点故障场景**:
```
Client -> Node1: ClientRequest (timeout)
Client -> Node2: ClientRequest (timeout)
Client -> Node3: ClientRequest {cmd="inc"}
Node3 -> Client: ClientResponse {success=true, response="42"}
```

---

## 4. 当前实现状态

### 4.1 已实现

| 组件 | 状态 | 说明 |
|------|------|------|
| CounterMachine | ✅ | 完整实现 Apply/Snapshot/Restore/WaitIndex |
| CounterSnapshot | ✅ | 二进制序列化 |
| 服务端启动流程 | ✅ | 参数解析、信号处理、主循环 |
| 客户端框架 | ✅ | 类结构、重试逻辑 |

### 4.2 依赖（待实现）

| 组件 | 状态 | 说明 |
|------|------|------|
| ClientRequest | ❌ | 需要定义结构 |
| ClientResponse | ❌ | 需要定义结构 |
| RpcCall | ❌ | 需要实现 RPC 调用函数 |
| 服务端请求处理 | ❌ | Server 需要处理 ClientRequest |

---

## 5. 部署和使用

### 5.1 编译

```bash
cd /path/to/rollingraft
mkdir -p build && cd build
cmake ..
make example_counter_server example_counter_client
```

### 5.2 启动集群

**终端 1** (Node 1):
```bash
./example_counter_server 1 8001 8002 8003
```

**终端 2** (Node 2):
```bash
./example_counter_server 2 8002 8001 8003
```

**终端 3** (Node 3):
```bash
./example_counter_server 3 8003 8001 8002
```

### 5.3 客户端操作（待 RPC 实现后）

```bash
./example_counter_client 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003

> inc
[Client] Sending 'inc' to Node 0 (127.0.0.1:8001)...
[Client] Success. Response: 1
> add 100
[Client] Sending 'add 100' to Node 0 (127.0.0.1:8001)...
[Client] Success. Response: 101
> exit
```

---

## 6. 代码文件说明

| 文件 | 说明 |
|------|------|
| `counter_server.cpp` | 服务端实现，包含 CounterMachine、CounterSnapshot、main 函数 |
| `counter_client.cpp` | 客户端实现，包含 CounterClient、交互式命令行 |
| `DESIGN.md` | 本文档 |
| `README.md` | 使用说明 |

---

## 7. 关键代码片段

### 7.1 CounterMachine::Apply

```cpp
rollingraft::ApplyResult Apply(std::span<const uint8_t> data, uint64_t index) {
  std::lock_guard<std::mutex> lock(mtx_);
  
  std::string cmd(data.begin(), data.end());
  
  if (cmd == "inc") {
    ++value_;
  } else if (cmd == "dec") {
    --value_;
  } else if (cmd.rfind("add ", 0) == 0) {
    int64_t delta = std::stoll(cmd.substr(4));
    value_ += delta;
  } else if (cmd.rfind("sub ", 0) == 0) {
    int64_t delta = std::stoll(cmd.substr(4));
    value_ -= delta;
  }
  
  last_applied_index_ = index;
  
  rollingraft::ApplyResult result;
  result.success_ = true;
  result.response_ = std::to_string(value_);
  result.applied_index_ = index;
  
  NotifyWaiters(index);
  return result;
}
```

### 7.2 主循环（Leader 检测）

```cpp
while (g_running) {
  std::this_thread::sleep_for(std::chrono::seconds(3));
  if (node.IsLeader()) {
    std::cout << "Current count value: " << sm->GetValue() << std::endl;
  }
}
```

---

## 8. 待办事项

1. **定义 ClientRequest/ClientResponse** - 添加到 rpc.h 或新建 client_rpc.h
2. **实现 RpcCall 函数** - 基于 Asio 的同步/异步 RPC 调用
3. **服务端处理 ClientRequest** - 在 Server 或 CommandHandler 中实现
4. **实现 Leader 重定向** - Follower 返回当前 Leader 信息
5. **添加 get 命令支持** - 直接读 StateMachine，不经过 Raft
6. **实现请求去重** - 基于 client_id + seq 的幂等处理
