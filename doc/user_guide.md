# RollingRaft 用户指南

## 1. 快速开始

### 1.1 最小示例

```cpp
#include <rollingraft/raft_node.h>
#include <rollingraft/state_machine.h>
#include <iostream>

// 1. 实现你的状态机
class MyStateMachine : public rollingraft::StateMachine {
 public:
  void Apply(const std::vector<uint8_t>& cmd) override {
    std::string s(cmd.begin(), cmd.end());
    std::cout << "Applied: " << s << std::endl;
  }

  std::vector<uint8_t> Query(const std::vector<uint8_t>& query) override {
    return {};  // 简单示例，不处理查询
  }
};

int main() {
  // 2. 配置节点
  rollingraft::RaftNode::Config config;
  config.node_id = 1;
  config.listen_addr = "0.0.0.0:8001";
  config.peers = {"127.0.0.1:8002", "127.0.0.1:8003"};
  config.data_dir = "./data/node1";

  // 3. 创建并启动
  auto sm = std::make_unique<MyStateMachine>();
  rollingraft::RaftNode node(config, std::move(sm));

  node.Start();
  std::cout << "Node started, waiting for election..." << std::endl;

  // 4. 等待成为 Leader 后提交命令
  std::this_thread::sleep_for(std::chrono::seconds(2));

  if (node.IsLeader()) {
    std::string cmd = "hello world";
    auto future = node.Propose(
      std::vector<uint8_t>(cmd.begin(), cmd.end())
    );

    auto status = future.get();
    if (status.ok()) {
      std::cout << "Command committed successfully!" << std::endl;
    }
  }

  // 5. 运行一段时间
  std::this_thread::sleep_for(std::chrono::seconds(30));
  node.Stop();

  return 0;
}
```

### 1.2 编译运行

```bash
# CMakeLists.txt
find_package(rollingraft REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app rollingraft::rollingraft)

# 编译
mkdir build && cd build
cmake ..
make

# 运行 3 节点集群
./my_app 1 8001 &  # 节点 1
./my_app 2 8002 &  # 节点 2
./my_app 3 8003 &  # 节点 3
```

---

## 2. 核心概念

### 2.1 三个角色

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Leader  │────>│ Follower│<────│Follower │
│  (主)   │     │  (从)   │     │  (从)   │
└─────────┘     └─────────┘     └─────────┘
     │                               │
     └───────────────────────────────┘
              复制日志
```

- **Leader**: 唯一可写，处理所有客户端请求
- **Follower**: 只读，复制 Leader 的日志
- **Candidate**: 选举中的临时状态

### 2.2 用户需要实现什么

```
┌─────────────────────────────────────────┐
│           RollingRaft (库提供)           │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐ │
│  │  Network│  │  Raft   │  │ Storage │ │
│  │ (Asio)  │  │ Consensus│  │(Files)  │ │
│  └────┬────┘  └────┬────┘  └────┬────┘ │
│       └─────────────┴─────────────┘     │
│                   │                     │
│              Apply()                    │
│                   │                     │
└───────────────────┼─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│         StateMachine (你实现)            │
│                                         │
│   class KVStore : public StateMachine { │
│     void Apply(cmd) {                   │
│       // 解析命令                       │
│       // 更新本地状态                   │
│     }                                   │
│                                         │
│     std::vector<uint8_t> Query(q) {     │
│       // 读取本地状态                   │
│       // 返回结果                       │
│     }                                   │
│   };                                    │
│                                         │
└─────────────────────────────────────────┘
```

**你只需要实现 StateMachine**，其他都由库管理。

### 2.3 写操作 vs 读操作

```cpp
// 写操作 - 通过 Raft 日志，保证一致性
auto future = node.Propose(command);
auto status = future.get();  // 等待多数派确认

// 读操作 - 直接读本地 StateMachine，不经过 Raft
auto result = node.Query(query);  // 立即返回
```

**注意**: 读操作不保证线性一致性（可能读到旧数据）。如果需要强一致读：

```cpp
// 方法 1: 只在 Leader 上读（可能读到 Leader 未提交的）
if (node.IsLeader()) {
  result = node.Query(key);
}

// 方法 2: 先 Propose 一个 ReadIndex（待实现）
auto future = node.ReadIndex();
future.wait();  // 等待确认是 Leader
result = node.Query(key);  // 现在安全了
```

---

## 3. 完整示例：KV Store

### 3.1 状态机实现

```cpp
#include <rollingraft/state_machine.h>
#include <map>
#include <string>
#include <sstream>

// 命令格式: "SET key value" 或 "GET key"
class KVStateMachine : public rollingraft::StateMachine {
 public:
  // 应用写命令（由 RaftNode 调用，保证所有节点顺序一致）
  void Apply(const std::vector<uint8_t>& command) override {
    std::string cmd(command.begin(), command.end());

    std::istringstream iss(cmd);
    std::string op, key, value;
    iss >> op >> key >> value;

    if (op == "SET") {
      data_[key] = value;
      std::cout << "[Apply] SET " << key << " = " << value << std::endl;
    } else if (op == "DEL") {
      data_.erase(key);
      std::cout << "[Apply] DEL " << key << std::endl;
    }
  }

  // 处理读命令（本地读，不经过 Raft）
  std::vector<uint8_t> Query(const std::vector<uint8_t>& query) override {
    std::string key(query.begin(), query.end());

    auto it = data_.find(key);
    if (it != data_.end()) {
      return std::vector<uint8_t>(it->second.begin(), it->second.end());
    }
    return {};  // 空表示不存在
  }

  // 生成快照（用于日志压缩）
  std::vector<uint8_t> Snapshot() override {
    std::string data;
    for (const auto& [k, v] : data_) {
      data += k + "\t" + v + "\n";
    }
    return std::vector<uint8_t>(data.begin(), data.end());
  }

  // 从快照恢复
  void Restore(const std::vector<uint8_t>& snapshot) override {
    data_.clear();
    std::string data(snapshot.begin(), snapshot.end());
    std::istringstream iss(data);
    std::string line;
    while (std::getline(iss, line)) {
      auto pos = line.find('\t');
      if (pos != std::string::npos) {
        data_[line.substr(0, pos)] = line.substr(pos + 1);
      }
    }
  }

 private:
  std::map<std::string, std::string> data_;
};
```

### 3.2 服务器程序

```cpp
#include <rollingraft/raft_node.h>
#include <iostream>
#include <thread>
#include <signal.h>

// 解析命令行参数
struct Options {
  uint64_t node_id;
  std::string listen_addr;
  std::vector<std::string> peers;
  std::string data_dir;
};

Options ParseArgs(int argc, char** argv) {
  Options opt;
  opt.node_id = std::stoul(argv[1]);
  opt.listen_addr = "0.0.0.0:" + std::string(argv[2]);
  opt.data_dir = "./data/node" + std::string(argv[1]);

  // 从配置文件或命令行读取其他节点
  if (argc > 3) {
    for (int i = 3; i < argc; i++) {
      opt.peers.push_back(argv[i]);
    }
  }

  return opt;
}

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0]
              << " <node_id> <port> [peer1] [peer2] ..." << std::endl;
    return 1;
  }

  auto opt = ParseArgs(argc, argv);

  // 配置
  rollingraft::RaftNode::Config config;
  config.node_id = opt.node_id;
  config.listen_addr = opt.listen_addr;
  config.peers = opt.peers;
  config.data_dir = opt.data_dir;

  // 创建节点
  auto sm = std::make_unique<KVStateMachine>();
  rollingraft::RaftNode node(config, std::move(sm));

  // 信号处理
  std::atomic<bool> running{true};
  signal(SIGINT, [&running](int) { running = false; });

  // 启动
  node.Start();
  std::cout << "Node " << opt.node_id << " started on "
            << opt.listen_addr << std::endl;

  // 简单的命令行界面
  std::thread cmd_thread([&node, &running]() {
    while (running) {
      std::cout << "> " << std::flush;

      std::string line;
      if (!std::getline(std::cin, line)) break;

      std::istringstream iss(line);
      std::string cmd;
      iss >> cmd;

      if (cmd == "put" || cmd == "SET") {
        std::string key, value;
        iss >> key >> value;

        if (!node.IsLeader()) {
          std::cout << "Not leader. Current leader: "
                    << node.LeaderAddr() << std::endl;
          continue;
        }

        std::string proposal = "SET " + key + " " + value;
        auto future = node.Propose(
          std::vector<uint8_t>(proposal.begin(), proposal.end())
        );

        auto status = future.get();
        if (status.ok()) {
          std::cout << "OK" << std::endl;
        } else {
          std::cout << "Failed: " << status.ToString() << std::endl;
        }

      } else if (cmd == "get" || cmd == "GET") {
        std::string key;
        iss >> key;

        auto result = node.Query(
          std::vector<uint8_t>(key.begin(), key.end())
        );

        if (result.empty()) {
          std::cout << "(nil)" << std::endl;
        } else {
          std::cout << std::string(result.begin(), result.end())
                    << std::endl;
        }

      } else if (cmd == "status") {
        std::cout << "Node ID: " << node.Id() << std::endl;
        std::cout << "State: " << (node.IsLeader() ? "Leader" : "Follower")
                  << std::endl;
        std::cout << "Term: " << node.CurrentTerm() << std::endl;
        std::cout << "Leader: " << node.LeaderAddr() << std::endl;

      } else if (cmd == "quit" || cmd == "exit") {
        running = false;
      } else {
        std::cout << "Unknown command: " << cmd << std::endl;
        std::cout << "Commands: put/get/status/quit" << std::endl;
      }
    }
  });

  // 等待退出
  while (running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  std::cout << "Shutting down..." << std::endl;
  node.Stop();
  cmd_thread.join();

  return 0;
}
```

### 3.3 运行集群

```bash
# 创建数据目录
mkdir -p data/node1 data/node2 data/node3

# 启动 3 个节点
./kv_store 1 8001 127.0.0.1:8002 127.0.0.1:8003 &
./kv_store 2 8002 127.0.0.1:8001 127.0.0.1:8003 &
./kv_store 3 8003 127.0.0.1:8001 127.0.0.1:8002 &

# 在节点 1 上操作（假设它成为 Leader）
> put foo bar
OK
> get foo
bar
> status
Node ID: 1
State: Leader
Term: 5
Leader: 127.0.0.1:8001
```

---

## 4. 配置详解

### 4.1 RaftNode::Config

```cpp
struct Config {
  // 节点标识
  uint64_t node_id;              // 必填，节点唯一 ID
  std::string listen_addr;       // 必填，监听地址，如 "0.0.0.0:8001"
  std::vector<std::string> peers; // 必填，其他节点地址列表

  // 存储
  std::string data_dir;          // 数据目录，存储日志和快照

  // 超时配置（通常不需要修改）
  uint32_t election_timeout_ms = 150;   // 选举超时
  uint32_t heartbeat_interval_ms = 50;  // 心跳间隔
  uint32_t max_log_entries_per_rpc = 100;  // 每次 RPC 最大日志条目数
};
```

### 4.2 配置文件示例

```json
// config.json
{
  "node_id": 1,
  "listen_addr": "0.0.0.0:8001",
  "peers": [
    "192.168.1.101:8001",
    "192.168.1.102:8001"
  ],
  "data_dir": "/var/lib/myapp/data",
  "election_timeout_ms": 300,
  "heartbeat_interval_ms": 100
}
```

```cpp
// 加载配置
auto config = LoadConfigFromFile("config.json");
RaftNode node(config, std::move(sm));
```

---

## 5. 高级用法

### 5.1 监听状态变化

```cpp
class MyStateMachine : public StateMachine {
 public:
  void OnStateChange(State old_state, State new_state) {
    if (new_state == State::Leader) {
      std::cout << "I become leader!" << std::endl;
      // 可以在这里加载缓存等
    }
  }

  void OnCommit(uint64_t index) {
    std::cout << "Log " << index << " committed" << std::endl;
  }
};
```

### 5.2 批量写入

```cpp
// 批量提交多个命令，减少网络往返
std::vector<std::future<Status>> futures;
for (const auto& cmd : commands) {
  futures.push_back(node.Propose(cmd));
}

// 等待全部完成
for (auto& f : futures) {
  auto status = f.get();
  if (!status.ok()) {
    // 处理错误
  }
}
```

### 5.3 优雅关闭

```cpp
// 1. 停止接受新请求
http_server.StopAccepting();

// 2. 等待正在进行的提交完成
node.WaitForCommit(last_proposed_index);

// 3. 创建快照（加速下次启动）
node.Snapshot();

// 4. 停止节点
node.Stop();
```

---

## 6. 常见问题

### Q1: 如何判断当前节点是 Leader？

```cpp
if (node.IsLeader()) {
  // 可以执行写操作
}
```

### Q2: Propose 返回失败怎么办？

```cpp
auto future = node.Propose(cmd);
auto status = future.get();

if (!status.ok()) {
  if (status.IsNotLeader()) {
    // 重定向到 Leader
    auto leader_addr = node.LeaderAddr();
    RedirectTo(leader_addr, cmd);
  } else if (status.IsTimeout()) {
    // 重试
    Retry(cmd);
  }
}
```

### Q3: 读操作如何保证一致性？

方案 1: 只读 Leader（可能读到未提交的）
```cpp
if (!node.IsLeader()) {
  return RedirectToLeader();
}
return node.Query(key);
```

方案 2: 使用 ReadIndex（强一致，待实现）
```cpp
auto future = node.ReadIndex();
future.wait();  // 等待确认是 Leader 且日志最新
return node.Query(key);
```

### Q4: 节点崩溃了如何恢复？

```cpp
// 只需用相同的 node_id 和 data_dir 重新启动
// Raft 会自动从日志恢复状态
RaftNode node(same_config, std::move(sm));
node.Start();  // 自动恢复
```

### Q5: 如何扩容（添加新节点）？

目前版本不支持动态成员变更。需要：
1. 停止所有节点
2. 修改配置添加新节点
3. 重新启动所有节点

未来版本会支持动态扩容。

---

## 7. 完整示例代码

参见 `example/kv_store/` 目录：
- `kv_store.h/cpp` - KV 状态机实现
- `main.cpp` - 服务器程序
- `client.cpp` - 客户端程序（可选）

---

## 8. 调试技巧

### 启用详细日志

```cpp
// 设置日志级别
rollingraft::SetLogLevel(rollingraft::LogLevel::DEBUG);

// 或在环境变量中设置
export ROLLINGRAFT_LOG_LEVEL=debug
```

### 查看节点状态

```cpp
// 定时打印状态
std::thread([]() {
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cout << "State: " << node.GetState()
              << " Term: " << node.CurrentTerm()
              << " Commit: " << node.GetCommitIndex()
              << std::endl;
  }
}).detach();
```

### 网络抓包

```bash
# 查看 Raft 节点间的通信
tcpdump -i lo port 8001 or port 8002 or port 8003 -X
```

---

## 9. 性能调优

### 批量提交
不要逐个提交小命令，尽量批量：
```cpp
// 差：每次提交都等待网络往返
for (auto& cmd : cmds) {
  node.Propose(cmd).get();  // 阻塞等待
}

// 好：批量提交，一次网络往返
std::vector<std::future<>> futures;
for (auto& cmd : cmds) {
  futures.push_back(node.Propose(cmd));
}
for (auto& f : futures) f.get();
```

### 调整超时参数
- 网络延迟高 → 增加 election_timeout_ms
- 写入频繁 → 减少 heartbeat_interval_ms

### 使用快照
定期创建快照，减少日志大小，加速重启：
```cpp
// 每 10000 条日志创建一个快照
if (log_count % 10000 == 0) {
  node.Snapshot();
}
```
