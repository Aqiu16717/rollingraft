# RollingRaft 代码注释风格规范

> **版本**: v1.0  
> **日期**: 2026-04-04  
> **状态**: 评审中

---

## 1. 设计原则

### 1.1 核心原则

| 原则 | 说明 |
|------|------|
| **英文优先** | 所有注释使用英文，确保国际化 |
| **自解释代码** | 命名清晰，减少不必要的注释 |
| **注释"为什么"** | 解释设计意图，而非"做了什么" |
| **保持简洁** | 一行注释不超过 80 字符 |

### 1.2 注释层次

```
文件头 (可选) -> 类/结构体 -> 公共 API -> 复杂逻辑
     ↑                ↑            ↑           ↑
   版权说明        整体职责      使用方法     算法解释
```

---

## 2. 注释风格

### 2.1 文件头注释

**新文件必须添加**：

```cpp
/**
 * @file raft_node.cpp
 * @brief Raft consensus node implementation
 * 
 * Core Raft state machine handling leader election,
 * log replication, and membership changes.
 */
```

**已有文件简化版**（如果已有版权声明）：

```cpp
// raft_node.cpp - Raft consensus node implementation
```

### 2.2 类/结构体注释

**使用 Doxygen 风格 `/** */`**：

```cpp
/**
 * Represents a Raft consensus node.
 * 
 * Manages the Raft state machine including:
 * - Leader election with randomized timeouts
 * - Log replication to followers
 * - Snapshot management for log compaction
 * - Dynamic membership changes
 * 
 * Thread-safe: all public methods can be called from any thread.
 */
class RaftNode {
    // ...
};

/**
 * Cluster configuration containing current node set.
 * Thread-safe for read operations.
 */
struct ClusterConfig {
    std::vector<NodeId> nodes;  // Current cluster node IDs
    uint64_t version = 0;       // Config version, incremented on each change
};
```

### 2.3 函数/方法注释

**公共 API 必须注释，私有方法可选**：

```cpp
/**
 * Propose a command to be replicated across the cluster.
 * 
 * Only the leader can propose commands. If called on a non-leader,
 * returns NotLeader error with current leader info.
 * 
 * @param command The command data to replicate
 * @param callback Called when command is applied or fails
 * @return Status::OK() if proposal was accepted (not yet applied)
 * @note Callback may be called asynchronously from a different thread
 * 
 * Example:
 *   auto status = node.Propose("set x 1", [](const ApplyResult& r) {
 *       if (r.success_) { /* handle success */ }
 *   });
 */
Status Propose(const std::string& command, 
               std::function<void(const ApplyResult&)> callback);

/**
 * Check if this node is the current leader.
 * @return true if node is leader and can accept proposals
 */
bool IsLeader() const;
```

### 2.4 行内注释

**使用 `//`，简洁明了**：

```cpp
// Good: 解释"为什么"
// Randomize timeout to prevent split votes
uint64_t timeout = base_timeout_ + rand() % base_timeout_;

// Bad: 重复代码显而易见的逻辑
// Add 1 to the index
++index;

// Good: 复杂算法的解释
// Binary search for the last matching entry
// Invariant: entries[lo].term <= target_term <= entries[hi].term
while (lo < hi) { ... }
```

### 2.5 TODO/FIXME 注释

**统一格式，便于搜索**：

```cpp
// TODO(aq1u): Add batch propose for better throughput
// FIXME: Handle edge case when all nodes are partitioned
// NOTE: LevelDB iterator is not thread-safe
// HACK: Workaround for rare race condition, remove after refactoring
```

---

## 3. 代码示例对比

### 3.1 当前问题代码

```cpp
// 有中文注释，风格不一
// LevelDB 是有序的，我们可以使用迭代器
std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(...));

void SetRoleChangeCallback(std::function<void(RaftNodeRole role, Term term)> callback);  // set cb befor calling Start()
```

### 3.2 规范后代码

```cpp
// LevelDB is sorted, use iterator for range scan
std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(...));

/**
 * Set callback for role state changes (follower/candidate/leader).
 * Must be called before Start().
 */
void SetRoleChangeCallback(
    std::function<void(RaftNodeRole role, Term term)> callback);
```

---

## 4. 注释内容指南

### 4.1 应该注释的内容

| 场景 | 示例 |
|------|------|
| 公共 API | 功能、参数、返回值、线程安全 |
| 复杂算法 | 算法思路、复杂度、边界条件 |
| 非直观代码 | 为什么这样写，而非做了什么 |
| 临时方案 | TODO 标记，说明何时移除 |
| 性能考虑 | 为什么用 A 而不是 B |

### 4.2 不应该注释的内容

| 场景 | 反例 |
------|------|
| 显而易见的逻辑 | `// Increment counter` |
| 命名清晰的操作 | `node.Start(); // Start the node` |
| 过时注释 | 代码改了，注释没改 |
| 冗余注释 | `// Getter for name` on `GetName()` |
| 大段注释代替好命名 | 应该用函数名表达意图 |

---

## 5. 现有代码迁移计划

### 5.1 优先级

| 优先级 | 范围 | 工作量 | 说明 |
|--------|------|--------|------|
| P1 | `include/rollingraft/*.h` | 中 | 公共 API，影响用户 |
| P2 | `src/*_persister.cpp` | 小 | 中文注释需要翻译 |
| P3 | `src/*.cpp` 实现文件 | 低 | 逐步改进 |

### 5.2 迁移策略

1. **新代码必须遵守规范** - 立即执行
2. **修改旧代码时顺手更新注释** - 渐进改进
3. **不专门大规模重写注释** - 避免无价值变更

### 5.3 检查清单

提交前自检：

- [ ] 新增公共 API 有 Doxygen 注释
- [ ] 无中文注释（遗留代码除外）
- [ ] 注释解释"为什么"而非"是什么"
- [ ] TODO/FIXME 有统一格式
- [ ] 无与代码不符的过时注释

---

## 6. 工具支持

### 6.1 Doxygen 生成文档

```bash
# 安装 doxygen
brew install doxygen  # macOS
apt-get install doxygen  # Ubuntu

# 生成配置文件
doxygen -g Doxyfile

# 编辑配置，启用提取
doxygen Doxyfile
```

### 6.2 clang-format 配置

`.clang-format` 已包含注释格式化：

```yaml
CommentPragmas: '^ IWYU pragma:'
ReflowComments: true
SortIncludes: true
```

### 6.3 CI 检查（可选）

```bash
# 检查中文注释（示例脚本）
grep -r "[\u4e00-\u9fa5]" src/ include/ || echo "No Chinese comments found"
```

---

## 7. 示例：完整头文件规范

```cpp
/**
 * @file raft_node.h
 * @brief Public API for Raft consensus node
 * 
 * RollingRaft is a C++ implementation of the Raft consensus algorithm.
 * This header provides the main RaftNode class for building distributed
 * systems with strong consistency guarantees.
 * 
 * @see https://raft.github.io/ for Raft algorithm details
 */

#pragma once

#include <functional>
#include <memory>

namespace rollingraft {

/**
 * Raft node role states.
 * 
 * Followers are passive and respond to requests.
 * Candidates initiate elections.
 * Leaders handle all client requests.
 */
enum class RaftNodeRole { kFollower, kCandidate, kLeader };

/**
 * Main Raft consensus node.
 * 
 * Thread-safety: All public methods are thread-safe.
 * Lifecycle: Create -> Configure -> Start -> [Use] -> Stop.
 */
class RaftNode {
 public:
  /**
   * Create a new Raft node.
   * @param config Node configuration (ID, peers, paths)
   * @param sm User state machine for applying committed commands
   */
  RaftNode(const RaftNodeConfig& config, std::shared_ptr<StateMachine> sm);
  
  ~RaftNode();
  
  // Non-copyable, non-movable
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  /**
   * Start the node and join the cluster.
   * @return Status::OK() on success
   * @note Blocks until initial setup completes
   */
  Status Start();

  /**
   * Gracefully stop the node.
   * @return Status::OK() on success
   */
  Status Stop();

  /**
   * Propose a command for cluster-wide replication.
   * 
   * @param command Opaque command data for state machine
   * @param callback Invoked when command is applied or fails
   * @return Status error if not leader or node stopped
   * 
   * Example:
   *   node.Propose("inc", [](const ApplyResult& r) {
   *     if (r.success_) LOG_INFO("Applied at index {}", r.applied_index_);
   *   });
   */
  Status Propose(const std::string& command,
                 std::function<void(const ApplyResult&)> callback);

  /**
   * Check if this node is the leader.
   * @return true if currently leader
   */
  bool IsLeader() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;  // PIMPL idiom for API stability
};

}  // namespace rollingraft
```

---

## 8. 文件头完整规范

### 8.1 必须包含的信息

```cpp
/**
 * @file raft_node.cpp
 * @brief Raft consensus node implementation
 * @author Your Name <email@example.com>
 * @date 2026-04-07
 * 
 * Core Raft state machine handling leader election,
 * log replication, and membership changes.
 * 
 * @copyright Copyright (c) 2026
 * @license MIT License
 */
```

| 字段 | 必需 | 说明 |
|------|------|------|
| `@file` | ✅ | 文件名 |
| `@brief` | ✅ | 一句话描述 |
| `@author` | ✅ | 作者（个人项目可省略） |
| `@date` | ❌ | 创建/最后修改日期 |
| 功能描述 | ✅ | 多行详细说明 |
| `@copyright` | ❌ | 版权信息 |
| `@license` | ❌ | 许可证 |

### 8.2 头文件 vs 实现文件

**头文件（.h）** - 必须完整：
```cpp
/**
 * @file raft_node.h
 * @brief Public API for Raft consensus node
 * @author Aqiu <aqiu16717@gmail.com>
 * 
 * RollingRaft C++20 Raft consensus implementation.
 * Provides distributed state machine replication
 * with strong consistency guarantees.
 */
```

**实现文件（.cpp）** - 可简化：
```cpp
/**
 * @file raft_node.cpp
 * @brief Raft consensus node implementation
 * 
 * Implementation details: leader election, log replication,
 * snapshot management, and membership changes.
 */
```

---

## 9. 代码全覆盖注释要求

### 9.1 必须注释的内容

```cpp
// 1. 所有公共类
/**
 * Raft consensus node implementation.
 * 
 * Thread-safe, supports dynamic membership changes.
 * Lifecycle: Create -> Configure -> Start -> Use -> Stop.
 */
class RaftNode { ... };

// 2. 所有公共方法（非getter/setter）
/**
 * Propose a command for replication.
 * @param command Data to replicate
 * @param callback Called when applied or failed
 * @return Status::OK() if accepted by leader
 * @throws Never throws
 * @note Callback may be called from different thread
 */
Status Propose(const std::string& command, Callback callback);

// 3. 所有复杂算法
/**
 * Find log conflict index using binary search.
 * Complexity: O(log n)
 * Invariant: entries[lo].term <= target <= entries[hi].term
 */
Index FindConflictIndex(Term target);

// 4. 所有非直观设计
// Why not use std::mutex? Asio strand provides better async integration
asio::io_context::strand strand_;

// 5. 所有TODO/FIXME
// TODO(aq1u): Optimize with batch propose for throughput
// FIXME: Handle edge case when all nodes partitioned
// NOTE: LevelDB iterator is not thread-safe
// HACK: Workaround for race condition, remove after refactor
```

### 9.2 不需要注释的内容

```cpp
// ❌ 不需要 - 显而易见的getter
/**
 * Get the node ID.
 * @return The node ID
 */
NodeId GetNodeId() const { return node_id_; }

// ✅ 正确 - 直接省略注释
NodeId GetNodeId() const { return node_id_; }

// ❌ 不需要 - 单行逻辑自解释
// Increment the counter
counter_++;

// ✅ 正确 - 直接写代码
counter_++;
```

---

## 10. 提交前检查清单（Pre-commit Checklist）

### 10.1 自检流程

```bash
# 1. 检查待提交文件
git status

# 2. 检查变更内容
git diff --cached --stat
git diff --cached

# 3. 编译检查
cd build && make -j$(sysctl -n hw.ncpu) 2>&1 | grep -E "(error|warning)"

# 4. 运行测试
./tests/unit_tests
./tests/integration_tests

# 5. 格式化检查
clang-format -i src/*.cpp include/rollingraft/*.h
```

### 10.2 检查清单

**代码规范：**
- [ ] 新增文件有正确的 `@file` 头注释
- [ ] 所有公共API有Doxygen注释（`/** */`）
- [ ] 所有中文注释已翻译为英文
- [ ] 行长度不超过80字符
- [ ] 包含必要的TODO/FIXME标记

**质量检查：**
- [ ] 编译零警告（`-Wall -Wextra -Wpedantic`）
- [ ] 单元测试通过
- [ ] 代码通过 `clang-format` 格式化
- [ ] 无临时调试代码（printf/cout）

**Git规范：**
- [ ] 提交信息符合 Conventional Commits
- [ ] 相关文件已 `git add`
- [ ] 无不应提交的文件（如 .patched, build/）

---

## 11. 变更类型处理指南

### 11.1 代码变更场景

| 变更类型 | 注释要求 | 示例 |
|----------|----------|------|
| **新增公共API** | 必须完整Doxygen注释 | 新类、新方法、新配置项 |
| **修改公共API** | 更新注释，说明变更 | 参数变更、行为变更 |
| **删除公共API** | 标记 `@deprecated` | 废弃警告，建议替代方案 |
| **内部重构** | 复杂逻辑需注释 | 提取函数、算法优化 |
| **Bug修复** | 说明根因和修复思路 | 边界条件、并发问题 |
| **性能优化** | 说明优化策略和效果 | 缓存、批量、异步 |

### 11.2 提交信息对应注释更新

```bash
# feat: 新增功能 → 必须添加完整注释
git commit -m "feat: add PreVote mechanism

* Add PreVote RPC to reduce disruptive candidates
* Document PreVoteRequest/Response structures
* TODO: Add configuration option to enable/disable"

# fix: Bug修复 → 添加/更新相关注释
git commit -m "fix: resolve race condition in ApplyLogs

* Root cause: unlocked access to applied_index_
* Solution: use std::atomic for thread safety
* Add NOTE comment explaining atomic memory order"

# refactor: 重构 → 复杂逻辑需注释
git commit -m "refactor: extract log replication logic

* Extract AppendEntries logic to LogReplicator class
* Add class-level documentation for responsibilities
* Update inline comments for consistency check"

# style: 格式调整 → 通常无需更新注释
git commit -m "style: format code to 80-char limit

* Wrap long lines in raft_node.cpp
* No functional changes"

# docs: 文档更新 → 仅注释变更
git commit -m "docs: update API documentation for ReadIndex

* Clarify linearizability guarantee
* Add usage example in comment"

# test: 测试代码 → 测试意图需注释
git commit -m "test: add unit tests for membership change

* Test single node addition
* Test concurrent membership changes
* Add comments explaining test scenarios"
```

### 11.3 代码审查清单

**审查者检查：**
- [ ] 新代码是否有充分的注释说明"为什么"
- [ ] 公共API文档是否清晰完整
- [ ] TODO/FIXME是否有跟踪计划
- [ ] 复杂算法是否有复杂度说明
- [ ] 边界条件是否有注释说明

---

## 12. 评审清单

- [ ] 规范是否过于严格或宽松？
- [ ] Doxygen 风格是否合适？
- [ ] 是否需要强制所有函数注释？
- [ ] 迁移计划是否可行？
- [ ] 是否有遗漏的场景？
