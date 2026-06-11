# cppcheck Known Issues & Suppressions

**版本**: v1.0  
**维护者**: Tom (Code Audit)  
**更新日期**: 2026-06-10  
**工具版本**: cppcheck 2.20.0  

---

## 快速参考

CI 中已配置的 suppression（`.github/workflows/ci.yml`）：

```bash
cppcheck --error-exitcode=1 \
  --enable=warning,performance,portability,information \
  --check-level=exhaustive --std=c++20 \
  --suppress=missingIncludeSystem \
  --suppress=unusedFunction:tests/ \
  --suppress=operatorEqToSelf:include/rollingraft/status.h \
  --suppress=syntaxError:tests/deterministic/test_chaos.cpp \
  --suppress=syntaxError:tests/unit/test_asio_ssl_context_factory.cpp \
  -I include src/ tests/
```

当前状态：**零 issue**（全部通过 ✅）

---

## 已知误报清单

### 1. `operatorEqToSelf` — `include/rollingraft/status.h:200`

| 属性 | 值 |
|------|-----|
| **级别** | warning |
| **检查器** | `operatorEqToSelf` |
| **文件** | `include/rollingraft/status.h:200` |
| **类型** | 误报 |

**cppcheck 报告**：
```
warning: 'operator=' should check for assignment to self to avoid problems with dynamic memory.
```

**实际情况**：`Status::operator=` 已经通过 `state_ != rhs.state_` 在语义上等价地处理了自赋值。`Status` 使用内部 `new[]` 分配的 `state_` 指针，两个不同对象的 `state_` 指针不可能相等（除非均为 `nullptr`），因此 `state_ != rhs.state_` 已足够保护自赋值场景。显式 `if (this == &rhs)` 在此处是冗余的。

**处理**：CI suppression。非代码缺陷。

---

### 2. `syntaxError` — gtest 宏解析失败

| 属性 | 值 |
|------|-----|
| **级别** | error |
| **检查器** | `syntaxError` |
| **文件** | `tests/deterministic/test_chaos.cpp:43`  |
| **文件** | `tests/unit/test_asio_ssl_context_factory.cpp:16` |
| **类型** | 误报（工具限制） |

**cppcheck 报告**：
```
error: syntax error [syntaxError]
TEST(ChaosTest, DelayStorm) {
^
```

**实际情况**：cppcheck 的 C++ 预处理器无法完整展开 gtest 的 `TEST` / `TEST_F` 宏。这些宏内部使用了复杂的模板和重载，超出了 cppcheck 的解析能力。**代码本身完全正确**，可以通过编译并正常运行。

**影响范围**：所有使用 `TEST` / `TEST_F` / `TEST_P` / `INSTANTIATE_TEST_SUITE_P` 宏的测试文件都可能触发此误报。当前仓库中仅 2 个文件被 cppcheck 扫描到（其余文件在 cppcheck 的预处理阶段可能以不同方式处理）。

**处理**：CI suppression 按文件。未来如新增测试文件触发相同误报，按相同模式添加 suppression。

---

## 已修复的真问题（历史记录）

以下问题已在 `e0a40dd` 之后修复，保留记录供参考：

### `useInitializationList` — 构造函数体赋值

| 文件 | 修复方式 | 说明 |
|------|----------|------|
| `src/logger_spdlog_adapter.cpp:20` | 移入初始化列表 | `std::shared_ptr<spdlog::logger>` 可在初始化列表直接构造 |
| `tests/deterministic/test_cluster.cpp:13` | 移入初始化列表 | `std::unique_ptr<SimulatedClock>` 可在初始化列表直接构造 |
| `tests/deterministic/simulated_network_transport.cpp:10` | 移入初始化列表 | `std::make_shared<State>()` 可在初始化列表，后续成员赋值保留在构造函数体 |
| `tests/deterministic/simulated_timer_service.cpp:7` | 移入初始化列表 | 同上 |

### `passedByValue` — 大型对象按值传递

| 文件 | 修复方式 | 说明 |
|------|----------|------|
| `tests/deterministic/test_cluster.cpp:53` | `const std::vector<NodeId>&` | `Partition()` 参数避免不必要的拷贝 |

---

## 维护指南

### 新增 suppression 的流程

1. **确认是误报**：手动检查代码，确认 cppcheck 报告的问题在实际代码逻辑中不存在
2. **尝试修复**：如为真问题，优先修复代码而非 suppression
3. **更新三处**：
   - `.github/workflows/ci.yml` — cppcheck job 的 `--suppress` 参数
   - `docs/cppcheck-known-issues.md` — 本文档
   - （可选）`.pre-commit-config.yaml` — 如 pre-commit 也受影响

### 升级 cppcheck 版本时

- 新版本可能引入新的检查器或改变行为
- 升级后应全量运行 cppcheck，检查是否有新的误报需要 suppression
- 某些旧误报可能在升级后自动消失（如 gtest 宏解析改进）

### 全量扫描命令

```bash
cppcheck --enable=warning,performance,portability,information \
  --check-level=exhaustive --std=c++20 \
  --suppress=missingIncludeSystem --suppress=unusedFunction:tests/ \
  -I include src/ tests/
```

---

*Document by Tom, 2026-06-10*
