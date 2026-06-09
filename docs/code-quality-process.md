# RollingRaft 代码质量监控流程

**版本**: v1.0  
**维护者**: Tom (Code Audit)  
**更新日期**: 2025-06-09  

---

## 目标

在不影响开发速度的前提下，通过自动化工具 + 定期人工审查，持续维持 RollingRaft 代码库的高质量，防止以下问题流入生产：

- 竞态条件 / 死锁
- 内存泄漏 / 资源泄漏
- 未定义行为（UB）
- 安全漏洞（注入、越界、信息泄露）
- 性能退化

---

## 1. 静态分析工具链

### 1.1 cppcheck（已启用）

**用途**: 快速扫描 C++ 代码的通用问题（未初始化变量、内存泄漏、竞态条件、死代码等）。

**当前配置**:
```bash
cppcheck --enable=all --std=c++20 --check-level=exhaustive \
  --suppress=missingIncludeSystem -I include src/ tests/
```

**推荐增强**:
```bash
# 增加 unusedFunction 检查（release build 更严格）
cppcheck --enable=all --std=c++20 --check-level=exhaustive \
  --suppress=missingIncludeSystem \
  --suppress=unusedFunction:tests/ \
  -I include src/ tests/
```

**CI 阈值**: 0 warnings（style 级别允许，但 security/performance/error 必须为 0）。

**已知限制**:
- 对 gtest 宏的语法误报（`test_chaos.cpp`, `test_asio_ssl_context_factory.cpp`）— 已通过编译验证排除
- `missingInclude` 对 third_party/ 的误报 — 使用 `--suppress=missingIncludeSystem`

---

### 1.2 clang-tidy（推荐新增）

**用途**: 基于 LLVM AST 的深度分析，检测 cppcheck 无法发现的 C++ 特有问题。

**推荐配置** (`.clang-tidy`):
```yaml
Checks: >
  bugprone-*,
  cppcoreguidelines-*,
  performance-*,
  portability-*,
  readability-*,
  clang-analyzer-*,
  modernize-*,
  -modernize-use-trailing-return-type,
  -readability-named-parameter,
  -cppcoreguidelines-avoid-magic-numbers,
  -cppcoreguidelines-pro-type-vararg,
  -llvm-header-guard

CheckOptions:
  - key:   readability-function-cognitive-complexity.Threshold
    value: '25'
  - key:   bugprone-argument-comment.StrictMode
    value: '1'
  - key:   cppcoreguidelines-special-member-functions.AllowSoleDefaultDtor
    value: '1'

HeaderFilterRegex: 'include/rollingraft/.*'
```

**重点检查项**（与 RollingRaft 高度相关）:

| Check | 说明 | 近期发现 |
|-------|------|----------|
| `bugprone-use-after-move` | move 后使用 | — |
| `bugprone-unused-return-value` | 忽略返回值 | `std::remove` (已修复) |
| `cppcoreguidelines-init-variables` | 变量未初始化 | `RaftLogEntry` (已修复) |
| `cppcoreguidelines-slicing` | 对象切片 | — |
| `performance-noexcept-move-constructor` | move 构造函数未标记 noexcept | — |
| `clang-analyzer-cplusplus.NewDelete` | new/delete 不匹配 | — |
| `clang-analyzer-unix.Malloc` | malloc/free 不匹配 | — |
| `clang-analyzer-deadcode.DeadStores` | 死存储 | `election_manager` req (已修复) |

**运行方式**:
```bash
# 生成 compile_commands.json (需要 bear 或 cmake)
cmake -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

# 运行 clang-tidy
clang-tidy -p build src/*.cpp tests/unit/*.cpp
```

**CI 阈值**: 0 `bugprone-*` / `clang-analyzer-*` / `performance-*` warnings。`readability-*` 和 `modernize-*` 允许存在（渐进式修复）。

---

### 1.3 clang-format（推荐新增）

**用途**: 统一代码格式，减少 review 中的 style noise。

**推荐配置** (`.clang-format`):
```yaml
BasedOnStyle: Google
Language: Cpp
Standard: c++20
ColumnLimit: 100
IndentWidth: 2
TabWidth: 2
UseTab: Never
AllowShortFunctionsOnASingleLine: Empty
BreakBeforeBraces: Attach
DerivePointerAlignment: false
PointerAlignment: Left
SortIncludes: CaseSensitive
IncludeBlocks: Preserve
```

**CI 集成**: PR gate 中运行 `clang-format --dry-run --Werror` 检查格式一致性。

---

## 2. CI 集成建议

### 2.1 Pre-commit Hook（推荐）

在 `.git/hooks/pre-commit` 或 `pre-commit` 框架中配置：

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: clang-format
        name: clang-format
        entry: clang-format --dry-run --Werror
        language: system
        files: '\.(cpp|h)$'
        pass_filenames: true

      - id: cppcheck
        name: cppcheck
        entry: cppcheck --error-exitcode=1 --enable=warning,performance,portability,information --std=c++20 -I include
        language: system
        files: '\.(cpp|h)$'
        pass_filenames: false
        args: ['src/', 'tests/']
```

**安装**:
```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files  # 首次运行检查全部文件
```

---

### 2.2 PR Gate（推荐）

在 GitHub Actions / GitLab CI 中配置以下 job：

```yaml
# .github/workflows/code-quality.yml
name: Code Quality

on: [pull_request]

jobs:
  static-analysis:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install tools
        run: |
          sudo apt-get update
          sudo apt-get install -y cppcheck clang-tidy clang-format

      - name: Generate compile_commands.json
        run: cmake -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

      - name: cppcheck
        run: |
          cppcheck --error-exitcode=1 --enable=all --std=c++20 \
            --suppress=missingIncludeSystem -I include src/ tests/

      - name: clang-tidy (critical checks only)
        run: |
          clang-tidy -p build \
            --checks='bugprone-*,clang-analyzer-*,performance-*' \
            src/*.cpp tests/unit/*.cpp

      - name: clang-format
        run: |
          find src include tests -name '*.cpp' -o -name '*.h' | \
            xargs clang-format --dry-run --Werror
```

**阻塞策略**: `static-analysis` job 失败时，PR 不可合并。

---

### 2.3 Nightly Build（推荐）

每晚运行更全面的检查（包括 `readability-*` / `modernize-*` / TSan）：

```yaml
# .github/workflows/nightly.yml
name: Nightly Quality Gate

on:
  schedule:
    - cron: '0 2 * * *'  # 每天凌晨 2 点

jobs:
  exhaustive-analysis:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Full clang-tidy
        run: |
          clang-tidy -p build --checks='*' src/*.cpp tests/unit/*.cpp 2>&1 | \
            tee clang-tidy-report.txt

      - name: TSan build + test
        run: |
          cmake -B build_tsan -DCMAKE_BUILD_TYPE=Debug \
            -DCMAKE_CXX_FLAGS="-fsanitize=thread"
          cmake --build build_tsan -j$(nproc)
          ./build_tsan/tests/unit_tests
```

---

## 3. 定期审查 Checklist

### 3.1 每周审查（15 分钟）

由值班人员执行：

- [ ] 查看本周 cppcheck / clang-tidy 报告，确认无新增 critical warning
- [ ] 查看 CI 失败日志，确认非 flaky test
- [ ] 检查本周新增代码的锁层次是否符合规范
- [ ] 检查本周新增代码的异常安全路径（RAII、cleanup）

### 3.2 每月深度审查（2-4 小时）

由 Tom（或轮值审计专家）执行：

- [ ] 使用 `--check-level=exhaustive` 运行 cppcheck 全仓库扫描
- [ ] 审查新增功能的竞态条件（重点关注锁的释放/重新获取模式）
- [ ] 审查新增功能的异常安全路径（析构时资源清理、临时文件管理）
- [ ] 审查新增 RPC / API 的输入验证（防止注入、越界）
- [ ] 运行 TSan build，确认 zero data races
- [ ] 更新本 checklist（如有新的 recurring issue 模式）

### 3.3 每季度架构审查（半天）

由 Cindy + Jack + 团队执行：

- [ ] 审查整体架构决策是否仍然有效（ADR 更新）
- [ ] 评估性能退化趋势（对比基准数据）
- [ ] 评估技术债务累积（TODO/FIXME 统计）
- [ ] 规划下一季度的质量改进目标

---

## 4. 工具版本锁定

为保证 CI 一致性，锁定以下工具版本：

| 工具 | 版本 | 说明 |
|------|------|------|
| cppcheck | 2.20.0+ | 当前使用版本 |
| clang-tidy | 17+ | Apple Clang 17 兼容 |
| clang-format | 17+ | 与 clang-tidy 同版本 |
| CMake | 3.20+ | 现有要求 |

**升级策略**: 每半年评估一次工具新版本，在独立分支验证无新增误报后统一升级。

---

## 5. 历史问题模式归档

| 问题模式 | 首次发现 | 修复 Commit | 预防措施 |
|----------|----------|-------------|----------|
| 默认构造函数未初始化 POD 成员 | `93c4c63` | `RaftLogEntry` / `RaftResponse` 添加默认初始化器 | `cppcoreguidelines-init-variables` |
| 析构函数调用虚函数 | `9f2793f` | 标记为已知模式（当前无派生类） | `clang-analyzer-cplusplus.VirtualCall` |
| 赋值未使用的临时对象 | `93c4c63` | 删除 `election_manager` 多余 req | `clang-analyzer-deadcode.DeadStores` |
| `std::remove` 返回值未检查 | `acb699f` | 添加返回值检查 + WARN 日志 | `bugprone-unused-return-value` |
| `uint32_t` 时间戳溢出 | `93c4c63` | `ttl_ms_` 改为 `uint64_t` | `bugprone-integer-overflow` |
| inflight_ / snapshot_sends_ 未在 step-down 时清理 | `4d44330` / `92d80a0` | 添加 `.clear()` | 人工审查（模式较复杂） |
| temp 文件异常路径泄漏 | `92d80a0` | 添加 try-catch + RAII | 人工审查 + `clang-analyzer-*` |

---

## 6. 快速参考

### 本地运行全量检查

```bash
# 1. 格式化检查
find src include tests -name '*.cpp' -o -name '*.h' | \
  xargs clang-format --dry-run --Werror

# 2. cppcheck
cppcheck --enable=all --std=c++20 --check-level=exhaustive \
  --suppress=missingIncludeSystem -I include src/ tests/

# 3. clang-tidy (critical only)
clang-tidy -p build --checks='bugprone-*,clang-analyzer-*,performance-*' \
  src/*.cpp tests/unit/*.cpp

# 4. 编译 + 测试
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc)
./build/tests/unit_tests
./build/tests/integration_tests
./build/tests/deterministic_tests
```

---

*Document by Tom, 2025-06-09*
