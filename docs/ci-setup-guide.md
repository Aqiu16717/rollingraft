# RollingRaft CI/CD 设置指南

**版本**: v1.0  
**维护者**: Tom (Code Audit)  
**更新日期**: 2025-06-10  

---

## 快速开始

### 1. 本地 Pre-commit Hook（推荐）

在每次 commit 前自动检查代码格式和基础问题：

```bash
# 安装 pre-commit 工具
pip install pre-commit

# 在仓库根目录安装 hook
pre-commit install

# （可选）首次运行检查全部文件
pre-commit run --all-files
```

**配置生效后**，每次 `git commit` 会自动运行：
- `clang-format` — 检查 C++ 代码格式
- `cppcheck --enable=error` — 快速扫描致命错误

如果检查失败，commit 会被拦截，修复后重试即可。

---

### 2. GitHub Actions CI（已启用）

当前 `.github/workflows/ci.yml` 包含以下 job：

| Job | 触发条件 | 说明 |
|-----|----------|------|
| `build-and-test` | push/PR | 多矩阵构建（GCC/Clang × Linux/macOS × Debug/Release）+ 全量测试 |
| `sanitizer` | push/PR | ASan / TSan / UBSan（仅 unit tests，避免 integration 超时） |
| `werror` | push/PR | `-Werror` 编译，确保零警告 |
| `format-check` | push/PR | `clang-format --dry-run --Werror` |
| `cppcheck` | push/PR | `cppcheck --check-level=exhaustive`，阻塞 error/performance |
| `docker-test` | push/PR | Docker 集成测试 |

**PR 合并策略**: 所有 job 必须通过方可合并。

---

### 3. 在 Fork 中启用 CI

Fork 本仓库后，GitHub Actions 默认自动启用。无需额外配置。

如需调整：
1. 进入仓库 **Settings → Actions → General**
2. 选择 **Allow all actions and reusable workflows**
3. （可选）在 **Settings → Branches** 中配置分支保护规则：
   - 勾选 **Require status checks to pass before merging**
   - 添加以下 required checks：
     - `build-and-test (ubuntu-22.04, gcc, Release)`
     - `build-and-test (ubuntu-22.04, clang, Release)`
     - `build-and-test (macos-latest, clang, Release)`
     - `sanitizer (tsan)`
     - `werror`
     - `format-check`
     - `cppcheck`

---

### 4. 本地运行全量检查

在提交 PR 前，建议在本地运行与 CI 一致的检查：

```bash
# 1. 格式化检查
find src tests benchmark example include -type f \
  \( -name '*.cpp' -o -name '*.h' -o -name '*.hpp' \) \
  -exec clang-format --dry-run --Werror --style=file {} +

# 2. 编译 + 测试
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc)
ctest --test-dir build --output-on-failure

# 3. cppcheck（完整）
cppcheck --enable=warning,performance,portability,information \
  --check-level=exhaustive --std=c++20 \
  --suppress=missingIncludeSystem --suppress=unusedFunction:tests/ \
  -I include src/ tests/

# 4. TSan 构建 + 测试（仅 unit tests）
cmake -B build_tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer"
cmake --build build_tsan -j$(nproc)
TSAN_OPTIONS="suppressions=tsan_suppressions.txt" \
  ctest --test-dir build_tsan -R "unit\." --output-on-failure
```

---

### 5. 故障排查

| 症状 | 原因 | 解决 |
|------|------|------|
| `clang-format` 未找到 | 未安装 | `brew install clang-format` (macOS) 或 `apt install clang-format` (Linux) |
| `cppcheck` 误报 `missingInclude` | third_party 头文件路径 | 已配置 `--suppress=missingIncludeSystem`，如仍报错请检查 `-I` 路径 |
| TSan 测试超时 | 单测在 TSan 下慢 5-10× | 正常，CI 已设置 timeout=600s |
| macOS 下 `std::span` 编译失败 | Apple Clang 版本过低 | 需要 Apple Clang 15+ 或 Xcode 15+ |
| Docker test 失败 | Docker 未运行 | 确保 Docker Desktop 已启动 |

---

### 6. 扩展 CI（高级）

如需添加新的静态分析工具：

1. 在 `.github/workflows/ci.yml` 中新增 job
2. 参考现有 `cppcheck` job 的格式
3. 确保 `--error-exitcode=1` 或等效机制以阻塞 merge
4. 在 `docs/code-quality-process.md` 中更新工具链说明

---

*Document by Tom, 2025-06-10*
