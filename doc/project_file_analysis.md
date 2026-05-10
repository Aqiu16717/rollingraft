# 项目文件分析报告

## 概述

本文档分析 RollingRaft 项目中存在的问题文件、冗余文件以及需要清理的内容。

## 1. 冗余/重复文件

### 1.1 设计文档重复

| 文件 | 状态 | 说明 |
|------|------|------|
| `DESIGN.md` | 可删除 | 早期设计，已被后续版本取代 |
| `DESIGNv2.md` | 可删除 | 过渡版本，内容已合并到 v6 |
| `DESIGNv3.md` | 可删除 | 过渡版本，内容已合并到 v6 |
| `DESIGNv4.md` | 可删除 | 过渡版本，内容已合并到 v6 |
| `DESIGNv5.md` | 可删除 | 过渡版本，内容已合并到 v6 |
| `DESIGNv6.md` | 保留 | 最新设计文档 |

**建议**: 只保留 `DESIGN.md`（作为最新版本）或 `DESIGNv6.md`，删除其他过渡版本。

### 1.2 doc/ 和 docs/ 目录重复

| 文件 | 状态 | 说明 |
|------|------|------|
| `docs/ARCHITECTURE_REFACTOR.md` | 评估后删除 | 重构计划，可能已完成 |
| `docs/PLUGGABLE_ARCHITECTURE.md` | 评估后删除 | 架构设计，已实现在代码中 |
| `docs/REFACTOR_BASED_ON_EXISTING.md` | 评估后删除 | 重构说明，已完成 |

**建议**: 统一使用 `doc/` 目录（单数形式更符合惯例），将 `docs/` 内容合并或删除。

## 2. 未实现/空实现文件

### 2.1 空文件

| 文件 | 状态 | 说明 |
|------|------|------|
| `tests/CMakeLists.txt` | 需实现 | 文件存在但内容为空，导致 BUILD_TESTING=ON 时可能出错 |

## 3. 临时/个人文件

| 文件 | 状态 | 说明 |
|------|------|------|
| `prompt.md` | 删除 | 个人提示文件 |
| `test.md` | 删除 | 临时测试笔记 |
| `CLAUDE.md` | 评估 | AI 助手相关说明，可合并到 AGENTS.md |

## 4. CMakeLists.txt 问题分析

### 4.1 当前存在的问题

#### 问题 1: file(GLOB_RECURSE) 的风险

```cmake
file(GLOB_RECURSE ROLLINGRAFT_SOURCES
  CONFIGURE_DEPENDS
  "src/*.cpp"
)
```

**风险**:
- `CONFIGURE_DEPENDS` 并非所有生成器都支持
- 新增/删除文件时，某些 IDE 可能不会自动重新运行 CMake
-  CMake 官方不推荐用于源文件收集

**建议**: 改为显式列出源文件，或使用 `target_sources` 在子目录中逐层添加。

#### 问题 2: tests/CMakeLists.txt 为空

```cmake
if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(tests)  # tests/CMakeLists.txt 是空的！
endif()
```

**影响**: 构建时可能出错或没有实际测试被编译。

**建议**: 实现 tests/CMakeLists.txt 的内容，或设置 `BUILD_TESTING=OFF` 默认禁用。

#### 问题 3: 安装导出不完整

```cmake
install(TARGETS rollingraft
  EXPORT rollingraft-targets  # 定义了导出，但未安装
  ...
)

# 缺少:
# install(EXPORT rollingraft-targets ...)
```

**影响**: 用户无法通过 `find_package(rollingraft)` 使用已安装的库。

**建议**: 添加完整的导出安装，或移除 EXPORT 关键字（如果不支持 find_package）。

#### 问题 4: spdlog 应为 PUBLIC

```cmake
target_link_libraries(rollingraft
  PUBLIC
    rollingraft::asio
    rollingraft::json
  PRIVATE
    rollingraft::spdlog  # 如果头文件包含 spdlog，应为 PUBLIC
    leveldb
)
```

**问题**: 如果 `rollingraft` 的头文件（如 logger.h）包含了 spdlog 头文件，那么依赖项目也需要 spdlog 的路径。

**建议**: 检查 `include/rollingraft/logger.h` 是否包含 spdlog，如果是则改为 PUBLIC。

#### 问题 5: 缺少版本文件

**建议**: 如需支持 `find_package(rollingraft 0.1.0)`，需要添加版本文件生成。

### 4.2 建议的 CMakeLists.txt 改进

```cmake
# 选项 1: 显式列出源文件（推荐）
target_sources(rollingraft
  PRIVATE
    src/asio_network_transport.cpp
    src/asio_timer_service.cpp
    src/json_protocol.cpp
    src/leveldb_persister.cpp
    src/logger.cpp
    src/logger_spdlog_adapter.cpp
    src/raft_log.cpp
    src/raft_node.cpp
    src/status.cpp
)

# 选项 2: 禁用测试默认
option(BUILD_TESTING "Build tests" OFF)  # 默认 OFF，直到 tests/ 完善

# 选项 3: 完整的安装导出（如需支持 find_package）
install(EXPORT rollingraft-targets
  FILE rollingraft-targets.cmake
  NAMESPACE rollingraft::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/rollingraft
)
```

## 5. 文档目录结构建议

### 当前结构
```
rollingraft/
├── DESIGN.md ~ DESIGNv6.md  (6个版本)
├── AGENTS.md
├── CLAUDE.md
├── USER_GUIDE.md
├── README.md
├── prompt.md
├── test.md
├── doc/
│   ├── CMakeLists_DESIGN.md
│   ├── network_transport_design.md
│   ├── PERSISTER_DESIGN.md
│   ├── protocol_design.md
│   ├── raft_log_design.md
│   ├── raft_node_design.md
│   ├── server_design.md
│   └── timer_service_design.md
└── docs/
    ├── ARCHITECTURE_REFACTOR.md
    ├── PLUGGABLE_ARCHITECTURE.md
    └── REFACTOR_BASED_ON_EXISTING.md
```

### 建议结构
```
rollingraft/
├── README.md
├── AGENTS.md              # 保留：开发指南
├── DESIGN.md              # 保留：最新设计（原 DESIGNv6.md）
├── USER_GUIDE.md          # 保留：用户指南
└── doc/
    ├── architecture/      # 架构文档
    │   ├── overview.md    # 原 DESIGNv6.md
    │   ├── network.md     # 原 network_transport_design.md
    │   ├── persister.md   # 原 PERSISTER_DESIGN.md
    │   ├── protocol.md    # 原 protocol_design.md
    │   ├── raft_log.md    # 原 raft_log_design.md
    │   ├── raft_node.md   # 原 raft_node_design.md
    │   └── timer.md       # 原 timer_service_design.md
    ├── cmake.md           # 原 CMakeLists_DESIGN.md
    └── development/       # 开发相关
        └── code_style.md  # 可添加代码风格指南
```

## 6. 清理清单

### 可立即删除的文件
```bash
# 过渡版本设计文档
rm DESIGNv2.md DESIGNv3.md DESIGNv4.md DESIGNv5.md

# 临时文件
rm prompt.md test.md

# 重复目录
rm -rf docs/
```

### 需要修复的文件
1. `tests/CMakeLists.txt` - 添加内容或默认禁用 BUILD_TESTING
2. `CMakeLists.txt` - 修复上述问题

## 7. 依赖分析

### 文件依赖关系

```
raft_node.cpp
├── json_protocol.h (src/)
├── asio_timer_service.h (src/)
└── asio_network_transport.cpp (已实现)

asio_network_transport.cpp
├── 独立实现，不依赖旧 Server

leveldb_persister.cpp
├── 已通过 GLOB 自动包含
```

## 8. 建议行动

### 优先级 1：CMake 修复
1. 修复 `tests/CMakeLists.txt`（添加内容或默认禁用测试）
2. 检查 spdlog 是否应该为 PUBLIC
3. 决定是否需要完整的 find_package 支持

### 优先级 2：代码清理
1. 删除过渡版本设计文档
2. 删除临时文件
3. 统一文档目录结构

### 优先级 3：可选改进
1. 考虑从 GLOB 改为显式源文件列表
2. 添加测试实现
3. 完善安装导出配置
