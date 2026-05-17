# Task #19: Agent-Friendly Control Plane API — Spec

## 1. 目标

将 RollingRaft 从"库"升级为"agent 可编程的分布式系统组件"。外部 agent（人类或 AI）可通过标准 HTTP/REST 接口：
- 观测集群状态
- 执行运维操作（成员变更、leader transfer、snapshot）
- 订阅实时事件（leadership change、membership change）

## 2. 设计原则

| 原则 | 说明 |
|------|------|
| **JSON 统一** | 所有请求/响应使用 JSON，error 也返回 JSON `{ "error": "...", "code": "..." }` |
| **幂等设计** | POST 操作支持 idempotency key，防止重放 |
| **事件驱动** | SSE (Server-Sent Events) 用于实时通知，agent 可流式消费 |
| **零侵入** | 控制平面作为独立组件，不修改 RaftNode 核心逻辑 |
| **可扩展** | 预留 `/v1/` 版本前缀，未来可升级 |

## 3. API 端点

### 3.1 状态查询

```
GET /v1/status
```

Response 200:
```json
{
  "node_id": 1,
  "role": "Leader",
  "term": 42,
  "leader_id": 1,
  "leader_addr": "127.0.0.1:8001",
  "commit_index": 1523,
  "last_applied": 1523,
  "last_log_index": 1523,
  "cluster_config": {
    "version": 3,
    "nodes": [
      { "id": 1, "addr": "127.0.0.1:8001" },
      { "id": 2, "addr": "127.0.0.1:8002" },
      { "id": 3, "addr": "127.0.0.1:8003" }
    ]
  },
  "uptime_ms": 3600000
}
```

### 3.2 成员管理

```
POST /v1/members
```

Request:
```json
{
  "node_id": 4,
  "addr": "127.0.0.1:8004"
}
```

Response 202 (Accepted — async):
```json
{
  "request_id": "uuid",
  "status": "pending",
  "message": "Configuration change proposed"
}
```

```
DELETE /v1/members/:node_id
```

Response 202:
```json
{
  "request_id": "uuid",
  "status": "pending",
  "message": "Configuration change proposed"
}
```

### 3.3 Leader Transfer

```
POST /v1/transfer-leadership
```

Request (optional — 不指定则让 Raft 自动选择):
```json
{
  "target_node_id": 2
}
```

Response 202:
```json
{
  "request_id": "uuid",
  "status": "pending"
}
```

### 3.4 Snapshot 触发

```
POST /v1/snapshot
```

Response 202:
```json
{
  "request_id": "uuid",
  "status": "pending"
}
```

### 3.5 事件订阅 (SSE)

```
GET /v1/events
```

Response: `Content-Type: text/event-stream`

```
event: leadership_change
data: {"term": 43, "leader_id": 2, "leader_addr": "127.0.0.1:8002", "timestamp": "2026-05-17T16:00:00Z"}

event: membership_change
data: {"version": 4, "action": "add", "node_id": 4, "timestamp": "2026-05-17T16:00:00Z"}

event: role_change
data: {"node_id": 1, "old_role": "Leader", "new_role": "Follower", "term": 43, "timestamp": "2026-05-17T16:00:00Z"}

event: snapshot_complete
data: {"index": 1500, "size_bytes": 1048576, "timestamp": "2026-05-17T16:00:00Z"}
```

### 3.6 Health Probes

```
GET /healthz   → 200/503，进程存活
GET /readyz    → 200/503，可接受请求（leader 或有 leader）
GET /livez     → 200/503，Raft 协议正常推进
```

## 4. 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                    ControlPlaneHttpServer                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │ /v1/status   │  │ /v1/members  │  │ /v1/events (SSE) │   │
│  │ /healthz     │  │ /transfer    │  │                  │   │
│  │ /readyz      │  │ /snapshot    │  │                  │   │
│  └──────────────┘  └──────────────┘  └──────────────────┘   │
│                        │                                      │
│                   EventBroadcaster                            │
│                        │                                      │
└────────────────────────┼──────────────────────────────────────┘
                         │
              ┌──────────▼──────────┐
              │     RaftNode        │
              │  (existing core)    │
              └─────────────────────┘
```

### 4.1 新增组件

#### `ControlPlaneHttpServer`
- 继承/复用现有 `MetricsHttpServer` 的 Asio HTTP 基础
- 添加路由分发（method + path）
- 解析 JSON request，调用 RaftNode API
- 序列化 JSON response

#### `EventBroadcaster`
- 维护 SSE client 列表（`std::vector<std::weak_ptr<SseClient>>`）
- 注册到 RaftNode 的 callbacks：
  - `SetRoleChangeCallback` → broadcast `role_change`
  - `SetLeaderChangeCallback` → broadcast `leadership_change`
  - 新增 `SetMembershipChangeCallback` → broadcast `membership_change`
  - 新增 `SetSnapshotCallback` → broadcast `snapshot_complete`

### 4.2 RaftNode 扩展（最小侵入）

在 `RaftNode` 中新增 callback setter（不破坏现有 API）：

```cpp
void SetMembershipChangeCallback(
    std::function<void(const ClusterConfig& old, const ClusterConfig& new_)> callback);

void SetSnapshotCallback(
    std::function<void(Index index, uint64_t size_bytes)> callback);
```

## 5. 错误处理

统一错误格式：
```json
{
  "error": "Not leader",
  "code": "NOT_LEADER",
  "leader_id": 2,
  "leader_addr": "127.0.0.1:8002"
}
```

HTTP 状态码映射：
| 场景 | 状态码 |
|------|--------|
| 成功 | 200/202 |
| 非 leader | 503 + leader hint |
| 参数错误 | 400 |
| 操作冲突 | 409 |
| 内部错误 | 500 |

## 6. 配置扩展

```cpp
struct RaftNodeConfig {
  // ... existing fields ...
  
  // Control plane
  bool control_plane_enabled = false;
  std::string control_plane_addr;  // e.g., "0.0.0.0:9001"
  
  // CORS (for web-based agent UI)
  std::vector<std::string> control_plane_cors_origins;
};
```

## 7. 实现建议

1. **Phase 1**: 基础 REST API (`/status`, `/members`, `/healthz`)
2. **Phase 2**: SSE 事件流 (`/events`)
3. **Phase 3**: 高级操作 (`/transfer-leadership`, `/snapshot`)

## 8. 与现有 MetricsHttpServer 的关系

建议合并：将 `MetricsHttpServer` 升级为 `ControlPlaneHttpServer`，保留 `GET /metrics`，新增 `/v1/*` 和 `/healthz` 等端点。或者保持独立，控制平面监听不同端口。

推荐：**合并**，避免多端口管理复杂度。控制平面地址配置复用 `metrics_addr` 或新增 `control_plane_addr`。

---

*Spec Version: 1.0*
*Author: Jack*
*Date: 2026-05-17*
