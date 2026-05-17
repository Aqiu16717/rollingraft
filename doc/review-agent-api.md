# Task #19 API Spec Review — Jack

## 总体评价

Spec 结构清晰，覆盖了 agent-friendly 的核心需求。与我之前提交的 `doc/agent-api.md` 基本一致，Cindy 的版本在错误码定义和 change_id 追踪上更细致。

## 回答 Open Questions

### 1. SSE vs WebSocket → **SSE**

理由：
- **单向推送足够**：agent 只需要接收事件，不需要通过同一通道发送命令
- **HTTP 兼容**：复用现有 MetricsHttpServer 的 ASIO 基础，零新增依赖
- **自动重连**：SSE 内置 `Last-Event-ID` 断线续传，agent 重启后可从上次位置恢复
- **防火墙友好**：标准 HTTP，不需要 WebSocket upgrade

WebSocket 保留到 v2，如果未来需要双向实时交互（如 agent 直接发送 propose 命令流）。

### 2. 认证 → **v1 不加，v2 再加**

理由：
- RollingRaft 当前定位为内网/本地部署组件，控制平面绑定 localhost 或私有网络
- 增加认证（API key / mTLS）会引入配置复杂度和密钥管理负担
- 如果部署在公网，应通过反向代理（nginx/traefik）处理 TLS 和认证，而不是在库内实现

**妥协方案**：v1 支持可选的 `control_plane_api_key` 配置字段，如果不设则不认证，如果设了则简单 Bearer token 校验。实现成本极低（一个 header check）。

### 3. Change ID 追踪 → **Events + 轮询 fallback**

建议机制：
- POST /v1/members 返回 `change_id: "cfg-{uuid}"`
- Agent 有两个选项追踪完成状态：
  a. **推荐**：订阅 SSE `/v1/events`，监听 `membership_change` 事件，匹配 `change_id`
  b. **Fallback**：GET /v1/status，对比 `cluster_config.version` 和 `cluster_config.nodes`

不需要专门的 `GET /v1/changes/{change_id}` 端点，避免过度设计。

### 4. Rate Limiting → **v1 不加**

Raft 本身的成员变更机制已经天然限制了并发（一次只能有一个未提交的成员变更）。HTTP 层的 rate limiting 属于运维层 concern，建议由反向代理处理。

## 与现有代码的兼容性

| Spec 要求 | 现有支持 | 需要新增 |
|-----------|----------|----------|
| GET /v1/status | 需暴露更多内部状态 | `RaftNodeImpl` 新增 `GetStatus()` |
| POST /v1/members | `RaftNode::AddNode()` 已存在 | HTTP handler 包装 |
| DELETE /v1/members | `RaftNode::RemoveNode()` 已存在 | HTTP handler 包装 |
| POST /v1/snapshot/trigger | 需新增 `TriggerSnapshot()` | `RaftNode` 新增 public API |
| POST /v1/leadership/transfer | 需新增 `TransferLeadership()` | `RaftNode` 新增 public API |
| GET /v1/events (SSE) | 需 EventBus + SSE connection mgr | 新增组件 |

**关键新增 public API 建议**：

```cpp
// In raft_node.h
Status TriggerSnapshot();
Status TransferLeadership(NodeId target_node_id = -1);  // -1 = auto select
```

## 实现优先级建议

1. **P0**（MVP）：GET /v1/status + /healthz + /readyz — Alice 正在做
2. **P1**：POST /v1/members + DELETE — 核心控制功能
3. **P1**：POST /v1/leadership/transfer — 运维必备
4. **P2**：POST /v1/snapshot/trigger — 高级功能
5. **P2**：GET /v1/events (SSE) — 依赖 EventBus 实现

## 与 Alice Task #21 的边界

- **Alice（#21）**：healthz/readyz/livez + JSON logs + Prometheus metrics
- **GeoHot（#19）**：/v1/status + /v1/members + /v1/leadership/transfer + /v1/snapshot/trigger + /v1/events

重叠点：`/v1/status` 和 health probes 都需要节点内部状态。建议：
- Alice 实现底层 `StatusProvider` callback（提供 JSON 字符串）
- GeoHot 在 #19 中复用该 callback 填充 `/v1/status` 的响应

## 结论

Spec 通过 ✅。建议按 P0→P1→P2 顺序实现，GeoHot 可以开始编码。
