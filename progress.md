# Progress Log

## Session 2026-08-09
- 创建任务计划（9 个 flaky 测试）
- Phase 1：确认当前名单 = 6 个 chaos 必挂 + 3 个间歇性
- Phase 2：根因修复（TimerService::Now() 时钟注入）→ 6/6 chaos 通过
- Phase 3：SyncByBatchSizeThreshold 等待窗口 2s→10s；其余 2 个多轮验证稳定
- Phase 4：Release ×3 全绿（364/364，首次零失败）、TSan 全绿（首次）、已提交推送（36e54f3 + c151c20）
- **全部 9 个 flaky 处理完毕** ✅

## Session 2026-08-09（延续）— 正确性遗留清理
- Phase 1：WAL 损坏恢复（截断到最后有效记录 + 无 trailer 崩溃半写覆盖；两个损坏测试改为断言截断恢复）
- Phase 2：JSON 协议补 command_/checksum_ round-trip（向后兼容）
- Phase 3：新增 MembershipChangeSurvivesRestart（真实 LevelDB 持久化路径；发现测试套件默认无 persister）
- Phase 4：全量 365/365 通过，提交推送（6f67085）
- **3 项正确性遗留全部完成** ✅

## Session 2026-08-10 — 锁内 I/O 改造（Phase 1+2）
- Phase 1：快照创建两阶段（ShouldTriggerSnapshotLocked/CreateAndPersistSnapshot/ApplySnapshotLocked/RunAutoSnapshotIfNeeded + snapshot_in_progress_ 防重入）
- Phase 2：快照接收三阶段（锁内捕获 → 锁外 Restore/Persist → 重锁应用 + 过期检查）
- Phase 3：截断保留现状（FlushSync 顺序性要求，评估后为设计决策）
- 验证：Release 365/365；TSan ×2（首轮 1 偶发网络层竞态已记录 findings.md，二次全绿）
- 提交：bed8ccb（perf）+ a02fcba（docs），已推送
- **锁内 I/O 改造 Phase 1+2 完成** ✅

## Session 2026-08-10（延续）— 锁内 I/O Phase 1b（B 项）
- 发送侧快照准备移出锁：SendInstallSnapshotToPeerLocked → 异步 PrepareSnapshotForPeer（锁外 CreateSnapshot + 重锁验证 + 发送）
- 防护：weak 守卫 + role/peer/next_index 四重检查
- 验证：Release 365/365
- 提交：890de54，已推送
- **锁内 I/O 清单全部完成（A/B/C 移出，D 设计决策保留）** ✅

## Session 2026-08-13 — Store 级 endpoints
- MetricsHttpServer admin handler 加 group_id 参数（POST body / DELETE query）
- RaftStore 创建 store 级 metrics server + RegisterStoreProviders（/v1/status 聚合 + admin group 路由）
- RaftStoreConfig.admin_token 透传；RaftNodeImpl.GetLeaderId()
- 新测试 StoreEndpointsWork（聚合 status + group 路由 snapshot trigger）
- Release 366/366（1 已知 flaky）；TSan 0 竞态（BothGroupsElectIndependentLeaders 并行偶发，单跑 3/3 过）
- 提交 787969e，已推送
- **Store 级 endpoints 完成** ✅

## Session 2026-08-14 — SSE 事件流
- 抽取 FormatSseEvent（legacy 广播 + store 订阅复用）
- RaftStore::CreateGroup 订阅 group EventBus → 共享 metrics server SSE 广播（带 group_id）
- **发现并修复真 bug**：sse_connections_ 存 weak_ptr → 连接在 headers 写完后被销毁，SSE 从未真正工作过。改 shared_ptr + RemoveDeadSseConnections 按 IsOpen 清理
- 测试：StoreEndpointsWork 扩展 3-store SSE + transfer 验证 group_id 事件
- Release 366/366；提交 a394c77（fix）+ d666312（feat），已推送
- **SSE 事件流完成** ✅

## Session 2026-08-15 — 性能热点
- WAL 读取 fd 缓存：ReadLogEntryAt/ScanSegment 不再每次 open/close（GC/Close 时失效）
- DeleteRange 评估后推迟：bundled LevelDB 版本不支持该 API（需依赖升级）
- Release 366/366；提交 3276997，已推送
