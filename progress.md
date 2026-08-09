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
