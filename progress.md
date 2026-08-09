# Progress Log — flaky 测试修复

## Session 2026-08-09
- 创建任务计划（9 个 flaky 测试）
- Phase 1：确认当前名单 = 6 个 chaos 必挂 + 3 个间歇性
- Phase 2：根因修复（TimerService::Now() 时钟注入）→ 6/6 chaos 通过
- Phase 3：SyncByBatchSizeThreshold 等待窗口 2s→10s；其余 2 个多轮验证稳定
- Phase 4：Release ×3 全绿（364/364，首次零失败）、TSan 全绿（首次）、已提交推送（36e54f3 + c151c20）
- **全部 9 个 flaky 处理完毕** ✅
