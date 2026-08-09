# Task Plan: 清理正确性遗留（3 项）

## Goal

修复 review 记录的三项低成本正确性遗留，使持久化/协议/测试更健壮。

## Phases

### Phase 1: WAL 损坏恢复（review #7 遗留）
- [x] 现状确认：ScanSegment CRC 错误 → Open 整体失败
- [x] 设计：损坏记录 → 物理截断段到最后有效记录 + 告警（etcd 语义；raft 日志可从 leader 重建）；无 trailer 的崩溃半写场景也覆盖（end_offset 回退到文件物理大小）
- [x] 实现 + 测试：CorruptionDetection 改为"截断恢复 + 二次 Open 干净"；DetectsCorruptedData 改为"Open OK + 损坏点前条目保留"

### Phase 2: command_/checksum_ 字段协议修复（review #10 遗留）
- [x] SerializeEntries/DeserializeEntries 补 command_/checksum_
- [x] 向后兼容（deserialize 时 command/checksum 可选，旧格式可读）

### Phase 3: 成员变更跨重启集成测试（review #5 测试缺口）
- [x] 新增 MembershipChangeSurvivesRestart：AddNode(4) → 提交 → 全集群重启 → 验证节点 4 仍在配置（真实 LevelDB 持久化路径；发现测试套件默认无 persister——新测试显式启用）

### Phase 4: 验证 + 提交
- [x] Release 全量 365/365 通过
- [x] 提交 + push（待 push）

## Next Step
Phase 1: 检查 test_wal_persister.cpp 的损坏测试期望

## Decisions Made
| Date | Decision |
|------|----------|
| 2026-08-09 | 完成 flaky 任务（前一个 plan）后启动本任务 |
| 2026-08-09 | 三项都做，按 WAL → 协议 → 测试 顺序 |
