#pragma once

#include <functional>
#include <rollingraft/raft_log.h>
#include <rollingraft/status.h>
#include <rollingraft/types.h>

namespace rollingraft {

// 持久化元数据（必须在每次选举前持久化）
struct PersistentState {
  Term current_term = 0;  // 当前任期
  NodeId voted_for = -1;  // 投票给的候选人ID（-1表示未投票）
};

// 持久化接口
class Persister {
 public:
  virtual ~Persister() = default;

  // ==================== 生命周期 ====================

  // 打开/创建持久化存储
  virtual Status Open(const std::string\& data_dir) = 0;

  // 关闭持久化存储
  virtual void Close() = 0;

  // ==================== 元数据操作 ====================

  // 保存持久化状态
  virtual Status SaveState(const PersistentState\& state) = 0;

  // 加载持久化状态
  virtual Status LoadState(PersistentState\& state) = 0;

  // ==================== 日志操作 ====================

  // 追加日志条目（批量）
  virtual Status AppendEntries(const std::vector<RaftLogEntry>\& entries) = 0;

  // 获取指定范围的日志条目 [start, end)
  virtual Status GetEntries(uint64_t start, uint64_t end,
                            std::vector<RaftLogEntry>* out) = 0;

  // 获取单个日志条目
  virtual Status GetEntry(uint64_t index, RaftLogEntry\& entry) = 0;

  // 截断后缀日志
  virtual Status TruncateSuffix(uint64_t from_index) = 0;

  // 截断前缀日志
  virtual Status TruncatePrefix(uint64_t before_index) = 0;

  // 获取最后一条日志的信息
  virtual std::pair<uint64_t, uint64_t> GetLastLogInfo() = 0;
  virtual Status Write(const std::string& data) = 0;
  virtual Status Read(std::string& data) = 0;
};
} // namespace rollingraft