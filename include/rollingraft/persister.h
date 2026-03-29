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
  // @param data_dir: 数据目录路径
  // @return: 成功返回 OK，失败返回 IOError
  virtual Status Open(const std::string& data_dir) = 0;

  // 关闭持久化存储
  virtual void Close() = 0;

  // ==================== 元数据操作 ====================

  // 保存持久化状态（必须同步写入）
  // 在以下场景调用：
  // - 成为 Candidate 时（term++, voted_for=self）
  // - 收到更高 term 的 RPC 时（转为 Follower）
  // - 投票给其他候选人时
  virtual Status SaveState(const PersistentState& state) = 0;

  // 加载持久化状态
  // 如果状态不存在，返回 OK 且 state 为默认值
  virtual Status LoadState(PersistentState& state) = 0;

  // ==================== 日志操作 ====================

  // 追加日志条目（批量）
  // 在 Leader 接收客户端命令时调用
  // 在 Follower 接收 AppendEntries 时调用
  virtual Status AppendEntries(const std::vector<RaftLogEntry>& entries) = 0;

  // 获取指定范围的日志条目 [start, end)
  // 用于 Leader 向 Follower 发送 AppendEntries
  virtual Status GetEntries(uint64_t start, uint64_t end,
                            std::vector<RaftLogEntry>* out) = 0;

  // 获取单个日志条目
  virtual Status GetEntry(uint64_t index, RaftLogEntry& entry) = 0;

  // 截断日志（删除 index 及之后的所有条目）
  // 在日志冲突时调用（Follower 的日志比 Leader 新）
  virtual Status TruncateSuffix(uint64_t from_index) = 0;

  // 删除前缀日志（用于快照后清理旧日志）
  // 删除 [1, before_index) 的所有日志
  virtual Status TruncatePrefix(uint64_t before_index) = 0;

  // 获取最后一条日志的信息
  virtual std::pair<uint64_t, uint64_t> GetLastLogInfo() = 0;

  // ==================== 快照操作（可选） ====================

  // 保存快照元数据
  // @param snapshot_data: 状态机序列化后的数据
  // @param last_index: 快照包含的最后日志索引
  // @param last_term: 快照包含的最后日志任期
  virtual Status SaveSnapshot(const std::string& snapshot_data,
                              uint64_t last_index, uint64_t last_term) {
    (void)snapshot_data;
    (void)last_index;
    (void)last_term;
    return Status::OK();  // 默认空实现
  }

  // 加载快照
  virtual Status LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                              uint64_t& last_term) {
    (void)snapshot_data;
    (void)last_index;
    (void)last_term;
    return Status::OK();  // 默认空实现
  }

  // 获取快照信息
  virtual bool HasSnapshot() const { return false; }
};

// 工厂函数类型
using PersisterFactory = std::function<std::unique_ptr<Persister>()>;

}  // namespace rollingraft
