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
  virtual Status Write(const std::string& data) = 0;
  virtual Status Read(std::string& data) = 0;
};
} // namespace rollingraft