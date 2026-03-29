#pragma once

#include <functional>
#include <rollingraft/status.h>

namespace rollingraft {

// 定时器标识符
using TimerId = uint64_t;

// 无效定时器 ID
constexpr TimerId kInvalidTimerId = 0;

class TimerService {
 public:
  virtual ~TimerService() = default;

  // ========== 生命周期 ==========

  // 启动定时器服务
  // 必须在调用任何 Set* 方法前启动
  virtual void Start() = 0;

  // 停止定时器服务
  // 取消所有未触发的定时器，等待正在执行的回调完成
  virtual void Stop() = 0;

  // ========== 定时器操作 ==========

  // 设置一次性定时器
  // @param delay: 延迟时间
  // @param callback: 超时回调
  // @return: 定时器 ID，用于取消
  virtual TimerId SetTimeout(std::chrono::milliseconds delay,
                             std::function<void()> callback) = 0;

  // 设置周期性定时器
  // @param interval: 周期间隔
  // @param callback: 每次触发的回调
  // @return: 定时器 ID
  virtual TimerId SetInterval(std::chrono::milliseconds interval,
                              std::function<void()> callback) = 0;

  // 取消定时器
  // @param timer_id: 要取消的定时器 ID
  // @return: true 如果成功取消，false 如果已触发或不存在
  virtual bool CancelTimer(TimerId timer_id) = 0;

  // 检查定时器是否存在
  virtual bool IsTimerActive(TimerId timer_id) const = 0;

  // ========== 工厂方法 ==========

  // 创建默认实现（基于 Asio）
  static std::unique_ptr<TimerService> CreateDefault();
};

}  // namespace rollingraft
