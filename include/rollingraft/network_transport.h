#pragma once

#include <functional>
#include <rollingraft/status.h>
#include <rollingraft/types.h>

namespace rollingraft {

// RPC 响应回调
// - response_data: 响应数据（已序列化）
// - success: 是否成功
// - error_msg: 错误信息（如果失败）
using RpcResponseCallback =
    std::function<void(const std::string& response_data, bool success,
                       const std::string& error_msg)>;

// RPC 请求处理器
// - from: 请求来源节点 ID
// - request_data: 请求数据（已序列化）
// - response_data: 输出响应数据（需要序列化）
using RpcRequestHandler = std::function<void(
    NodeId from, const std::string& request_data, std::string& response_data)>;

// 连接状态回调
// - peer_id: 对端节点 ID
// - addr: 对端地址
// - connected: true=连接建立, false=连接断开
using ConnectionCallback =
    std::function<void(NodeId peer_id, const NodeAddr& addr, bool connected)>;

class NetworkTransport {
 public:
  virtual ~NetworkTransport() = default;

  // 初始化传输层
  // @param listen_addr: 监听地址 (e.g., "0.0.0.0:8001")
  // @param handler: 收到请求时的处理回调
  // @return Status::OK() 表示成功
  virtual Status Initialize(const NodeAddr& listen_addr,
                            RpcRequestHandler handler) = 0;

  // 设置连接状态回调（可选）
  virtual void SetConnectionCallback(ConnectionCallback callback) = 0;

  // 启动传输层，开始监听
  virtual Status Start() = 0;

  // 停止传输层，关闭所有连接
  virtual Status Stop() = 0;

  // 发送 RPC 请求（异步）
  // @param to: 目标节点 ID
  // @param addr: 目标地址
  // @param request_data: 请求数据（已序列化）
  // @param timeout: 超时时间
  // @param callback: 响应回调（在 IO 线程执行）
  virtual void SendRpc(NodeId to, const NodeAddr& addr,
                       const std::string& request_data,
                       std::chrono::milliseconds timeout,
                       RpcResponseCallback callback) = 0;
};

}  // namespace rollingraft
