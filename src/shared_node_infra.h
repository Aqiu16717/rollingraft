#pragma once

#include <memory>

#include "rollingraft/metrics.h"
#include "rollingraft/network_transport.h"
#include "rollingraft/protocol.h"
#include "rollingraft/runtime_config.h"
#include "rollingraft/timer_service.h"

#include "metrics_http_server.h"

namespace rollingraft {

/**
 * @brief Node-level infrastructure shared by multiple RaftGroup instances.
 *
 * In single-group mode a RaftNode owns one SharedNodeInfra exclusively.
 * In multi-raft mode one SharedNodeInfra is injected into every RaftGroup
 * managed by a RaftStore.
 *
 * This struct is intentionally a plain aggregate of dependencies. Lifecycle
 * (Start/Stop) is managed by the owner (RaftNode or RaftStore).
 */
struct SharedNodeInfra {
  std::unique_ptr<NetworkTransport> network_;
  std::unique_ptr<TimerService> timer_;
  std::unique_ptr<Protocol> protocol_;
  std::unique_ptr<MetricsRegistry> metrics_;
  std::unique_ptr<MetricsHttpServer> metrics_server_;
  std::unique_ptr<RuntimeConfig> runtime_config_;
};

}  // namespace rollingraft
