#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "rollingraft/network_transport.h"
#include "rollingraft/raft_node.h"
#include "rollingraft/rpc.h"

namespace rollingraft {

/** Immutable authorization policy for authenticated high-level clients. */
class ClientAuthorizationPolicy {
 public:
  explicit ClientAuthorizationPolicy(const std::vector<ClientAuthorizationRule>& rules);

  /** Authorize one client request; read_only distinguishes queries from writes. */
  Status AuthorizeClientRequest(const RpcRequestContext& context, bool read_only) const;

 private:
  std::unordered_map<std::string, ClientPermission> rules_;
};

/** Return whether a message type belongs to the node-to-node Raft protocol. */
bool IsRaftProtocolRequest(RaftMessageType type);

}  // namespace rollingraft
