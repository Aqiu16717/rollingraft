#include "client_authorization.h"

namespace rollingraft {

ClientAuthorizationPolicy::ClientAuthorizationPolicy(
    const std::vector<ClientAuthorizationRule>& rules) {
  for (const auto& rule : rules) {
    rules_.emplace(rule.identity, rule.permission);
  }
}

Status ClientAuthorizationPolicy::AuthorizeClientRequest(const RpcRequestContext& context,
                                                         bool read_only) const {
  if (context.peer.kind != RpcPeerKind::CLIENT) {
    return Status::Unauthenticated("A client certificate is required for client requests");
  }
  const auto it = rules_.find(context.peer.client_identity);
  if (it == rules_.end()) {
    return Status::PermissionDenied("Client identity is not authorized");
  }
  if (!read_only && it->second != ClientPermission::READ_WRITE) {
    return Status::PermissionDenied("Client identity is not authorized to write");
  }
  return Status::OK();
}

bool IsRaftProtocolRequest(RaftMessageType type) {
  switch (type) {
    case RaftMessageType::KRequestVoteRequest:
    case RaftMessageType::KAppendEntriesRequest:
    case RaftMessageType::KInstallSnapshotRequest:
    case RaftMessageType::KPreVoteRequest:
    case RaftMessageType::KReadIndexRequest:
      return true;
    default:
      return false;
  }
}

}  // namespace rollingraft
