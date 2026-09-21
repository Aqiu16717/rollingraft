/**
 * @file rpc_peer_identity.h
 * @brief Authenticated identity attached to an inbound RPC connection.
 */

#pragma once

#include <string>

#include "rollingraft/types.h"

namespace rollingraft {

/** Type of principal authenticated by the transport. */
enum class RpcPeerKind { ANONYMOUS, NODE, CLIENT };

/** Identity authenticated from a peer TLS certificate. */
struct RpcPeerIdentity {
  RpcPeerKind kind = RpcPeerKind::ANONYMOUS;
  NodeId node_id = -1;
  std::string client_identity;
};

}  // namespace rollingraft
