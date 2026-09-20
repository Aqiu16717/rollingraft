#pragma once

#include <string>
#include <string_view>

#include "rollingraft/rpc_peer_identity.h"
#include "rollingraft/status.h"
#include "rollingraft/types.h"

#include <openssl/x509.h>

namespace rollingraft {

inline constexpr char kRaftNodeSanPrefix[] = "rollingraft-node:";
inline constexpr char kRaftClientSanPrefix[] = "rollingraft-client:";

/** Return whether an exact client URI SAN suffix has the supported grammar. */
bool IsValidClientIdentity(std::string_view identity);

/** Extract the single recognized RollingRaft URI SAN from a certificate. */
Status ExtractCertificatePeerIdentity(X509* certificate, RpcPeerIdentity& identity);

/** Extract a NodeId from the certificate's rollingraft-node URI SAN. */
Status ExtractCertificateNodeId(X509* certificate, NodeId& node_id);

/** Load a PEM certificate and require its URI SAN to match expected_node_id. */
Status ValidateCertificateNodeId(const std::string& cert_file, NodeId expected_node_id);

/** Load a PEM certificate and require one valid rollingraft-client URI SAN. */
Status ValidateCertificateClientIdentity(const std::string& cert_file);

}  // namespace rollingraft
