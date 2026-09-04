#pragma once

#include <string>

#include "rollingraft/status.h"
#include "rollingraft/types.h"

#include <openssl/x509.h>

namespace rollingraft {

inline constexpr char kRaftNodeSanPrefix[] = "rollingraft-node:";

/** Extract a NodeId from the certificate's rollingraft-node URI SAN. */
Status ExtractCertificateNodeId(X509* certificate, NodeId& node_id);

/** Load a PEM certificate and require its URI SAN to match expected_node_id. */
Status ValidateCertificateNodeId(const std::string& cert_file, NodeId expected_node_id);

}  // namespace rollingraft
