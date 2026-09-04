#pragma once

#include <string>
#include <vector>

#include "rollingraft/types.h"

namespace rollingraft {

/**
 * @brief TLS / mTLS configuration for node-to-node and client communication.
 */
struct TlsConfig {
  bool enabled = false;

  // Server certificate (required when enabled)
  std::string cert_file;
  std::string key_file;

  // CA bundle for verifying peer certificates (required for mTLS)
  std::string ca_file;

  // Enable mutual TLS — require and verify client certificates
  bool mutual_auth = false;

  // Local Raft node identity. With mutual_auth enabled, the certificate must
  // contain URI SAN "rollingraft-node:<node_id>".
  NodeId node_id = -1;

  // Whitelist of exact URI SAN identities such as "rollingraft-node:2".
  // Empty = allow any valid rollingraft-node identity signed by the CA.
  std::vector<std::string> allowed_cns;

  // TLS handshake timeout
  uint32_t handshake_timeout_ms = 5000;

  // Minimum TLS version (default 1.2)
  // 0 = TLS 1.0, 1 = TLS 1.1, 2 = TLS 1.2, 3 = TLS 1.3
  int min_version = 2;

  bool IsValid() const {
    if (!enabled) {
      return true;
    }
    if (cert_file.empty() || key_file.empty()) {
      return false;
    }
    if (mutual_auth && ca_file.empty()) {
      return false;
    }
    return true;
  }
};

}  // namespace rollingraft
