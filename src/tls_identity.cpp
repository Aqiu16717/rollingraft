#include "tls_identity.h"

#include <charconv>
#include <cstdio>
#include <limits>
#include <memory>
#include <string_view>

#include <openssl/pem.h>
#include <openssl/x509v3.h>

namespace rollingraft {

namespace {

using GeneralNamesPtr = std::unique_ptr<GENERAL_NAMES, decltype(&GENERAL_NAMES_free)>;
using FilePtr = std::unique_ptr<FILE, decltype(&std::fclose)>;
using X509Ptr = std::unique_ptr<X509, decltype(&X509_free)>;

bool IsAsciiAlphaNumeric(char value) {
  return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
         (value >= '0' && value <= '9');
}

}  // namespace

bool IsValidClientIdentity(std::string_view identity) {
  if (identity.empty() || identity.size() > 128 || !IsAsciiAlphaNumeric(identity.front())) {
    return false;
  }
  for (char value : identity) {
    if (IsAsciiAlphaNumeric(value) || value == '.' || value == '_' || value == '-') {
      continue;
    }
    return false;
  }
  return true;
}

Status ExtractCertificatePeerIdentity(X509* certificate, RpcPeerIdentity& identity) {
  if (certificate == nullptr) {
    return Status::Error("TLS_IDENTITY_MISSING", "Peer certificate is missing");
  }

  GeneralNamesPtr names(static_cast<GENERAL_NAMES*>(
                            X509_get_ext_d2i(certificate, NID_subject_alt_name, nullptr, nullptr)),
                        GENERAL_NAMES_free);
  if (!names) {
    return Status::Error("TLS_IDENTITY_MISSING", "Certificate has no subjectAltName extension");
  }

  RpcPeerIdentity parsed_identity;
  bool found = false;
  for (int i = 0; i < sk_GENERAL_NAME_num(names.get()); ++i) {
    const GENERAL_NAME* name = sk_GENERAL_NAME_value(names.get(), i);
    if (name == nullptr || name->type != GEN_URI) {
      continue;
    }
    const ASN1_IA5STRING* uri = name->d.uniformResourceIdentifier;
    const auto* data = ASN1_STRING_get0_data(uri);
    int length = ASN1_STRING_length(uri);
    if (data == nullptr || length <= 0) {
      continue;
    }
    std::string_view value(reinterpret_cast<const char*>(data), static_cast<size_t>(length));
    const bool is_node = value.starts_with(kRaftNodeSanPrefix);
    const bool is_client = value.starts_with(kRaftClientSanPrefix);
    if (!is_node && !is_client) {
      continue;
    }

    if (found) {
      return Status::Error("TLS_IDENTITY_INVALID",
                           "Certificate contains multiple RollingRaft identities");
    }

    if (is_node) {
      std::string_view id_text = value.substr(std::string_view(kRaftNodeSanPrefix).size());
      int64_t candidate = -1;
      auto [end, error] =
          std::from_chars(id_text.data(), id_text.data() + id_text.size(), candidate);
      if (error != std::errc{} || end != id_text.data() + id_text.size() || candidate < 0 ||
          candidate > std::numeric_limits<NodeId>::max()) {
        return Status::Error("TLS_IDENTITY_INVALID",
                             "Invalid rollingraft-node URI SAN: " + std::string(value));
      }
      parsed_identity.kind = RpcPeerKind::NODE;
      parsed_identity.node_id = static_cast<NodeId>(candidate);
    } else {
      std::string_view client_identity =
          value.substr(std::string_view(kRaftClientSanPrefix).size());
      if (!IsValidClientIdentity(client_identity)) {
        return Status::Error("TLS_IDENTITY_INVALID",
                             "Invalid rollingraft-client URI SAN: " + std::string(value));
      }
      parsed_identity.kind = RpcPeerKind::CLIENT;
      parsed_identity.client_identity = std::string(client_identity);
    }
    found = true;
  }

  if (!found) {
    return Status::Error("TLS_IDENTITY_MISSING", "Certificate has no RollingRaft URI SAN");
  }
  identity = std::move(parsed_identity);
  return Status::OK();
}

Status ExtractCertificateNodeId(X509* certificate, NodeId& node_id) {
  RpcPeerIdentity identity;
  auto status = ExtractCertificatePeerIdentity(certificate, identity);
  if (!status.ok()) {
    return status;
  }
  if (identity.kind != RpcPeerKind::NODE) {
    return Status::Error("TLS_IDENTITY_MISSING", "Certificate has no rollingraft-node URI SAN");
  }
  node_id = identity.node_id;
  return Status::OK();
}

Status ValidateCertificateNodeId(const std::string& cert_file, NodeId expected_node_id) {
  FilePtr file(std::fopen(cert_file.c_str(), "r"), std::fclose);
  if (!file) {
    return Status::Error("TLS_CERT_LOAD_FAILED", "Failed to open certificate: " + cert_file);
  }
  X509Ptr certificate(PEM_read_X509(file.get(), nullptr, nullptr, nullptr), X509_free);
  if (!certificate) {
    return Status::Error("TLS_CERT_LOAD_FAILED", "Failed to parse certificate: " + cert_file);
  }
  NodeId certificate_node_id = -1;
  auto status = ExtractCertificateNodeId(certificate.get(), certificate_node_id);
  if (!status.ok()) {
    return status;
  }
  if (certificate_node_id != expected_node_id) {
    return Status::Error("TLS_IDENTITY_MISMATCH", "Certificate identity " +
                                                      std::to_string(certificate_node_id) +
                                                      " does not match configured node_id " +
                                                      std::to_string(expected_node_id));
  }
  return Status::OK();
}

Status ValidateCertificateClientIdentity(const std::string& cert_file) {
  FilePtr file(std::fopen(cert_file.c_str(), "r"), std::fclose);
  if (!file) {
    return Status::Error("TLS_CERT_LOAD_FAILED", "Failed to open certificate: " + cert_file);
  }
  X509Ptr certificate(PEM_read_X509(file.get(), nullptr, nullptr, nullptr), X509_free);
  if (!certificate) {
    return Status::Error("TLS_CERT_LOAD_FAILED", "Failed to parse certificate: " + cert_file);
  }
  RpcPeerIdentity identity;
  auto status = ExtractCertificatePeerIdentity(certificate.get(), identity);
  if (!status.ok()) {
    return status;
  }
  if (identity.kind != RpcPeerKind::CLIENT) {
    return Status::Error("TLS_IDENTITY_MISMATCH",
                         "Certificate does not contain a rollingraft-client URI SAN");
  }
  return Status::OK();
}

}  // namespace rollingraft
