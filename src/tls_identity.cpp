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

}  // namespace

Status ExtractCertificateNodeId(X509* certificate, NodeId& node_id) {
  if (certificate == nullptr) {
    return Status::Error("TLS_IDENTITY_MISSING", "Peer certificate is missing");
  }

  GeneralNamesPtr names(static_cast<GENERAL_NAMES*>(
                            X509_get_ext_d2i(certificate, NID_subject_alt_name, nullptr, nullptr)),
                        GENERAL_NAMES_free);
  if (!names) {
    return Status::Error("TLS_IDENTITY_MISSING", "Certificate has no subjectAltName extension");
  }

  bool found = false;
  NodeId parsed_id = -1;
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
    constexpr std::string_view prefix = kRaftNodeSanPrefix;
    if (!value.starts_with(prefix)) {
      continue;
    }

    std::string_view id_text = value.substr(prefix.size());
    int64_t candidate = -1;
    auto [end, error] = std::from_chars(id_text.data(), id_text.data() + id_text.size(), candidate);
    if (error != std::errc{} || end != id_text.data() + id_text.size() || candidate < 0 ||
        candidate > std::numeric_limits<NodeId>::max()) {
      return Status::Error("TLS_IDENTITY_INVALID",
                           "Invalid rollingraft-node URI SAN: " + std::string(value));
    }
    if (found && parsed_id != static_cast<NodeId>(candidate)) {
      return Status::Error("TLS_IDENTITY_INVALID",
                           "Certificate contains conflicting rollingraft-node identities");
    }
    found = true;
    parsed_id = static_cast<NodeId>(candidate);
  }

  if (!found) {
    return Status::Error("TLS_IDENTITY_MISSING", "Certificate has no rollingraft-node URI SAN");
  }
  node_id = parsed_id;
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

}  // namespace rollingraft
