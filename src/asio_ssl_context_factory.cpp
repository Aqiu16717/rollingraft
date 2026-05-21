#include "rollingraft/asio_ssl_context_factory.h"

#include "rollingraft/logger.h"

namespace rollingraft {

AsioSslContextFactory::AsioSslContextFactory(const TlsConfig& config)
    : config_(config) {}

Status AsioSslContextFactory::CreateServerContext(
    asio::ssl::context& out_ctx) const {
  out_ctx = asio::ssl::context(asio::ssl::context::tls_server);
  ConfigureVersion(out_ctx);

  auto status = LoadCertificateChain(out_ctx);
  if (!status.ok()) return status;

  status = LoadPrivateKey(out_ctx);
  if (!status.ok()) return status;

  if (config_.mutual_auth) {
    out_ctx.set_verify_mode(
        asio::ssl::verify_peer | asio::ssl::verify_fail_if_no_peer_cert);
    status = LoadCaBundle(out_ctx);
    if (!status.ok()) return status;
  } else {
    out_ctx.set_verify_mode(asio::ssl::verify_none);
  }

  return Status::OK();
}

Status AsioSslContextFactory::CreateClientContext(
    asio::ssl::context& out_ctx) const {
  out_ctx = asio::ssl::context(asio::ssl::context::tls_client);
  ConfigureVersion(out_ctx);

  if (config_.mutual_auth) {
    out_ctx.set_verify_mode(asio::ssl::verify_peer);
    auto status = LoadCertificateChain(out_ctx);
    if (!status.ok()) return status;
    status = LoadPrivateKey(out_ctx);
    if (!status.ok()) return status;
  }

  if (!config_.ca_file.empty()) {
    auto status = LoadCaBundle(out_ctx);
    if (!status.ok()) return status;
  } else if (config_.mutual_auth) {
    return Status::Error("TLS_CA_REQUIRED",
                         "CA file required for client verification");
  }

  return Status::OK();
}

Status AsioSslContextFactory::LoadCertificateChain(
    asio::ssl::context& ctx) const {
  try {
    ctx.use_certificate_chain_file(config_.cert_file);
  } catch (const std::exception& e) {
    return Status::Error("TLS_CERT_LOAD_FAILED",
                         "Failed to load certificate: " + std::string(e.what()));
  }
  return Status::OK();
}

Status AsioSslContextFactory::LoadPrivateKey(
    asio::ssl::context& ctx) const {
  try {
    ctx.use_private_key_file(config_.key_file, asio::ssl::context::pem);
  } catch (const std::exception& e) {
    return Status::Error("TLS_KEY_LOAD_FAILED",
                         "Failed to load private key: " + std::string(e.what()));
  }
  return Status::OK();
}

Status AsioSslContextFactory::LoadCaBundle(
    asio::ssl::context& ctx) const {
  try {
    ctx.load_verify_file(config_.ca_file);
  } catch (const std::exception& e) {
    return Status::Error("TLS_CA_LOAD_FAILED",
                         "Failed to load CA bundle: " + std::string(e.what()));
  }
  return Status::OK();
}

void AsioSslContextFactory::ConfigureVersion(
    asio::ssl::context& ctx) const {
  long opts = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;
  if (config_.min_version >= 1) {
    opts |= SSL_OP_NO_TLSv1;
  }
  if (config_.min_version >= 2) {
    opts |= SSL_OP_NO_TLSv1_1;
  }
  if (config_.min_version >= 3) {
    opts |= SSL_OP_NO_TLSv1_2;
  }
  SSL_CTX_set_options(ctx.native_handle(), opts);
}

}  // namespace rollingraft
