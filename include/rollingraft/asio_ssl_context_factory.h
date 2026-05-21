#pragma once

#include <asio.hpp>
#include <asio/ssl.hpp>

#include "rollingraft/status.h"
#include "rollingraft/tls_config.h"

namespace rollingraft {

/**
 * @brief Factory for creating ASIO SSL contexts from TlsConfig.
 */
class AsioSslContextFactory {
 public:
  explicit AsioSslContextFactory(const TlsConfig& config);

  Status CreateServerContext(asio::ssl::context& out_ctx) const;
  Status CreateClientContext(asio::ssl::context& out_ctx) const;

 private:
  TlsConfig config_;

  Status LoadCertificateChain(asio::ssl::context& ctx) const;
  Status LoadPrivateKey(asio::ssl::context& ctx) const;
  Status LoadCaBundle(asio::ssl::context& ctx) const;
  void ConfigureVersion(asio::ssl::context& ctx) const;
};

}  // namespace rollingraft
