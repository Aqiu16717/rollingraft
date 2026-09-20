#include <cstdio>
#include <memory>

#include "rollingraft/asio_ssl_context_factory.h"

#include "mock/mock_network.h"
#include "tls_identity.h"
#include <gtest/gtest.h>
#include <openssl/pem.h>

namespace rollingraft {

class AsioSslContextFactoryTest : public ::testing::Test {
 protected:
#ifdef NODE_TEST_CERTS_DIR
  std::string node_certs_dir_ = NODE_TEST_CERTS_DIR;
#else
  std::string node_certs_dir_ = "../generated-node-certs/";
#endif
#ifdef TEST_CERTS_DIR
  std::string certs_dir_ = TEST_CERTS_DIR;
#else
  std::string certs_dir_ = "../../tests/certs/";
#endif
};

namespace {

std::unique_ptr<X509, decltype(&X509_free)> LoadCertificate(const std::string& path) {
  std::unique_ptr<FILE, decltype(&std::fclose)> file(std::fopen(path.c_str(), "r"), std::fclose);
  if (!file) {
    return {nullptr, X509_free};
  }
  return {PEM_read_X509(file.get(), nullptr, nullptr, nullptr), X509_free};
}

}  // namespace

TEST_F(AsioSslContextFactoryTest, CreateServerContext_Success) {
  TlsConfig config;
  config.enabled = true;
  config.cert_file = certs_dir_ + "server.crt";
  config.key_file = certs_dir_ + "server.key";

  AsioSslContextFactory factory(config);
  asio::ssl::context ctx(asio::ssl::context::tls_server);
  auto status = factory.CreateServerContext(ctx);
  EXPECT_TRUE(status.ok()) << status.GetMessage();
}

TEST_F(AsioSslContextFactoryTest, CreateServerContext_MissingCert) {
  TlsConfig config;
  config.enabled = true;
  config.cert_file = "/nonexistent/cert.pem";
  config.key_file = certs_dir_ + "server.key";

  AsioSslContextFactory factory(config);
  asio::ssl::context ctx(asio::ssl::context::tls_server);
  auto status = factory.CreateServerContext(ctx);
  EXPECT_FALSE(status.ok());
}

TEST_F(AsioSslContextFactoryTest, CreateServerContext_MutualAuthRequiresCa) {
  TlsConfig config;
  config.enabled = true;
  config.cert_file = certs_dir_ + "server.crt";
  config.key_file = certs_dir_ + "server.key";
  config.mutual_auth = true;

  AsioSslContextFactory factory(config);
  asio::ssl::context ctx(asio::ssl::context::tls_server);
  auto status = factory.CreateServerContext(ctx);
  EXPECT_FALSE(status.ok());
}

TEST_F(AsioSslContextFactoryTest, CreateClientContext_Success) {
  TlsConfig config;
  config.enabled = true;
  config.ca_file = node_certs_dir_ + "node_ca.crt";

  AsioSslContextFactory factory(config);
  asio::ssl::context ctx(asio::ssl::context::tls_client);
  auto status = factory.CreateClientContext(ctx);
  EXPECT_TRUE(status.ok()) << status.GetMessage();
}

TEST_F(AsioSslContextFactoryTest, CreateClientContext_mTLS) {
  TlsConfig config;
  config.enabled = true;
  config.mutual_auth = true;
  config.cert_file = node_certs_dir_ + "node1.crt";
  config.key_file = node_certs_dir_ + "node1.key";
  config.ca_file = node_certs_dir_ + "node_ca.crt";
  config.node_id = 1;

  AsioSslContextFactory factory(config);
  asio::ssl::context ctx(asio::ssl::context::tls_client);
  auto status = factory.CreateClientContext(ctx);
  EXPECT_TRUE(status.ok()) << status.GetMessage();
}

TEST_F(AsioSslContextFactoryTest, ExtractCertificateNodeId_FromUriSan) {
  std::unique_ptr<FILE, decltype(&std::fclose)> file(
      std::fopen((node_certs_dir_ + "node2.crt").c_str(), "r"), std::fclose);
  ASSERT_NE(file, nullptr);
  std::unique_ptr<X509, decltype(&X509_free)> certificate(
      PEM_read_X509(file.get(), nullptr, nullptr, nullptr), X509_free);
  ASSERT_NE(certificate, nullptr);

  NodeId node_id = -1;
  auto status = ExtractCertificateNodeId(certificate.get(), node_id);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(node_id, 2);
}

TEST_F(AsioSslContextFactoryTest, ExtractCertificatePeerIdentity_ClientUriSan) {
  auto certificate = LoadCertificate(node_certs_dir_ + "writer.crt");
  ASSERT_NE(certificate, nullptr);

  RpcPeerIdentity identity;
  auto status = ExtractCertificatePeerIdentity(certificate.get(), identity);

  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(identity.kind, RpcPeerKind::CLIENT);
  EXPECT_EQ(identity.node_id, -1);
  EXPECT_EQ(identity.client_identity, "writer");
}

TEST_F(AsioSslContextFactoryTest, ExtractCertificatePeerIdentity_RejectsAmbiguousOrMalformedUriSan) {
  for (const char* certificate_name : {"mixed.crt", "duplicate_client.crt", "invalid_client.crt"}) {
    auto certificate = LoadCertificate(node_certs_dir_ + certificate_name);
    ASSERT_NE(certificate, nullptr) << certificate_name;

    RpcPeerIdentity identity;
    auto status = ExtractCertificatePeerIdentity(certificate.get(), identity);

    EXPECT_FALSE(status.ok()) << certificate_name;
    EXPECT_NE(status.ToString().find("TLS_IDENTITY_INVALID"), std::string::npos)
        << certificate_name;
  }
}

TEST_F(AsioSslContextFactoryTest, CreateServerContext_RejectsMismatchedNodeIdentity) {
  TlsConfig config;
  config.enabled = true;
  config.mutual_auth = true;
  config.cert_file = node_certs_dir_ + "node2.crt";
  config.key_file = node_certs_dir_ + "node2.key";
  config.ca_file = node_certs_dir_ + "node_ca.crt";
  config.node_id = 1;

  AsioSslContextFactory factory(config);
  asio::ssl::context ctx(asio::ssl::context::tls_server);
  auto status = factory.CreateServerContext(ctx);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("TLS_IDENTITY_MISMATCH"), std::string::npos);
}

TEST_F(AsioSslContextFactoryTest, CreateServerContext_ClientAuthRequiresClientCa) {
  TlsConfig config;
  config.enabled = true;
  config.mutual_auth = true;
  config.client_auth_enabled = true;
  config.cert_file = node_certs_dir_ + "node1.crt";
  config.key_file = node_certs_dir_ + "node1.key";
  config.ca_file = node_certs_dir_ + "node_ca.crt";
  config.node_id = 1;

  AsioSslContextFactory factory(config);
  asio::ssl::context ctx(asio::ssl::context::tls_server);
  auto status = factory.CreateServerContext(ctx);

  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("TLS_CLIENT_CA_REQUIRED"), std::string::npos);
}

TEST_F(AsioSslContextFactoryTest, CreateServerContext_ClientAuthLoadsClientCa) {
  TlsConfig config;
  config.enabled = true;
  config.mutual_auth = true;
  config.client_auth_enabled = true;
  config.cert_file = node_certs_dir_ + "node1.crt";
  config.key_file = node_certs_dir_ + "node1.key";
  config.ca_file = node_certs_dir_ + "node_ca.crt";
  config.client_ca_file = node_certs_dir_ + "client_ca.crt";
  config.node_id = 1;

  AsioSslContextFactory factory(config);
  asio::ssl::context ctx(asio::ssl::context::tls_server);
  EXPECT_TRUE(factory.CreateServerContext(ctx).ok());
}

TEST(NetworkTransportAuthTest, DefaultAuthenticatedEntryPointIsUnsupported) {
  MockNetworkTransport transport;

  EXPECT_FALSE(transport.SupportsAuthenticatedPeerIdentity());
  auto status = transport.InitializeAuthenticated("127.0.0.1:1", {});
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("UNSUPPORTED"), std::string::npos);
}

}  // namespace rollingraft
