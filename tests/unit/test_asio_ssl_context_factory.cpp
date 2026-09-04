#include <cstdio>
#include <memory>

#include "rollingraft/asio_ssl_context_factory.h"

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

}  // namespace rollingraft
