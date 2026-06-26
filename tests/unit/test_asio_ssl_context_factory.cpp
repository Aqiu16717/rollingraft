#include "rollingraft/asio_ssl_context_factory.h"

#include <gtest/gtest.h>

namespace rollingraft {

class AsioSslContextFactoryTest : public ::testing::Test {
 protected:
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
  config.ca_file = certs_dir_ + "ca.crt";

  AsioSslContextFactory factory(config);
  asio::ssl::context ctx(asio::ssl::context::tls_client);
  auto status = factory.CreateClientContext(ctx);
  EXPECT_TRUE(status.ok()) << status.GetMessage();
}

TEST_F(AsioSslContextFactoryTest, CreateClientContext_mTLS) {
  TlsConfig config;
  config.enabled = true;
  config.mutual_auth = true;
  config.cert_file = certs_dir_ + "client.crt";
  config.key_file = certs_dir_ + "client.key";
  config.ca_file = certs_dir_ + "ca.crt";

  AsioSslContextFactory factory(config);
  asio::ssl::context ctx(asio::ssl::context::tls_client);
  auto status = factory.CreateClientContext(ctx);
  EXPECT_TRUE(status.ok()) << status.GetMessage();
}

}  // namespace rollingraft
