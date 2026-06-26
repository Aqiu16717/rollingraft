#include "rollingraft/metrics.h"

#include "metrics_http_server.h"
#include "test_port.h"
#include <gtest/gtest.h>

using namespace rollingraft;

// Testable subclass to access protected BuildResponse
class TestableMetricsHttpServer : public MetricsHttpServer {
 public:
  TestableMetricsHttpServer(const std::string& bind_addr, MetricsRegistry* registry,
                            const TlsConfig& tls_config = {}, const std::string& admin_token = {})
      : MetricsHttpServer(bind_addr, registry, tls_config, admin_token) {}

  auto TestBuildResponse(const std::string& request) { return BuildResponse(request); }

  bool TestCheckRateLimit(const std::string& client_ip) { return CheckRateLimit(client_ip); }
};

class MetricsHttpServerAuthTest : public ::testing::Test {
 protected:
  std::string MakeRequest(const std::string& method, const std::string& path,
                          const std::string& body = "", const std::string& auth_token = "") {
    std::string req = method + " " + path + " HTTP/1.1\r\n";
    req += "Host: localhost\r\n";
    if (!auth_token.empty()) {
      req += "Authorization: Bearer " + auth_token + "\r\n";
    }
    if (!body.empty()) {
      req += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    }
    req += "\r\n";
    req += body;
    return req;
  }

  bool IsSuccess(const std::string& status_line) {
    return status_line.find("200 OK") != std::string::npos ||
           status_line.find("202 Accepted") != std::string::npos;
  }

  bool IsUnauthorized(const std::string& status_line) {
    return status_line.find("401 Unauthorized") != std::string::npos;
  }
};

TEST_F(MetricsHttpServerAuthTest, NoTokenAllowsAll) {
  MetricsRegistry registry;
  TestableMetricsHttpServer server("127.0.0.1:" + std::to_string(GetUniqueTestPort()), &registry);

  // Admin endpoints without token should succeed when no admin_token is set
  auto [body, status, ct, sse] = server.TestBuildResponse(MakeRequest("POST", "/v1/members"));
  EXPECT_TRUE(IsSuccess(status) || status.find("404") != std::string::npos)
      << "Expected success or 404 (handler not set), got: " << status;
}

TEST_F(MetricsHttpServerAuthTest, MissingTokenReturns401) {
  MetricsRegistry registry;
  TestableMetricsHttpServer server("127.0.0.1:" + std::to_string(GetUniqueTestPort()), &registry,
                                   {}, "secret-token");

  auto [body, status, ct, sse] = server.TestBuildResponse(MakeRequest("POST", "/v1/members"));
  EXPECT_TRUE(IsUnauthorized(status)) << "Expected 401 for missing token, got: " << status;
  EXPECT_NE(body.find("UNAUTHORIZED"), std::string::npos);
}

TEST_F(MetricsHttpServerAuthTest, WrongTokenReturns401) {
  MetricsRegistry registry;
  TestableMetricsHttpServer server("127.0.0.1:" + std::to_string(GetUniqueTestPort()), &registry,
                                   {}, "secret-token");

  auto [body, status, ct, sse] =
      server.TestBuildResponse(MakeRequest("POST", "/v1/members", "", "wrong-token"));
  EXPECT_TRUE(IsUnauthorized(status)) << "Expected 401 for wrong token, got: " << status;
}

TEST_F(MetricsHttpServerAuthTest, CorrectTokenAllowsAdmin) {
  MetricsRegistry registry;
  TestableMetricsHttpServer server("127.0.0.1:" + std::to_string(GetUniqueTestPort()), &registry,
                                   {}, "secret-token");

  auto [body, status, ct, sse] =
      server.TestBuildResponse(MakeRequest("POST", "/v1/members", "", "secret-token"));
  EXPECT_TRUE(IsSuccess(status) || status.find("404") != std::string::npos)
      << "Expected success for correct token, got: " << status;
}

TEST_F(MetricsHttpServerAuthTest, PublicEndpointsIgnoreToken) {
  MetricsRegistry registry;
  TestableMetricsHttpServer server("127.0.0.1:" + std::to_string(GetUniqueTestPort()), &registry,
                                   {}, "secret-token");

  // /metrics should be accessible without token
  auto [body1, status1, ct1, sse1] = server.TestBuildResponse(MakeRequest("GET", "/metrics"));
  EXPECT_TRUE(IsSuccess(status1)) << "Expected 200 for /metrics, got: " << status1;

  // /healthz should be accessible without token
  auto [body2, status2, ct2, sse2] = server.TestBuildResponse(MakeRequest("GET", "/healthz"));
  EXPECT_TRUE(IsSuccess(status2)) << "Expected 200 for /healthz, got: " << status2;

  // /v1/status should be accessible without token
  auto [body3, status3, ct3, sse3] = server.TestBuildResponse(MakeRequest("GET", "/v1/status"));
  EXPECT_TRUE(IsSuccess(status3) || status3.find("404") != std::string::npos)
      << "Expected success for /v1/status, got: " << status3;

  // /v1/events should be accessible without token (SSE endpoint)
  auto [body4, status4, ct4, sse4] = server.TestBuildResponse(MakeRequest("GET", "/v1/events"));
  EXPECT_TRUE(sse4) << "Expected SSE endpoint to be identified";
}

TEST_F(MetricsHttpServerAuthTest, RateLimitAllowsWithinThreshold) {
  MetricsRegistry registry;
  TestableMetricsHttpServer server("127.0.0.1:" + std::to_string(GetUniqueTestPort()), &registry);

  // 9 requests within 1s window should be allowed
  for (int i = 0; i < 9; ++i) {
    EXPECT_TRUE(server.TestCheckRateLimit("192.168.1.1"))
        << "Request " << i << " should be allowed";
  }
}

TEST_F(MetricsHttpServerAuthTest, RateLimitBlocksOverThreshold) {
  MetricsRegistry registry;
  TestableMetricsHttpServer server("127.0.0.1:" + std::to_string(GetUniqueTestPort()), &registry);

  // 10 requests within 1s window should be allowed
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(server.TestCheckRateLimit("192.168.1.1"))
        << "Request " << i << " should be allowed";
  }

  // 11th request should be blocked
  EXPECT_FALSE(server.TestCheckRateLimit("192.168.1.1")) << "11th request should be blocked";
}

TEST_F(MetricsHttpServerAuthTest, RateLimitIsPerIp) {
  MetricsRegistry registry;
  TestableMetricsHttpServer server("127.0.0.1:" + std::to_string(GetUniqueTestPort()), &registry);

  // Fill up one IP
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(server.TestCheckRateLimit("192.168.1.1"));
  }
  EXPECT_FALSE(server.TestCheckRateLimit("192.168.1.1"));

  // Different IP should still be allowed
  EXPECT_TRUE(server.TestCheckRateLimit("192.168.1.2"));
}

TEST_F(MetricsHttpServerAuthTest, AllAdminEndpointsProtected) {
  MetricsRegistry registry;
  TestableMetricsHttpServer server("127.0.0.1:" + std::to_string(GetUniqueTestPort()), &registry,
                                   {}, "admin-key");

  struct Endpoint {
    std::string method;
    std::string path;
  };
  std::vector<Endpoint> admin_endpoints = {
      {"POST", "/v1/members"},
      {"DELETE", "/v1/members/2"},
      {"POST", "/v1/snapshot/trigger"},
      {"POST", "/v1/leadership/transfer"},
      {"GET", "/v1/config"},
      {"PATCH", "/v1/config"},
  };

  for (const auto& ep : admin_endpoints) {
    auto [body, status, ct, sse] = server.TestBuildResponse(MakeRequest(ep.method, ep.path));
    EXPECT_TRUE(IsUnauthorized(status))
        << "Expected 401 for " << ep.method << " " << ep.path << ", got: " << status;
  }
}
