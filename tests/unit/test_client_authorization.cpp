#include "client_authorization.h"
#include "json_protocol.h"

#include <gtest/gtest.h>

namespace rollingraft {
namespace {

RpcRequestContext ClientContext(std::string identity) {
  return RpcRequestContext{.peer = {.kind = RpcPeerKind::CLIENT,
                                    .node_id = -1,
                                    .client_identity = std::move(identity)}};
}

}  // namespace

TEST(ClientAuthorizationPolicyTest, ReadWriteIdentityAllowsReadsAndWrites) {
  ClientAuthorizationPolicy policy({{"writer", ClientPermission::READ_WRITE}});

  EXPECT_TRUE(policy.AuthorizeClientRequest(ClientContext("writer"), true).ok());
  EXPECT_TRUE(policy.AuthorizeClientRequest(ClientContext("writer"), false).ok());
}

TEST(ClientAuthorizationPolicyTest, ReadOnlyIdentityRejectsWrite) {
  ClientAuthorizationPolicy policy({{"reader", ClientPermission::READ_ONLY}});

  EXPECT_TRUE(policy.AuthorizeClientRequest(ClientContext("reader"), true).ok());
  auto status = policy.AuthorizeClientRequest(ClientContext("reader"), false);
  EXPECT_TRUE(status.IsPermissionDenied());
}

TEST(ClientAuthorizationPolicyTest, UnknownIdentityIsPermissionDenied) {
  ClientAuthorizationPolicy policy({{"writer", ClientPermission::READ_WRITE}});

  EXPECT_TRUE(policy.AuthorizeClientRequest(ClientContext("unknown"), true).IsPermissionDenied());
}

TEST(ClientAuthorizationPolicyTest, NonClientPrincipalsAreUnauthenticated) {
  ClientAuthorizationPolicy policy({{"writer", ClientPermission::READ_WRITE}});
  RpcRequestContext node_context{.peer = {.kind = RpcPeerKind::NODE, .node_id = 1}};
  RpcRequestContext anonymous_context;

  EXPECT_TRUE(policy.AuthorizeClientRequest(node_context, true).IsUnauthenticated());
  EXPECT_TRUE(policy.AuthorizeClientRequest(anonymous_context, true).IsUnauthenticated());
}

TEST(ClientAuthorizationPolicyTest, ClassifiesOnlyRaftProtocolMessagesAsRaft) {
  EXPECT_TRUE(IsRaftProtocolRequest(RaftMessageType::KRequestVoteRequest));
  EXPECT_TRUE(IsRaftProtocolRequest(RaftMessageType::KAppendEntriesRequest));
  EXPECT_TRUE(IsRaftProtocolRequest(RaftMessageType::KInstallSnapshotRequest));
  EXPECT_TRUE(IsRaftProtocolRequest(RaftMessageType::KPreVoteRequest));
  EXPECT_TRUE(IsRaftProtocolRequest(RaftMessageType::KReadIndexRequest));
  EXPECT_FALSE(IsRaftProtocolRequest(RaftMessageType::KClientRequest));
}

TEST(ClientAuthorizationPolicyTest, ResponseErrorCodeRoundTripsAndOlderResponsesRemainValid) {
  JsonProtocol protocol;
  ClientResponse response;
  response.success = false;
  response.error = "Client identity is not authorized";
  response.error_code = "PERMISSION_DENIED";

  std::string serialized;
  ASSERT_TRUE(protocol.SerializeResponse(response, serialized).ok());
  ClientResponse parsed;
  ASSERT_TRUE(protocol.DeserializeResponse(serialized, parsed).ok());
  EXPECT_EQ(parsed.error_code, "PERMISSION_DENIED");

  ASSERT_TRUE(protocol.DeserializeResponse(
                  R"({"type":7,"success":false,"error":"legacy failure"})", parsed)
                  .ok());
  EXPECT_TRUE(parsed.error_code.empty());
}

}  // namespace rollingraft
