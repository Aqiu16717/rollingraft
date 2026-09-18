# Client mTLS Authentication and Authorization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Authenticate high-level clients with client mTLS certificates and enforce static read-only/read-write authorization before any Raft client handler runs.

**Architecture:** The shared ASIO listener will classify CA-verified URI SAN identities into immutable node/client request contexts. New authenticated `NetworkTransport` entry points preserve legacy custom transports while allowing `RaftNode` and `RaftStore` to reject unsupported transport configurations. A small authorization policy component gates `ClientRequest`; existing node-only Raft dispatch and sender/membership checks remain intact.

**Tech Stack:** C++20, standalone ASIO/OpenSSL, nlohmann/json, GoogleTest, CMake, Make.

**Spec:** `docs/superpowers/specs/2026-09-14-client-mtls-authorization-design.md`

## Global Constraints

- Preserve `ClientRequest.client_id` and `seq` as caller-controlled deduplication keys; never use either as identity.
- Client authentication is opt-in. With `client_auth_enabled == false`, existing plaintext and node-mTLS client behavior must remain compatible.
- Enabling client authentication requires `tls_enabled == true`, `tls_mutual_auth == true`, a non-empty `client_ca_file`, at least one unique static authorization rule, and an authenticated-context-capable transport.
- Node URI SAN is exactly `rollingraft-node:<node_id>`; client URI SAN matches `rollingraft-client:[A-Za-z0-9][A-Za-z0-9._-]{0,127}`.
- A certificate with zero, duplicate, mixed, malformed, or conflicting RollingRaft identities is rejected. Unrelated SAN entries do not supply identity.
- Client mTLS uses a client CA separate in configuration from the node CA. Test keys and certificates are generated under the build directory and are never committed.
- Return `UNAUTHENTICATED` for missing/wrong principal authentication and `PERMISSION_DENIED` for a valid but unauthorized client. Neither error is retryable and neither clears leader cache state.
- Client certificates may issue only `ClientRequest`; node certificates may issue only Raft protocol requests. Authentication and authorization precede handler invocation.
- ACLs are immutable after startup, node-wide for all `RaftStore` groups, case-sensitive, exact-match, and deny by default.
- Keep the public legacy `NetworkTransport::Initialize`, `RpcRequestHandler`, and `GroupRequestHandler` source-compatible.
- Follow Google C++ style; comments are English; build warning-free under `-Werror`.
- Preserve the dirty `third_party/leveldb` submodule and untracked `.codex/` directory. Never stage either.

---

## File structure

| File | Responsibility |
|---|---|
| `include/rollingraft/raft_node.h` | Public client authorization rule and node-level opt-in configuration. |
| `src/raft_store.h` | Mirror node-level client-auth configuration for shared multi-raft infrastructure. |
| `include/rollingraft/rpc_peer_identity.h` | Public typed peer identity shared by TLS parsing and transport context. |
| `include/rollingraft/network_transport.h` | Immutable authenticated peer context and backward-compatible authenticated transport hooks. |
| `include/rollingraft/tls_config.h` | TLS-only fields for the client CA and identity classification mode. |
| `src/tls_identity.h/.cpp` | Strict client/node URI SAN parsing and local client certificate validation. |
| `src/client_authorization.h/.cpp` | Immutable static ACL validation and request authorization decision. |
| `include/rollingraft/status.h`, `src/status.cpp` | Stable in-process unauthenticated and permission-denied status codes. |
| `include/rollingraft/asio_ssl_context_factory.h`, `src/asio_ssl_context_factory.cpp` | Server/client SSL contexts that load node and client trust material correctly. |
| `src/asio_network_transport.cpp` | Per-connection identity capture and context-aware dispatch on the shared listener. |
| `src/raft_node.cpp`, `src/raft_node_core.cpp`, `src/raft_node_impl.h`, `src/rpc_handlers.cpp` | Config validation, authenticated initialization, and single-group authorization gate. |
| `src/raft_store.cpp` | Context-aware multi-raft routing and node-wide authorization propagation. |
| `include/rollingraft/rpc.h`, `src/json_protocol.cpp`, `src/rpc_client.cpp` | Stable `ClientResponse.error_code` and TLS client request/response framing. |
| `include/rollingraft/client.h`, `src/client.cpp`, `src/client/retry_policy.cpp` | Client TLS options, construction-time validation, and non-retryable auth errors. |
| `tests/generate_node_test_certs.sh`, `tests/CMakeLists.txt` | Build-generated distinct node/client/rogue credentials. |
| `tests/unit/test_asio_ssl_context_factory.cpp`, `tests/unit/test_raft_node_config.cpp`, `tests/unit/test_client.cpp` | Focused TLS/config/client regression coverage. |
| `tests/unit/test_client_authorization.cpp` | Pure policy and dispatch classification tests. |
| `tests/integration/test_cluster_3nodes.cpp`, `tests/integration/test_multi_raft_2groups.cpp` | Real TLS authorization and multi-raft routing tests. |
| `docs/public-api-guide.md`, `docs/operations-guide.md` | User-facing client credential/configuration and deployment guidance. |

### Task 1: Define authenticated identity and configuration primitives

**Files:**
- Modify: `include/rollingraft/raft_node.h:149-214`
- Create: `include/rollingraft/rpc_peer_identity.h`
- Modify: `src/raft_store.h:26-46`
- Modify: `include/rollingraft/tls_config.h:11-57`
- Modify: `include/rollingraft/status.h`
- Modify: `src/status.cpp`
- Modify: `src/tls_identity.h`
- Modify: `src/tls_identity.cpp`
- Modify: `src/raft_node.cpp:55-116`
- Modify: `tests/generate_node_test_certs.sh`
- Modify: `tests/CMakeLists.txt`
- Modify: `tests/unit/test_asio_ssl_context_factory.cpp`
- Modify: `tests/unit/test_raft_node_config.cpp`

**Interfaces:**
- Produces `enum class ClientPermission { READ_ONLY, READ_WRITE };` and `struct ClientAuthorizationRule { std::string identity; ClientPermission permission; };` in the public Raft configuration surface.
- Produces `RpcPeerKind { ANONYMOUS, NODE, CLIENT }` and `RpcPeerIdentity` in a small public header independent of the transport interface.
- Produces `Status ExtractCertificatePeerIdentity(X509*, RpcPeerIdentity&)`, `Status ValidateCertificateClientIdentity(const std::string&)`, and `Status ValidateCertificateNodeId(const std::string&, NodeId)`.
- Produces `Status::Unauthenticated`, `Status::PermissionDenied`, `IsUnauthenticated`, and `IsPermissionDenied` backed by distinct `Status::Code` values.
- Produces `bool client_auth_enabled`, `std::string client_ca_file`, and `std::vector<ClientAuthorizationRule> client_authorizations` in both node/store configuration types.
- Consumed by Tasks 2–5.

- [ ] **Step 1: Write failing identity parser and config tests**

  Add tests that load generated node and client certificates and assert the exact peer kind and value. Add table-style configuration cases for: client auth with TLS disabled, with mutual auth disabled, missing client CA, empty ACL, duplicate ACL identity, invalid client identity token, and a fully valid client-auth configuration.

  ```cpp
  TEST_F(AsioSslContextFactoryTest, ExtractCertificatePeerIdentity_ClientUriSan) {
    RpcPeerIdentity identity;
    ASSERT_TRUE(LoadAndExtract(client_certs_dir_ + "writer.crt", identity).ok());
    EXPECT_EQ(identity.kind, RpcPeerKind::CLIENT);
    EXPECT_EQ(identity.client_identity, "writer");
  }

  TEST_F(RaftNodeConfigValidateTest, ClientAuthRequiresMutualTlsClientCaAndRules) {
    auto config = MakeValidConfig();
    config.client_auth_enabled = true;
    EXPECT_FALSE(config.Validate().ok());
    EXPECT_NE(config.Validate().GetMessage().find("CONFIG_INVALID"), std::string::npos);
    config.tls_enabled = true;
    config.tls_mutual_auth = true;
    config.tls_cert_file = "/tmp/node.crt";
    config.tls_key_file = "/tmp/node.key";
    config.tls_ca_file = "/tmp/node-ca.crt";
    config.client_ca_file = "/tmp/client-ca.crt";
    config.client_authorizations = {{"writer", ClientPermission::READ_WRITE}};
    EXPECT_TRUE(config.Validate().ok());
  }
  ```

- [ ] **Step 2: Run the focused tests and verify they fail**

  Run: `./build/release/tests/unit_tests --gtest_filter='AsioSslContextFactoryTest.ExtractCertificatePeerIdentity_ClientUriSan:RaftNodeConfigValidateTest.ClientAuthRequiresMutualTlsClientCaAndRules'`

  Expected: compile failure because the peer identity API and client-auth fields do not exist.

- [ ] **Step 3: Extend the build-time certificate generator before parser implementation**

  Keep all generated private keys in `${CMAKE_BINARY_DIR}/generated-node-certs`. Extend `tests/generate_node_test_certs.sh` to generate a separate client CA; `reader`, `writer`, and `unknown` client certificates with one `rollingraft-client:<identity>` URI SAN; a wrong-CA client certificate; and mixed-kind/malformed certificates for parser tests. Update `tests/CMakeLists.txt` so unit and integration tests receive the same generated directory macro. Do not add generated output or any private key under `tests/certs/` to Git.

- [ ] **Step 4: Add the public configuration types, status codes, and strict SAN classifier**

  Put `ClientPermission` and `ClientAuthorizationRule` next to `RaftNodeConfig`; use the same rule type from `RaftStoreConfig`. Define `RpcPeerKind` and `RpcPeerIdentity` in `rpc_peer_identity.h`, and include it from `tls_identity.h`. Add the three client-auth configuration fields with safe disabled defaults. Add TLS-only `client_ca_file` and `client_auth_enabled` fields to `TlsConfig` so the ASIO factory receives only TLS concerns.

  Add `kUnauthenticated` and `kPermissionDenied` to `Status::Code`, their named constructors and predicates in `status.h`, and both `ToString()`/`CodeName()` cases in `status.cpp`. Keep existing generic `CONFIG_INVALID` and `UNSUPPORTED` internal error conventions unchanged.

  Replace the node-only certificate traversal with a shared traversal that:

  ```cpp
  inline constexpr char kRaftClientSanPrefix[] = "rollingraft-client:";

  Status ExtractCertificatePeerIdentity(X509* certificate, RpcPeerIdentity& out) {
    // Read GEN_URI SANs, reject more than one recognized RollingRaft URI,
    // parse node IDs with from_chars, and validate the exact client token grammar.
    // Set out.kind/node_id/client_identity only after all SANs validate.
  }
  ```

  Keep `ExtractCertificateNodeId` as a compatibility wrapper that calls the shared parser and rejects non-node identities. Implement `ValidateCertificateClientIdentity` with the same PEM loading and error-code style as `ValidateCertificateNodeId`.

- [ ] **Step 5: Validate all new node configuration invariants**

  Extend `RaftNodeConfig::Validate()` to reject partial client-auth configuration before any network object is constructed. Validate identity grammar and duplicate authorization rules with an `std::unordered_set<std::string>`. Add a small `ValidateClientAuthConfig(...)` helper shared by the single-node and store paths; `RaftStore::Initialize()` calls it before setting `initialized_` or allocating infrastructure.

  ```cpp
  if (client_auth_enabled && (!tls_enabled || !tls_mutual_auth || client_ca_file.empty() ||
                              client_authorizations.empty())) {
    return Status::Error("CONFIG_INVALID", "client authentication requires mTLS, client CA, and rules");
  }
  ```

- [ ] **Step 6: Re-run focused tests and inspect the diff**

  Run: `make release && ./build/release/tests/unit_tests --gtest_filter='AsioSslContextFactoryTest.*:RaftNodeConfigValidateTest.*'`

  Expected: PASS, including existing node certificate validation tests.

  Run: `git diff --check`

  Expected: no whitespace errors.

- [ ] **Step 7: Commit the primitive/configuration slice**

  ```bash
  git add include/rollingraft/raft_node.h include/rollingraft/rpc_peer_identity.h \
    src/raft_store.h include/rollingraft/tls_config.h include/rollingraft/status.h \
    src/status.cpp src/tls_identity.h src/tls_identity.cpp src/raft_node.cpp \
    tests/generate_node_test_certs.sh tests/CMakeLists.txt \
    tests/unit/test_asio_ssl_context_factory.cpp tests/unit/test_raft_node_config.cpp
  git commit -m "feat(auth): add client identity configuration"
  ```

### Task 2: Add compatible authenticated transport dispatch and TLS trust roots

**Files:**
- Modify: `include/rollingraft/network_transport.h:24-124`
- Modify: `include/rollingraft/asio_ssl_context_factory.h`
- Modify: `src/asio_ssl_context_factory.cpp`
- Modify: `src/asio_network_transport.cpp:67-526,893-1234`
- Modify: `tests/mock/mock_network.h`
- Modify: `tests/mock/mock_network.cpp`
- Modify: `tests/unit/test_asio_ssl_context_factory.cpp`

**Interfaces:**
- Consumes `RpcPeerIdentity` and `TlsConfig.client_ca_file` from Task 1.
- Produces `RpcRequestContext`, `AuthenticatedRpcRequestHandler`, `AuthenticatedGroupRequestHandler`, `NetworkTransport::InitializeAuthenticated`, `NetworkTransport::SupportsAuthenticatedPeerIdentity`, and `NetworkTransport::SetAuthenticatedGroupRequestHandler`.
- Produces ASIO connection-level `RpcRequestContext` populated after TLS handshake.
- Consumed by Tasks 3 and 5.

- [ ] **Step 1: Write failing compatibility and SSL-context tests**

  Add a mock transport test proving the legacy `Initialize` path still compiles and works. Add tests that a client-auth server SSL context loads both trust roots and requires a peer certificate, while a client context loads its client certificate/key and node CA.

  ```cpp
  TEST(NetworkTransportAuthTest, DefaultAuthenticatedEntryPointIsUnsupported) {
    MockNetworkTransport transport;
    EXPECT_FALSE(transport.SupportsAuthenticatedPeerIdentity());
    EXPECT_NE(transport.InitializeAuthenticated("127.0.0.1:1", {}).GetMessage().find("UNSUPPORTED"),
              std::string::npos);
  }

  TEST_F(AsioSslContextFactoryTest, CreateServerContext_ClientAuthLoadsNodeAndClientCas) {
    auto config = MakeClientAuthTlsConfig();
    asio::ssl::context context(asio::ssl::context::tls_server);
    EXPECT_TRUE(AsioSslContextFactory(config).CreateServerContext(context).ok());
  }
  ```

- [ ] **Step 2: Run tests and verify the new transport API is absent**

  Run: `./build/release/tests/unit_tests --gtest_filter='NetworkTransportAuthTest.*:AsioSslContextFactoryTest.CreateServerContext_ClientAuthLoadsNodeAndClientCas'`

  Expected: compile failure for `InitializeAuthenticated` and the client-auth TLS setup helper.

- [ ] **Step 3: Extend `NetworkTransport` without breaking existing implementers**

  Define the context and aliases in `network_transport.h`, importing `RpcPeerIdentity` from Task 1:

  ```cpp
  struct RpcRequestContext { RpcPeerIdentity peer; };
  using AuthenticatedRpcRequestHandler = std::function<void(
      const RpcRequestContext&, const std::string&, std::string&)>;
  using AuthenticatedGroupRequestHandler = std::function<void(
      const RpcRequestContext&, uint64_t, const std::string&, std::string&)>;
  ```

  Keep the existing pure virtual `Initialize` unchanged. Add non-pure virtual defaults: `InitializeAuthenticated(...)` returns `Status::Error("UNSUPPORTED", ...)`, `SupportsAuthenticatedPeerIdentity()` returns false, and `SetAuthenticatedGroupRequestHandler(...)` is a no-op. Do not modify mock or deterministic transport behavior beyond compile coverage.

- [ ] **Step 4: Load both trust roots and preserve outbound node mTLS**

  In `AsioSslContextFactory::CreateServerContext`, when `client_auth_enabled` is true, require and load both `ca_file` and `client_ca_file`, set `verify_peer | verify_fail_if_no_peer_cert`, and validate the local certificate as a node certificate. Keep `CreateClientContext` for outbound Raft links tied to the node certificate and node CA only; high-level client TLS gets its own factory/helper in Task 4.

- [ ] **Step 5: Route ASIO inbound requests by immutable authenticated context**

  Make `AsioNetworkTransport::SupportsAuthenticatedPeerIdentity()` true. `InitializeAuthenticated` owns the authenticated handler; `TcpConnection` stores a `RpcRequestContext` assigned exactly once after `AuthenticateTlsPeer` succeeds. In client-auth mode, `AuthenticateTlsPeer` calls `ExtractCertificatePeerIdentity`; in node-mTLS-only mode it retains the existing node-ID binding behavior.

  In `TcpConnection::HandleMessage`, select the authenticated handler if configured; otherwise retain the legacy handler path. Copy handlers and context under the existing connection mutex before invoking them. A client principal must never be converted to a synthetic `NodeId`.

- [ ] **Step 6: Re-run focused transport and TLS tests**

  Run: `make release && ./build/release/tests/unit_tests --gtest_filter='NetworkTransportAuthTest.*:AsioSslContextFactoryTest.*'`

  Expected: PASS. Existing mock, deterministic, and node-mTLS compilation remains intact.

- [ ] **Step 7: Commit the transport slice**

  ```bash
  git add include/rollingraft/network_transport.h include/rollingraft/asio_ssl_context_factory.h \
    src/asio_ssl_context_factory.cpp src/asio_network_transport.cpp \
    tests/mock/mock_network.h tests/mock/mock_network.cpp \
    tests/unit/test_asio_ssl_context_factory.cpp
  git commit -m "feat(auth): carry peer identity through transport"
  ```

### Task 3: Enforce authorization in single-node and multi-raft dispatch

**Files:**
- Create: `src/client_authorization.h`
- Create: `src/client_authorization.cpp`
- Modify: `include/rollingraft/rpc.h:230-246`
- Modify: `src/raft_node_impl.h:97-110`
- Modify: `src/raft_node_core.cpp:180-195`
- Modify: `src/rpc_handlers.cpp:23-165,840-900`
- Modify: `src/raft_store.h:101-105`
- Modify: `src/raft_store.cpp:92-108,321-348,590-601`
- Modify: `CMakeLists.txt`
- Create: `tests/unit/test_client_authorization.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes authenticated transport types from Task 2, static rules from Task 1, and the stable `Status` predicates from Task 1.
- Produces `ClientAuthorizationPolicy`, `Status AuthorizeClientRequest(const RpcRequestContext&, bool read_only) const`, and context-aware `HandleIncomingRpc`/`OnIncomingRpc` overloads.
- Produces `ClientResponse.error_code` for a response that is denied before a client handler runs.
- Produces serialized client authorization errors with no state-machine invocation.
- Consumed by Tasks 4 and 5.

- [ ] **Step 1: Write failing pure-policy tests**

  Create `test_client_authorization.cpp` with direct, no-socket tests for every policy row: READ_WRITE read/write success, READ_ONLY read success/write denial, unknown client denial, node principal denial for client requests, client principal denial for Raft messages, and anonymous denial when auth is enabled.

  ```cpp
  TEST(ClientAuthorizationPolicyTest, ReadOnlyIdentityRejectsWrite) {
    ClientAuthorizationPolicy policy({{"reader", ClientPermission::READ_ONLY}});
    RpcRequestContext context{.peer = {.kind = RpcPeerKind::CLIENT,
                                       .client_identity = "reader"}};
    auto status = policy.AuthorizeClientRequest(context, false);
    EXPECT_TRUE(status.IsPermissionDenied());
  }
  ```

- [ ] **Step 2: Run the policy test and verify it fails**

  Run: `./build/release/tests/unit_tests --gtest_filter='ClientAuthorizationPolicyTest.*'`

  Expected: build failure because the policy component is not yet registered in CMake.

- [ ] **Step 3: Implement immutable policy and request-type classification**

  Implement `ClientAuthorizationPolicy` as a validated `std::unordered_map<std::string, ClientPermission>`. Its methods return `Status::Unauthenticated(...)` for non-client principals and `Status::PermissionDenied(...)` for unknown or insufficiently privileged client identities. Add `IsRaftProtocolRequest(RaftMessageType)` covering RequestVote, AppendEntries, InstallSnapshot, PreVote, and ReadIndex.

- [ ] **Step 4: Thread context through node and store routes**

  Add context-aware handlers without removing the legacy `NodeId` signatures. `RaftNodeImpl::HandleIncomingRpc(const RpcRequestContext&, ...)` authenticates message class before deserializing/mutating a handler; legacy handler wraps its `NodeId` as a node context only when client auth is disabled. `RaftStore::OnIncomingRpc(const RpcRequestContext&, group_id, ...)` preserves that context through group lookup.

  When `client_auth_enabled` is true, `RaftNodeCore::Start` and `RaftStore::Start` require `SupportsAuthenticatedPeerIdentity()`, register authenticated handlers, and call `InitializeAuthenticated`. Fail deterministically with `CONFIG_INVALID` before `Start` opens listener threads if the selected custom transport cannot support it.

- [ ] **Step 5: Add the wire error-code field and serialize authorization denial before handler entry**

  Add `std::string error_code` to `ClientResponse`. In the `KClientRequest` dispatcher branch, deserialize only enough to retain correlation/group IDs, call the policy, and on failure set `success = false`, `error_code` to `UNAUTHENTICATED` or `PERMISSION_DENIED`, and a non-secret error message. Do not call `HandleClientRequest`, `ReadIndex`, `Query`, or `Propose` after a denial. For Raft message types received from a client principal, log the principal type and close/reject without invoking a Raft handler.

- [ ] **Step 6: Run policy and existing routing tests**

  Run: `make release && ./build/release/tests/unit_tests --gtest_filter='ClientAuthorizationPolicyTest.*:RaftNodeConfigValidateTest.*'`

  Run: `./build/release/tests/integration_tests --gtest_filter='MultiRaft*'`

  Expected: PASS; multi-raft legacy routing remains functional with client auth disabled.

- [ ] **Step 7: Commit the authorization slice**

  ```bash
  git add CMakeLists.txt src/client_authorization.h src/client_authorization.cpp \
    src/raft_node_impl.h src/raft_node_core.cpp src/rpc_handlers.cpp \
    src/raft_store.h src/raft_store.cpp tests/unit/test_client_authorization.cpp tests/CMakeLists.txt
  git commit -m "feat(auth): authorize client rpc requests"
  ```

### Task 4: Add response error codes and high-level client TLS/retry behavior

**Files:**
- Modify: `include/rollingraft/rpc.h:230-246`
- Modify: `src/json_protocol.cpp:235-360`
- Modify: `src/rpc_client.cpp:17-190`
- Modify: `include/rollingraft/client.h:77-107`
- Modify: `src/client.cpp:52-288`
- Modify: `src/client/retry_policy.cpp`
- Modify: `tests/unit/test_client.cpp`
- Modify: `tests/unit/test_client_authorization.cpp`

**Interfaces:**
- Consumes `ClientResponse.error_code` from Task 3 and TLS identity validation from Tasks 1–3.
- Produces `ClientOptions::{tls_enabled,tls_cert_file,tls_key_file,tls_ca_file}` and TLS-aware `RpcCall` for `ClientRequest`.
- Produces non-retryable client `Status` values for `UNAUTHENTICATED` and `PERMISSION_DENIED`.
- Consumed by Task 5.

- [ ] **Step 1: Write failing protocol and client behavior tests**

  Test JSON round-trip for `ClientResponse.error_code` and acceptance of an older response without it. Test invalid client TLS options produce a stored initialization error without opening a socket. Test `RetryPolicy::IsRetryableError` rejects both stable authorization codes, and a cached leader is retained after such an error.

  ```cpp
  TEST(ClientTest, TlsEnabledWithoutClientCertificateReturnsConfigError) {
    ClientOptions options;
    options.tls_enabled = true;
    options.tls_ca_file = "/tmp/node-ca.crt";
    Client client({"127.0.0.1:59991"}, options);
    auto result = client.Execute("write");
    ASSERT_TRUE(result.has_error());
    EXPECT_NE(result.error().GetMessage().find("CONFIG_INVALID"), std::string::npos);
  }
  ```

- [ ] **Step 2: Run focused tests and verify failure**

  Run: `./build/release/tests/unit_tests --gtest_filter='ClientTest.TlsEnabledWithoutClientCertificateReturnsConfigError:ClientAuthorizationPolicyTest.ResponseErrorCodeRoundTrips'`

  Expected: compile failure because TLS client fields and `error_code` do not exist.

- [ ] **Step 3: Add backward-compatible response error-code serialization**

  `JsonProtocol::SerializeResponse` writes `ClientResponse.error_code` when non-empty; `JsonProtocol::DeserializeResponse` and `rpc_client.cpp` accept it as optional. Preserve older peers and existing response fields exactly.

- [ ] **Step 4: Implement a TLS-capable synchronous high-level client path**

  Extend `DoRpcCall` to accept `const ClientOptions&`. When TLS is enabled, create an `asio::ssl::context::tls_client`, load the configured node CA, load client certificate and key, require peer verification, perform the TLS handshake, retrieve the peer certificate, and require one valid node URI SAN through `ExtractCertificatePeerIdentity`. Use the existing length-prefixed framing over the SSL stream after handshake. Keep the raw-socket behavior byte-for-byte unchanged when TLS is disabled.

  In `Client::Impl`, validate all TLS paths and the local client certificate once during construction; retain a `std::optional<Status> initialization_error_`. `DoExecute` returns that error before leader lookup or any network call. Map response `error_code` `UNAUTHENTICATED` to `Status::Unauthenticated(resp.error)` and `PERMISSION_DENIED` to `Status::PermissionDenied(resp.error)`; map unknown codes to the existing generic `Status::Error`.

- [ ] **Step 5: Make stable authorization errors terminal**

  Extend `RetryPolicy::IsRetryableError` to return false for `status.IsUnauthenticated()` and `status.IsPermissionDenied()`. In `DoExecute`, check terminal errors from a cached-leader attempt before calling `leader_tracker_.ClearLeader()`, preserving the cache. Keep existing not-leader redirect behavior and transport retry behavior unchanged.

- [ ] **Step 6: Run client and protocol tests**

  Run: `make release && ./build/release/tests/unit_tests --gtest_filter='ClientTest.*:ClientAuthorizationPolicyTest.*'`

  Expected: PASS, including existing plaintext client tests.

- [ ] **Step 7: Commit the client protocol slice**

  ```bash
  git add include/rollingraft/rpc.h src/json_protocol.cpp src/rpc_client.cpp \
    include/rollingraft/client.h src/client.cpp src/client/retry_policy.cpp \
    tests/unit/test_client.cpp tests/unit/test_client_authorization.cpp
  git commit -m "feat(client): add mtls authorization errors"
  ```

### Task 5: Prove end-to-end authorization with generated credentials

**Files:**
- Modify: `tests/integration/test_cluster_3nodes.cpp`
- Modify: `tests/integration/test_multi_raft_2groups.cpp`

**Interfaces:**
- Consumes client TLS options, authenticated transport, and authorization policy from Tasks 1–4.
- Produces real-socket tests for the complete identity and authorization matrix.

- [ ] **Step 1: Write failing integration scenarios**

  Add a fixture that starts a three-node cluster with node mTLS, client auth enabled, a separate client CA, and two ACL entries (`reader` READ_ONLY and `writer` READ_WRITE). Add tests for writer query/execute, reader query/write denial, unknown identity denial, wrong-CA handshake failure, node certificate attempted as a client, client certificate attempting a Raft RPC, and client-auth-disabled plaintext compatibility.

  ```cpp
  TEST_F(Cluster3NodesTest, ClientMtlsReaderCannotExecute) {
    Client reader(addrs_, ReaderClientOptions());
    auto result = reader.Execute("set restricted value", std::chrono::seconds(3));
    ASSERT_TRUE(result.has_error());
    EXPECT_TRUE(result.error().IsPermissionDenied());
  }

  TEST_F(Cluster3NodesTest, ClientMtlsWriterCanExecuteAndQuery) {
    Client writer(addrs_, WriterClientOptions());
    ASSERT_TRUE(writer.Execute("set key value", std::chrono::seconds(3)).ok());
    EXPECT_TRUE(writer.Query("get key", std::chrono::seconds(3)).ok());
  }
  ```

- [ ] **Step 2: Run the focused integration filter and verify it fails**

  Run: `./build/release/tests/integration_tests --gtest_filter='Cluster3NodesTest.ClientMtls*'`

  Expected: build failure because the fixture and client credentials are not yet defined.

- [ ] **Step 3: Implement cluster authorization integration coverage**

  Reuse the existing mTLS cluster fixture and generated node addresses. Start the secure cluster only after all nodes have `client_ca_file` and the same static ACL. Assert stable error codes, not only strings. For the client-certificate Raft request, use the raw TLS framed helper to send a serialized `RequestVoteRequest`; assert no Raft handler response/mutation and that the connection is rejected. Ensure every test stops nodes and removes temporary data directories on failure paths.

- [ ] **Step 4: Add multi-raft node-wide ACL coverage**

  Start the existing two-group store fixture with client authentication. Use one writer identity to access both configured groups and a reader identity to show writes are denied in both. This proves the policy is node-wide and context survives group routing.

- [ ] **Step 5: Run focused integration suites**

  Run: `make release && ./build/release/tests/integration_tests --gtest_filter='Cluster3NodesTest.ClientMtls*:MultiRaft*ClientAuth*'`

  Expected: PASS. If sandbox execution fails with `bind: Operation not permitted`, re-run the same command with permitted local TCP execution and record the sandbox limitation separately from code results.

- [ ] **Step 6: Commit integration coverage**

  ```bash
  git add tests/integration/test_cluster_3nodes.cpp tests/integration/test_multi_raft_2groups.cpp
  git commit -m "test(auth): cover client mtls authorization"
  ```

### Task 6: Document the public configuration and verify the complete change

**Files:**
- Modify: `docs/public-api-guide.md:377-450, client API section`
- Modify: `docs/operations-guide.md:306-322`
- Modify: `doc/todo.md`

**Interfaces:**
- Consumes the final public APIs and verified deployment behavior from Tasks 1–5.
- Produces operator guidance for separate node/client CAs, URI SAN issuance, static ACLs, client TLS options, failure codes, and compatibility mode.

- [ ] **Step 1: Write documentation acceptance assertions**

  List the statements that must be true after implementation: one shared listener, separate node/client CA configuration, exact `rollingraft-client:<identity>` SAN, `READ_ONLY`/`READ_WRITE`, no wildcard/default rule, no retries for the two stable error codes, no private keys in the repository, and client auth disabled by default.

- [ ] **Step 2: Update public API and operations documentation**

  Add a copyable C++ server configuration and `ClientOptions` example:

  ```cpp
  config.client_auth_enabled = true;
  config.client_ca_file = "/run/secrets/client-ca.crt";
  config.client_authorizations = {
      {"analytics", ClientPermission::READ_ONLY},
      {"writer-service", ClientPermission::READ_WRITE},
  };

  ClientOptions client_options;
  client_options.tls_enabled = true;
  client_options.tls_cert_file = "/run/secrets/writer-service.crt";
  client_options.tls_key_file = "/run/secrets/writer-service.key";
  client_options.tls_ca_file = "/run/secrets/node-ca.crt";
  ```

  Replace the outdated statement that a strict mTLS endpoint accepts only node traffic. Mark the client-auth TODO complete only after the implementation and tests in Tasks 1–5 pass; leave future revocation, dynamic ACLs, and group-scoped authorization explicitly tracked.

- [ ] **Step 3: Run documentation and formatting checks**

  Run: `make format-check && git diff --check`

  Expected: PASS.

- [ ] **Step 4: Run the complete validation ladder**

  Run: `make test`

  Run: `make test-tsan`

  Run: `make werror`

  Run: `./scripts/docker-test.sh full`

  Expected: every command passes. Use a permitted local-TCP environment for socket suites if the sandbox blocks `bind`.

- [ ] **Step 5: Inspect repository hygiene and commit documentation**

  Run: `git status --short && git diff --check && git diff --cached --check`

  Expected: staged files are limited to the three documentation files; `third_party/leveldb` and `.codex/` remain unstaged and untouched.

  ```bash
  git add docs/public-api-guide.md docs/operations-guide.md doc/todo.md
  git commit -m "docs(auth): document client mtls access"
  ```

### Task 7: Final review and delivery preparation

**Files:**
- Review: all files changed by Tasks 1–6

**Interfaces:**
- Consumes all implementation and test results.
- Produces a clean, reviewable feature branch ready for a pull request.

- [ ] **Step 1: Review the feature diff against the accepted spec**

  Run: `git diff origin/main...HEAD --check && git diff origin/main...HEAD --stat`

  Verify each acceptance criterion has a corresponding implementation and test: certificate identity binding, denial before handlers, Raft/client principal separation, dedupe separation, terminal errors, multi-raft behavior, generated-only credentials, and compatibility-off behavior.

- [ ] **Step 2: Run targeted regression filters once more**

  Run: `./build/release/tests/unit_tests --gtest_filter='AsioSslContextFactoryTest.*:RaftNodeConfigValidateTest.*:ClientAuthorizationPolicyTest.*:ClientTest.*'`

  Run: `./build/release/tests/integration_tests --gtest_filter='Cluster3NodesTest.ClientMtls*:MultiRaft*ClientAuth*'`

  Expected: PASS.

- [ ] **Step 3: Inspect security-sensitive diff content**

  Run: `git diff origin/main...HEAD -- tests/generate_node_test_certs.sh include/rollingraft/client.h include/rollingraft/raft_node.h src/tls_identity.cpp src/asio_network_transport.cpp src/rpc_handlers.cpp`

  Confirm no private key, token, test secret, broad ACL default, identity derived from request JSON, or retry path for authorization errors is present.

- [ ] **Step 4: Commit any final test-only corrections and prepare review**

  If the review reveals a defect, add a focused regression test first, make the minimal correction, re-run the affected command, and commit with a conventional message such as `fix(auth): preserve client identity boundary`.

  Do not amend or stage unrelated local changes. After all checks pass, request code review before opening or merging a pull request.
