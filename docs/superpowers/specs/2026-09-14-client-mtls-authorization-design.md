# Client mTLS Authentication and Authorization — Design

- Date: 2026-09-14
- Status: approved design (written spec review pending)

## Goal

Authenticate application clients at the Raft RPC boundary and authorize each
authenticated identity for read-only or read-write access. The transport, not
the request body, is the source of identity. Existing deployments remain
compatible because client authentication is opt-in.

This closes the highest-priority production-readiness gap in `doc/todo.md`:
anyone who can currently reach a node can send a `ClientRequest`.

## Non-goals

Phase one does not include:

- bearer tokens or passwords;
- dynamic ACL reload;
- certificate revocation or automated rotation;
- authorization that differs by Raft group, command, or key prefix;
- TLS termination on a separate application port;
- authentication for the metrics/admin HTTP server, which keeps its existing
  bearer-token mechanism.

## Current constraints

- Raft and application requests share one framed TCP listener.
- Node mTLS certificates use URI SAN `rollingraft-node:<node_id>`.
- The high-level `Client` uses a separate synchronous ASIO RPC path and does
  not currently support TLS.
- `NetworkTransport` is a public, pluggable interface. Existing custom
  transports and tests implement the legacy `NodeId` handler signature.
- `ClientRequest.client_id` is a caller-supplied deduplication key. It is not
  an authenticated identity and must remain independent of authorization.
- Multi-raft routes requests by `group_id` over the same node-level transport.

## Approaches considered

### 1. Shared listener with typed certificate identities — selected

The existing listener trusts separate node and client certificate authorities.
After the TLS handshake it classifies the peer as a node or client from a
strict URI SAN. A typed, transport-authenticated context accompanies the RPC
to the dispatcher.

This reuses framing, connection lifecycle, and multi-raft routing while
preventing request fields from claiming an identity. The main cost is a
careful extension of the transport handler interface.

### 2. Separate client listener

A second port and TLS context would create a clear network boundary, but it
would duplicate listener lifecycle, framing, configuration, and multi-raft
routing. It would also expand operational surface area before the project has
a need for independent client endpoint scaling.

### 3. Bearer tokens in `ClientRequest`

Tokens would be simpler to serialize, but they are replayable application
secrets, require a storage and rotation model, and are not bound to the TLS
channel. The project already has certificate identity machinery, so this
option adds a second trust model without a present requirement.

## Identity model

### Certificate identities

- Node certificate: URI SAN `rollingraft-node:<node_id>`
- Client certificate: URI SAN `rollingraft-client:<identity>`

`<identity>` is an ASCII token matching
`[A-Za-z0-9][A-Za-z0-9._-]{0,127}`. Authorization compares the complete
identity byte-for-byte and is case-sensitive. This grammar fits the URI SAN's
IA5String representation and excludes delimiters and control characters.

A peer certificate must contain exactly one recognized RollingRaft identity.
Multiple copies, conflicting identities, both a node and a client identity,
an empty client identity, or a malformed node identifier cause authentication
failure. Unrelated SAN entries are ignored. CA verification must succeed
before SAN classification is trusted.

The node/server certificate remains the certificate presented by the shared
listener. A high-level client trusts the node CA when verifying servers and
presents a certificate signed by the separately configured client CA. A
client accepts a server certificate only if it chains to its configured CA
and has one valid `rollingraft-node:<node_id>` URI SAN.

### Typed request context

Transport code constructs an immutable context for each inbound request:

```cpp
enum class RpcPeerKind { ANONYMOUS, NODE, CLIENT };

struct RpcPeerIdentity {
  RpcPeerKind kind = RpcPeerKind::ANONYMOUS;
  NodeId node_id = -1;
  std::string client_identity;
};

struct RpcRequestContext {
  RpcPeerIdentity peer;
};
```

Only transport-authenticated certificate data populates `NODE` or `CLIENT`.
No field in serialized RPC data can set or override this context. A built-in
plaintext connection is `ANONYMOUS`; a legacy custom transport cannot enable
client authentication until it implements the authenticated context API.

## Configuration and public API

### Authorization rules

```cpp
enum class ClientPermission { READ_ONLY, READ_WRITE };

struct ClientAuthorizationRule {
  std::string identity;
  ClientPermission permission = ClientPermission::READ_ONLY;
};
```

`RaftNodeConfig` and `RaftStoreConfig` gain:

```cpp
bool client_auth_enabled = false;
std::string client_ca_file;
std::vector<ClientAuthorizationRule> client_authorizations;
```

Rules apply node-wide, including every group in a `RaftStore`. Duplicate or
empty identities are invalid configuration. An authenticated client not in
the rules is denied; there is no wildcard rule and no implicit default.

When `client_auth_enabled` is true:

- `tls_enabled` and `tls_mutual_auth` must both be true;
- `client_ca_file` must be non-empty and load successfully;
- `client_authorizations` must be non-empty;
- the configured transport must support authenticated peer contexts.

An incomplete configuration returns `CONFIG_INVALID` during initialization,
before the listener starts. Requiring node mTLS as a prerequisite avoids a
mode in which clients are authenticated while consensus traffic is not.

When `client_auth_enabled` is false, existing node TLS/mTLS and plaintext
behavior is unchanged, and anonymous `ClientRequest` traffic remains allowed
for backward compatibility.

### High-level client TLS

`ClientOptions` gains:

```cpp
bool tls_enabled = false;
std::string tls_cert_file;
std::string tls_key_file;
std::string tls_ca_file;
```

Enabling client TLS requires all three paths. The certificate must contain
exactly one valid `rollingraft-client:<identity>` URI SAN. Configuration and
certificate validation happen when `Client` is constructed; because the
constructors cannot return `Status`, invalid configuration is recorded in the
implementation and every operation returns that stable initialization error
without attempting a connection. This preserves the existing constructor API.

When client TLS is disabled, the existing plaintext RPC path is retained.

## Transport interface compatibility

The existing `RpcRequestHandler`, `GroupRequestHandler`, and pure virtual
`NetworkTransport::Initialize` remain source-compatible. The interface adds:

- authenticated single-group and group-scoped handler types that receive
  `RpcRequestContext`;
- `InitializeAuthenticated`, with a default
  `UNSUPPORTED` result;
- `SupportsAuthenticatedPeerIdentity`, whose default is false;
- `SetAuthenticatedGroupRequestHandler`, whose default is a no-op.

The built-in ASIO transport implements the new entry points and routes all
inbound messages through them when client authentication is enabled. Existing
custom transports continue to compile and work when client authentication is
off. If it is enabled with a transport that does not advertise support,
initialization fails with `CONFIG_INVALID`; RollingRaft never silently treats
an unverified `NodeId` as an authenticated identity.

The ASIO server trust store loads both the node CA and client CA in client-auth
mode. Outbound Raft connections continue to use the node certificate, key,
and node CA. High-level application clients use their own client certificate
and key while trusting the node CA supplied through `ClientOptions`.

## Authorization and dispatch

The dispatcher classifies the request type and applies these rules before it
invokes a state-mutating or state-reading handler:

| Peer identity | Raft protocol request | Read-only `ClientRequest` | Write `ClientRequest` |
|---|---:|---:|---:|
| authenticated node | allow, subject to existing membership/sender checks | reject | reject |
| allowlisted `READ_ONLY` client | reject | allow | `PERMISSION_DENIED` |
| allowlisted `READ_WRITE` client | reject | allow | allow |
| unknown authenticated client | reject | `PERMISSION_DENIED` | `PERMISSION_DENIED` |
| anonymous peer, auth enabled | reject | `UNAUTHENTICATED` | `UNAUTHENTICATED` |

An authenticated node sending `ClientRequest` receives `UNAUTHENTICATED`
because it did not present a client principal. A client certificate sending a
Raft protocol request is rejected before the Raft handler runs; the transport
closes that request/connection without synthesizing a message-specific Raft
response. Existing node membership and claimed-sender checks remain in force.

`READ_ONLY` authorizes only `ClientRequest.read_only == true` (`Client::Query`).
`READ_WRITE` authorizes both query and execute. `client_id` and `seq` continue
to serve deduplication only and do not need to match certificate identity.

For multi-raft, the same request context is retained when the transport routes
by `group_id`. Authorization is node-wide and must pass before the selected
group's client handler executes. The phase-one ACL cannot reveal or vary by
group.

## Error contract and retry behavior

`ClientResponse` gains an optional `error_code` string. Both protocol
implementations serialize it for failed responses and accept older responses
that omit it. Authentication and authorization failures use stable codes:

- `UNAUTHENTICATED`: no acceptable client principal was presented;
- `PERMISSION_DENIED`: a valid client principal lacks the requested access.

Human-readable `error` text remains available but is not used for policy.
The high-level client maps `error_code` and `error` to `Status`. It must not
retry `UNAUTHENTICATED` or `PERMISSION_DENIED`, must not discard the leader
cache because of those errors, and must return the first such result to the
caller. Existing retry and leader-redirection behavior remains unchanged for
transport failures, timeouts, and not-leader responses.

TLS handshake failures cannot carry a `ClientResponse`; the high-level client
returns a non-retryable `UNAUTHENTICATED` status when certificate verification
or peer identity validation fails. Ordinary connection failures remain
retryable according to existing policy.

## State, concurrency, and logging

Authorization rules are validated once and stored as immutable node-level
state. Request checks perform read-only lookups and introduce no new mutex in
the Raft lock hierarchy. Dynamic rule reload is deliberately excluded.

Logs may include the authenticated identity and denial reason, but never
certificate private-key material or full certificate contents. Authentication
failures are logged at warning level; expected authorization denials do not
log request command contents.

## Test strategy

### Unit tests

- Parse valid node and client URI SAN identities.
- Reject missing, duplicate, conflicting, mixed-kind, malformed, empty,
  non-ASCII, and oversized RollingRaft identities.
- Validate every client-auth configuration invariant for `RaftNodeConfig`,
  `RaftStoreConfig`, and `ClientOptions`.
- Verify the authorization matrix for anonymous, node, unknown client,
  `READ_ONLY`, and `READ_WRITE` identities.
- Verify `ClientResponse.error_code` serialization and compatibility when it
  is absent.
- Verify permission failures are non-retryable and do not invalidate the
  leader cache.
- Verify a custom transport without authenticated-context support is rejected
  only when client authentication is enabled.

### Integration tests

Use build-generated certificates and private keys in the build directory,
with separate node and client CAs. No private key fixture is committed.

- `READ_WRITE` client can query and execute.
- `READ_ONLY` client can query but receives `PERMISSION_DENIED` on execute.
- Client identity signed by the client CA but absent from the ACL is denied.
- Client certificate signed by the wrong CA fails authentication.
- Node certificate cannot impersonate an application client.
- Client certificate cannot send vote, append, snapshot, pre-vote, or
  forwarded ReadIndex traffic to a Raft handler.
- Client authentication disabled preserves existing plaintext client tests.
- Multi-raft applies one node-level client identity consistently to two groups.

### Verification

- `make format-check`
- focused unit and integration tests for identity, policy, transport, client,
  and multi-raft behavior
- `make test`
- `make test-tsan`
- `make werror`
- `./scripts/docker-test.sh full`
- all GitHub Actions jobs pass

Socket-based tests may require execution outside a restricted sandbox that
forbids local TCP binds; such an environment failure must not be reported as a
code regression.

## Security properties

- Identity is channel-bound to a CA-verified client certificate.
- Request-controlled deduplication fields cannot grant permissions.
- Node and client principal types cannot impersonate each other even when
  their trust roots are both loaded by the listener.
- Authorization is deny-by-default whenever client authentication is enabled.
- No private keys are stored in source control.
- Disabling the feature is an explicit compatibility mode, not a claim of
  secure client access.

## Acceptance criteria

1. A configured client identity is authenticated from its certificate URI SAN
   and can perform only the operations allowed by its static rule.
2. Anonymous, wrong-CA, wrong-principal-type, and unlisted clients cannot reach
   a client handler when authentication is enabled.
3. A client certificate cannot reach a Raft protocol handler, and existing
   node membership and sender-identity checks remain effective.
4. `client_id` remains a deduplication key and cannot influence authorization.
5. Permission failures return stable codes and stop high-level client retries.
6. Single-group, multi-raft, custom-transport compatibility, and auth-disabled
   behavior are covered by tests.
7. Generated test credentials remain outside source control, and the full
   local and hosted verification matrix passes.
