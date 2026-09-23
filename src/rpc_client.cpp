/**
 * @file rpc_client.cpp
 * @brief Client-side RPC call implementation
 *
 * Implements synchronous RPC client for sending commands
 * to the Raft cluster.
 */

#include <chrono>
#include <cstring>
#include <memory>
#include <string>

#include <asio.hpp>

#include "rollingraft/client.h"
#include "rollingraft/logger.h"
#include "rollingraft/rpc.h"
#include "rollingraft/status.h"

#include "nlohmann/json.hpp"
#include "tls_identity.h"
#include <arpa/inet.h>
#include <asio/ssl.hpp>
#include <openssl/x509.h>

namespace rollingraft {

// Helper to serialize ClientRequest to JSON
static std::string SerializeClientRequest(const ClientRequest& req, uint64_t group_id = 0) {
  nlohmann::json j;
  j["type"] = static_cast<int>(RaftMessageType::KClientRequest);
  j["command"] = req.command;
  j["client_id"] = req.client_id;
  j["seq"] = req.seq;
  j["read_only"] = req.read_only;
  j["group_id"] = group_id;
  return j.dump();
}

// Helper to deserialize ClientResponse from JSON
static bool DeserializeClientResponse(const std::string& data, ClientResponse& resp) {
  try {
    auto j = nlohmann::json::parse(data);

    if (!j.contains("success")) {
      return false;
    }

    resp.success = j["success"];
    if (j.contains("response")) {
      resp.response = j["response"];
    }
    if (j.contains("error")) {
      resp.error = j["error"];
    }
    resp.error_code.clear();
    if (j.contains("error_code")) {
      resp.error_code = j["error_code"];
    }
    if (j.contains("last_applied_index")) {
      resp.last_applied_index = j["last_applied_index"];
    }
    if (j.contains("leader_id")) {
      resp.leader_id = j["leader_id"];
    }
    if (j.contains("leader_addr")) {
      resp.leader_addr = j["leader_addr"];
    }
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

// Helper to serialize ReadIndexRequest to JSON
static std::string SerializeReadIndexRequest(const ReadIndexRequest& req) {
  nlohmann::json j;
  j["type"] = static_cast<int>(RaftMessageType::KReadIndexRequest);
  j["correlation_id"] = req.correlation_id_;
  return j.dump();
}

// Helper to deserialize ReadIndexResponse from JSON
static bool DeserializeReadIndexResponse(const std::string& data, ReadIndexResponse& resp) {
  try {
    auto j = nlohmann::json::parse(data);
    if (!j.contains("term") || !j.contains("read_index") || !j.contains("leader_valid")) {
      return false;
    }
    resp.term_ = j["term"];
    resp.read_index_ = j["read_index"];
    resp.leader_valid_ = j["leader_valid"];
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

// Generic synchronous RPC helper using ASIO
template <typename Stream>
Status ExchangeRpc(Stream& stream, const std::string& request_data, std::string& response_data) {
  uint32_t length = htonl(static_cast<uint32_t>(request_data.size()));
  asio::write(stream, asio::buffer(&length, sizeof(length)));
  asio::write(stream, asio::buffer(request_data));

  uint32_t response_length_net;
  size_t bytes_read = asio::read(stream, asio::buffer(&response_length_net, sizeof(length)));
  if (bytes_read != sizeof(length)) {
    return Status::Error("Failed to read response length");
  }

  uint32_t response_length = ntohl(response_length_net);
  if (response_length > 10 * 1024 * 1024) {
    return Status::Error("Response too large");
  }

  response_data.resize(response_length);
  bytes_read = asio::read(stream, asio::buffer(response_data.data(), response_length));
  if (bytes_read != response_length) {
    return Status::Error("Failed to read complete response");
  }
  return Status::OK();
}

static Status ConnectSocket(const std::string& addr, std::chrono::milliseconds timeout,
                            asio::io_context& io_context, asio::ip::tcp::socket& socket) {
  auto colon_pos = addr.find(':');
  if (colon_pos == std::string::npos) {
    return Status::Error("Invalid address format, expected host:port");
  }
  std::string host = addr.substr(0, colon_pos);
  std::string port_str = addr.substr(colon_pos + 1);
  uint16_t port = static_cast<uint16_t>(std::stoi(port_str));
  asio::ip::tcp::resolver resolver(io_context);
  auto endpoints = resolver.resolve(host, std::to_string(port));

  std::error_code connect_ec;
  asio::steady_timer timer(io_context);
  bool connect_done = false;
  timer.expires_after(timeout);
  timer.async_wait([&](std::error_code ec) {
    if (!ec && !connect_done) {
      socket.close();
    }
  });
  asio::async_connect(socket, endpoints, [&](std::error_code ec, const asio::ip::tcp::endpoint&) {
    connect_done = true;
    connect_ec = ec;
    timer.cancel();
  });
  io_context.run_for(timeout);
  if (!connect_done) {
    return Status::Error("Timeout connecting to " + addr);
  }
  if (connect_ec) {
    return Status::Error("Failed to connect to " + addr + ": " + connect_ec.message());
  }
  return Status::OK();
}

static Status DoTlsRpcCall(const std::string& addr, const std::string& request_data,
                           std::string& response_data, std::chrono::milliseconds timeout,
                           const ClientOptions& options) {
  asio::io_context io_context;
  asio::ip::tcp::socket socket(io_context);
  auto status = ConnectSocket(addr, timeout, io_context, socket);
  if (!status.ok()) {
    return status;
  }

  try {
    asio::ssl::context tls_context(asio::ssl::context::tls_client);
    tls_context.set_verify_mode(asio::ssl::verify_peer);
    tls_context.load_verify_file(options.tls_ca_file);
    tls_context.use_certificate_chain_file(options.tls_cert_file);
    tls_context.use_private_key_file(options.tls_key_file, asio::ssl::context::pem);

    asio::ssl::stream<asio::ip::tcp::socket> stream(std::move(socket), tls_context);
    stream.handshake(asio::ssl::stream_base::client);

    X509* certificate = SSL_get1_peer_certificate(stream.native_handle());
    if (certificate == nullptr) {
      return Status::Error("TLS_IDENTITY_MISSING", "Server did not present a certificate");
    }
    std::unique_ptr<X509, decltype(&X509_free)> certificate_guard(certificate, X509_free);
    RpcPeerIdentity identity;
    status = ExtractCertificatePeerIdentity(certificate, identity);
    if (!status.ok()) {
      return status;
    }
    if (identity.kind != RpcPeerKind::NODE) {
      return Status::Error("TLS_IDENTITY_MISMATCH",
                           "Server certificate does not contain a node identity");
    }

    status = ExchangeRpc(stream, request_data, response_data);
    std::error_code shutdown_ec;
    stream.shutdown(shutdown_ec);
    return status;
  } catch (const std::exception& e) {
    return Status::Error(std::string("TLS RPC call failed: ") + e.what());
  }
}

static Status DoRpcCall(const std::string& addr, const std::string& request_data,
                        std::string& response_data, std::chrono::milliseconds timeout) {
  try {
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    auto status = ConnectSocket(addr, timeout, io_context, socket);
    if (!status.ok()) {
      return status;
    }
    status = ExchangeRpc(socket, request_data, response_data);

    std::error_code ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    socket.close(ec);
    return status;

  } catch (const std::exception& e) {
    return Status::Error(std::string("RPC call failed: ") + e.what());
  }
}

Status RpcCall(const std::string& addr, const ReadIndexRequest& req, ReadIndexResponse& resp,
               std::chrono::milliseconds timeout) {
  std::string request_data = SerializeReadIndexRequest(req);
  std::string response_data;
  auto status = DoRpcCall(addr, request_data, response_data, timeout);
  if (!status.ok()) {
    return status;
  }
  if (!DeserializeReadIndexResponse(response_data, resp)) {
    return Status::Error("Failed to deserialize ReadIndexResponse");
  }
  return Status::OK();
}

Status RpcCall(const std::string& addr, const ClientRequest& req, ClientResponse& resp,
               std::chrono::milliseconds timeout) {
  std::string request_data = SerializeClientRequest(req);
  std::string response_data;
  auto status = DoRpcCall(addr, request_data, response_data, timeout);
  if (!status.ok()) {
    return status;
  }
  if (!DeserializeClientResponse(response_data, resp)) {
    return Status::Error("Failed to deserialize response");
  }
  return Status::OK();
}

Status RpcCall(const std::string& addr, const ClientRequest& req, ClientResponse& resp,
               std::chrono::milliseconds timeout, const ClientOptions& options) {
  std::string request_data = SerializeClientRequest(req, options.group_id);
  std::string response_data;
  auto status = options.tls_enabled
                    ? DoTlsRpcCall(addr, request_data, response_data, timeout, options)
                    : DoRpcCall(addr, request_data, response_data, timeout);
  if (!status.ok()) {
    return status;
  }
  if (!DeserializeClientResponse(response_data, resp)) {
    return Status::Error("Failed to deserialize response");
  }
  return Status::OK();
}

}  // namespace rollingraft
