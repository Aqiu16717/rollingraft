/**
 * @file rpc_client.cpp
 * @brief Client-side RPC call implementation
 *
 * Implements synchronous RPC client for sending commands
 * to the Raft cluster.
 */

#include "rollingraft/rpc.h"

#include <arpa/inet.h>

#include <asio.hpp>
#include <chrono>
#include <cstring>
#include <string>

#include "rollingraft/logger.h"
#include "rollingraft/status.h"

#include "nlohmann/json.hpp"

namespace rollingraft {

// Helper to serialize ClientRequest to JSON
static std::string SerializeClientRequest(const ClientRequest& req) {
  nlohmann::json j;
  j["type"] = static_cast<int>(RaftMessageType::KClientRequest);
  j["command"] = req.command;
  j["client_id"] = req.client_id;
  j["seq"] = req.seq;
  j["read_only"] = req.read_only;
  return j.dump();
}

// Helper to deserialize ClientResponse from JSON
static bool DeserializeClientResponse(const std::string& data,
                                       ClientResponse& resp) {
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

Status RpcCall(const std::string& addr, const ClientRequest& req,
               ClientResponse& resp) {
  try {
    // Parse address
    auto colon_pos = addr.find(':');
    if (colon_pos == std::string::npos) {
      return Status::Error("Invalid address format, expected host:port");
    }

    std::string host = addr.substr(0, colon_pos);
    std::string port_str = addr.substr(colon_pos + 1);
    uint16_t port = static_cast<uint16_t>(std::stoi(port_str));

    // Setup ASIO
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    asio::ip::tcp::resolver resolver(io_context);

    // Resolve and connect with timeout
    auto endpoints = resolver.resolve(host, std::to_string(port));

    // Set connection timeout
    std::error_code connect_ec;

    asio::steady_timer timer(io_context);
    timer.expires_after(std::chrono::seconds(5));
    timer.wait(connect_ec);

    asio::connect(socket, endpoints, connect_ec);
    if (connect_ec) {
      return Status::Error("Failed to connect to " + addr + ": " +
                           connect_ec.message());
    }

    // Serialize request
    std::string request_data = SerializeClientRequest(req);

    // Send length-prefixed message
    uint32_t length = htonl(static_cast<uint32_t>(request_data.size()));
    asio::write(socket, asio::buffer(&length, sizeof(length)));
    asio::write(socket, asio::buffer(request_data));

    // Read response length
    uint32_t response_length_net;
    size_t bytes_read =
        asio::read(socket, asio::buffer(&response_length_net, sizeof(length)));
    if (bytes_read != sizeof(length)) {
      return Status::Error("Failed to read response length");
    }

    uint32_t response_length = ntohl(response_length_net);
    if (response_length > 10 * 1024 * 1024) {  // 10MB limit
      return Status::Error("Response too large");
    }

    // Read response data
    std::string response_data;
    response_data.resize(response_length);
    bytes_read =
        asio::read(socket, asio::buffer(&response_data[0], response_length));
    if (bytes_read != response_length) {
      return Status::Error("Failed to read complete response");
    }

    // Deserialize response
    if (!DeserializeClientResponse(response_data, resp)) {
      return Status::Error("Failed to deserialize response");
    }

    // Gracefully close connection
    std::error_code ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    socket.close(ec);

    return Status::OK();

  } catch (const std::exception& e) {
    return Status::Error(std::string("RPC call failed: ") + e.what());
  }
}

}  // namespace rollingraft
