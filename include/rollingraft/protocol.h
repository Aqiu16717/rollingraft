/**
 * @file protocol.h
 * @brief RPC serialization protocol interface
 *
 * Abstract interface for serializing/deserializing Raft RPC messages.
 * Implementations can use different formats (JSON, Protocol Buffers, etc.).
 */

#pragma once

#include <string>

#include "rollingraft/rpc.h"
#include "rollingraft/status.h"

namespace rollingraft {

/**
 * Abstract serialization protocol interface.
 *
 * Implement this interface to provide custom serialization format.
 * The protocol is responsible for converting RPC messages to/from
 * wire format for network transmission.
 */
class Protocol {
 public:
  virtual ~Protocol() = default;

  /**
   * Serialize a request message.
   *
   * @param req Request to serialize
   * @param output Output buffer for serialized data
   * @return Status::OK() on success
   */
  virtual Status SerializeRequest(const RaftRequest& req, std::string& output) const = 0;

  /**
   * Deserialize a request message.
   *
   * @param input Serialized data
   * @param req Output request object
   * @return Status::OK() on success
   */
  virtual Status DeserializeRequest(const std::string& input, RaftRequest& req) = 0;

  /**
   * Serialize a response message.
   *
   * @param res Response to serialize
   * @param output Output buffer for serialized data
   * @return Status::OK() on success
   */
  virtual Status SerializeResponse(const RaftResponse& res, std::string& output) const = 0;

  /**
   * Deserialize a response message.
   *
   * @param input Serialized data
   * @param res Output response object
   * @return Status::OK() on success
   */
  virtual Status DeserializeResponse(const std::string& input, RaftResponse& res) = 0;
};

}  // namespace rollingraft
