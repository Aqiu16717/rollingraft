#include "json_protocol.h"
#include "rollingraft/status.h"
#include "nlohmann/json.hpp"

using namespace rollingraft;

Status JsonProtocol::ParseRequest(const std::string& input, RaftRequest& req) {
  auto j = nlohmann::json::parse(input);
  return Status();
}

Status JsonProtocol::SerializeResponse(const RaftResponse& res, std::string& output) {
  return Status();
}
