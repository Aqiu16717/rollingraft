#pragma once

#include <string>
#include "rollingraft/rpc.h"
#include "rollingraft/status.h"

namespace rollingraft {

class Protocol {
 public:
  virtual ~Protocol() = default;    
  virtual Status ParseRequest(const std::string& input, RaftRequest& req) = 0;
  virtual Status SerializeResponse(const RaftResponse& res, std::string& output) = 0;
  virtual void Serialize() = 0;
  virtual void DeSerialize() = 0;
};

} // namespace rollingraft
