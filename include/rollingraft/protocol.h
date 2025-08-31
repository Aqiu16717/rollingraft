#pragma once

#include <string>

#include "rollingraft/rpc.h"
#include "rollingraft/status.h"

namespace rollingraft {

class Protocol {
 public:
  virtual ~Protocol() = default;

  virtual Status SerializeRequest(const RaftRequest& req,
                                  std::string& output) const = 0;
  virtual Status DeserializeRequest(const std::string& input,
                                    RaftRequest& req) = 0;

  virtual Status SerializeResponse(const RaftResponse& res,
                                   std::string& output) const = 0;
  virtual Status DeserializeResponse(const std::string& input,
                                     RaftResponse& res) = 0;
};

}  // namespace rollingraft
