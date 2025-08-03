#include "rollingraft/protocol.h"

using namespace rollingraft;

class JsonProtocol : public Protocol {
public:
  Status ParseRequest(const std::string& input, RaftRequest& req) override;
  Status SerializeResponse(const RaftResponse& res, std::string& output) override;
};
