#include "rollingraft/protocol.h"

using namespace rollingraft;

class JsonProtocol : public Protocol {
public:
    Status SerializeRequest(const RaftRequest& req, std::string& output) const override;
    Status DeserializeRequest(const std::string& input, RaftRequest& req) override;
    
    Status SerializeResponse(const RaftResponse& res, std::string& output) const override;
    Status DeserializeResponse(const std::string& input, RaftResponse& res) override;
};
