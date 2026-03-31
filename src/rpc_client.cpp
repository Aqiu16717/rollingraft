#include "rollingraft/rpc.h"
#include "rollingraft/status.h"

namespace rollingraft {

// Stub implementation - client RPC not yet fully implemented
Status RpcCall(const std::string& addr, const ClientRequest& req,
               ClientResponse& resp) {
  (void)addr;
  (void)req;
  (void)resp;
  return Status::Error("RpcCall not yet implemented");
}

}  // namespace rollingraft
