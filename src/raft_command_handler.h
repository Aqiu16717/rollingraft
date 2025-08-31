#pragma once

#include <memory>
#include <string>

#include "rollingraft/command_handler.h"
#include "rollingraft/protocol.h"
#include "rollingraft/raft_node.h"
#include "rollingraft/status.h"

namespace rollingraft {

class RaftCommandHandler : public CommandHandler {
 public:
  RaftCommandHandler(std::shared_ptr<RaftNode> raft_node,
                     std::shared_ptr<Protocol> protocol)
      : raft_node_(raft_node), protocol_(protocol) {}

  Status HandleCommand(const std::string& request,
                       std::string& response) override;

 private:
  std::shared_ptr<RaftNode> raft_node_;
  std::shared_ptr<Protocol> protocol_;
};

}  // namespace rollingraft
