#pragma once

#include <memory>
#include <string>
#include "rollingraft/command_handler.h"
#include "rollingraft/raft_node.h"

namespace rollingraft {

class RaftCommandHandler : public CommandHandler {
 public:
  RaftCommandHandler(std::shared_ptr<RaftNode> raft_node) : raft_node_(raft_node) {}
  void HandleCommand(const std::string& request, std::string& response) override;
 private:
  std::shared_ptr<RaftNode> raft_node_;
};

}  // namespace rollingraft
