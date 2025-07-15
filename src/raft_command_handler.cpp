#include "raft_command_handler.h"
using namespace rollingraft;


void RaftCommandHandler::HandleCommand(const std::string& request,
                     std::string& response) {
  protocol_->DeSerialize();
}
