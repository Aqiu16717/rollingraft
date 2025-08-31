#include "raft_command_handler.h"
#include "rollingraft/status.h"
using namespace rollingraft;

Status RaftCommandHandler::HandleCommand(const std::string& request,
                                         std::string& response) {
  // Deserialize the request
  RaftRequest raft_req(RaftMessageType::KInvalid);
  Status status = protocol_->DeserializeRequest(request, raft_req);
  if (!status.ok()) {
    response = status.ToString();
    return status;
  }

  // Process the request according to its type
  RaftResponse raft_res(RaftMessageType::KInvalid);
  switch (raft_req.type_) {
    case RaftMessageType::KRequestVoteRequest: {
      status = raft_node_->RequestVote(
          static_cast<const RequestVoteRequest&>(raft_req),
          static_cast<RequestVoteResponse&>(raft_res));
      break;
    }
    case RaftMessageType::KAppendEntriesRequest: {
      status = raft_node_->AppendEntries(
          static_cast<const AppendEntriesRequest&>(raft_req),
          static_cast<AppendEntriesResponse&>(raft_res));
      break;
    }
    case RaftMessageType::KInstallSnapshotRequest: {
      status = raft_node_->InstallSnapshot(
          static_cast<const InstallSnapshotRequest&>(raft_req),
          static_cast<InstallSnapshotResponse&>(raft_res));
      break;
    }
    default: {
      status = Status::ProtocolError("Unknown request type");
      break;
    }
  }

  if (!status.ok()) {
    response = status.ToString();
    return status;
  }

  // Serialize the response
  status = protocol_->SerializeResponse(raft_res, response);
  if (!status.ok()) {
    response = status.ToString();
    return status;
  }

  return Status::OK();
}
