#include "json_protocol.h"

#include "rollingraft/rpc.h"
#include "rollingraft/status.h"

#include "nlohmann/json.hpp"

using namespace rollingraft;

RaftMessageType IntToMessageType(int type_id) {
  return static_cast<RaftMessageType>(type_id);
}

Status JsonProtocol::SerializeRequest(const RaftRequest& req,
                                      std::string& output) const {
  try {
    nlohmann::json j;
    switch (req.type_) {
      case RaftMessageType::KRequestVoteRequest: {
        const RequestVoteRequest& vote_req =
            static_cast<const RequestVoteRequest&>(req);
        j["type"] = static_cast<int>(req.type_);
        j["term"] = vote_req.term_;
        j["candidate_id"] = vote_req.candidate_id_;
        j["last_log_index"] = vote_req.last_log_index_;
        j["last_log_term"] = vote_req.last_log_term_;
        break;
      }

      case RaftMessageType::KAppendEntriesRequest: {
        const AppendEntriesRequest& append_req =
            static_cast<const AppendEntriesRequest&>(req);
        j["type"] = static_cast<int>(req.type_);
        j["term"] = append_req.term_;
        j["leader_id"] = append_req.leader_id_;
        j["prev_log_index"] = append_req.prev_log_index_;
        j["prev_log_term"] = append_req.prev_log_term_;
        j["leader_commit"] = append_req.leader_commit_;
        break;
      }

      case RaftMessageType::KInstallSnapshotRequest: {
        const InstallSnapshotRequest& snapshot_req =
            static_cast<const InstallSnapshotRequest&>(req);
        j["type"] = static_cast<int>(req.type_);
        j["term"] = snapshot_req.term_;
        j["leader_id"] = snapshot_req.leader_id_;
        j["last_included_index"] = snapshot_req.last_included_index_;
        j["last_included_term"] = snapshot_req.last_included_term_;
        j["offset"] = snapshot_req.offset_;
        j["data"] =
            std::string(snapshot_req.data_.begin(), snapshot_req.data_.end());
        j["done"] = snapshot_req.done_;
        break;
      }

      default:
        return Status::ProtocolError("Unknown request type");
    }

    output = j.dump();
    return Status::OK();

  } catch (const std::exception& e) {
    return Status::SerializeError("Failed to serialize request: " +
                                  std::string(e.what()));
  }
}

Status JsonProtocol::DeserializeRequest(const std::string& input,
                                        RaftRequest& req) {
  try {
    auto j = nlohmann::json::parse(input);

    if (!j.contains("type")) {
      return Status::DeSerializeError("Missing 'type' field in request");
    }

    int type_id = j["type"];
    RaftMessageType message_type = IntToMessageType(type_id);

    if (message_type == RaftMessageType::KInvalid) {
      return Status::ProtocolError("Unknown request type: " +
                                   std::to_string(type_id));
    }

    switch (message_type) {
      case RaftMessageType::KRequestVoteRequest: {
        if (!j.contains("term") || !j.contains("candidate_id") ||
            !j.contains("last_log_index") || !j.contains("last_log_term")) {
          return Status::DeSerializeError("Missing required fields for RequestVote");
        }

        uint32_t term = j["term"];
        uint32_t candidate_id = j["candidate_id"];
        uint32_t last_log_index = j["last_log_index"];
        uint32_t last_log_term = j["last_log_term"];

        RequestVoteRequest* vote_req = new RequestVoteRequest(
            term, candidate_id, last_log_index, last_log_term);
        req = *vote_req;
        delete vote_req;
        break;
      }

      case RaftMessageType::KAppendEntriesRequest: {
        if (!j.contains("term") || !j.contains("leader_id") ||
            !j.contains("prev_log_index") || !j.contains("prev_log_term") ||
            !j.contains("leader_commit")) {
          return Status::DeSerializeError(
              "Missing required fields for AppendEntries");
        }

        uint32_t term = j["term"];
        uint32_t leader_id = j["leader_id"];
        uint32_t prev_log_index = j["prev_log_index"];
        uint32_t prev_log_term = j["prev_log_term"];
        uint32_t leader_commit = j["leader_commit"];

        // 创建AppendEntriesRequest对象（暂时不处理entries）
        RaftLog entries;
        AppendEntriesRequest* append_req =
            new AppendEntriesRequest(term, leader_id, prev_log_index,
                                     prev_log_term, entries, leader_commit);
        req = *append_req;
        delete append_req;
        break;
      }

      case RaftMessageType::KInstallSnapshotRequest: {
        if (!j.contains("term") || !j.contains("leader_id") ||
            !j.contains("last_included_index") ||
            !j.contains("last_included_term") || !j.contains("offset") ||
            !j.contains("data") || !j.contains("done")) {
          return Status::DeSerializeError(
              "Missing required fields for InstallSnapshot");
        }

        uint32_t term = j["term"];
        uint32_t leader_id = j["leader_id"];
        uint32_t last_included_index = j["last_included_index"];
        uint32_t last_included_term = j["last_included_term"];
        uint32_t offset = j["offset"];
        bool done = j["done"];

        // 解析data字段
        std::string data_str = j["data"];
        std::vector<char> data(data_str.begin(), data_str.end());

        InstallSnapshotRequest* snapshot_req =
            new InstallSnapshotRequest(term, leader_id, last_included_index,
                                       last_included_term, offset, data, done);
        req = *snapshot_req;
        delete snapshot_req;
        break;
      }

      default:
        return Status::ProtocolError("Unsupported request type");
    }

    return Status::OK();

  } catch (const nlohmann::json::parse_error& e) {
    return Status::DeSerializeError("JSON parse error: " + std::string(e.what()));
  } catch (const std::exception& e) {
    return Status::DeSerializeError("Deserialization error: " +
                              std::string(e.what()));
  }
}

Status JsonProtocol::SerializeResponse(const RaftResponse& res,
                                       std::string& output) const {
  try {
    nlohmann::json j;

    switch (res.type_) {
      case RaftMessageType::KRequestVoteResponse: {
        const RequestVoteResponse& vote_res =
            static_cast<const RequestVoteResponse&>(res);
        j["type"] = static_cast<int>(res.type_);
        j["term"] = vote_res.term_;
        j["vote_granted"] = vote_res.vote_granted_;
        break;
      }

      case RaftMessageType::KAppendEntriesResponse: {
        const AppendEntriesResponse& append_res =
            static_cast<const AppendEntriesResponse&>(res);
        j["type"] = static_cast<int>(res.type_);
        j["term"] = append_res.term_;
        j["success"] = append_res.success_;
        break;
      }

      case RaftMessageType::KInstallSnapshowResponse: {
        const InstallSnapshotResponse& snapshot_res =
            static_cast<const InstallSnapshotResponse&>(res);
        j["type"] = static_cast<int>(res.type_);
        j["term"] = snapshot_res.term_;
        break;
      }

      default:
        return Status::ProtocolError("Unknown response type");
    }

    output = j.dump();
    return Status::OK();

  } catch (const std::exception& e) {
    return Status::SerializeError("Failed to serialize response: " +
                                  std::string(e.what()));
  }
}

Status JsonProtocol::DeserializeResponse(const std::string& input,
                                         RaftResponse& res) {
  try {
    auto j = nlohmann::json::parse(input);

    if (!j.contains("type")) {
      return Status::DeSerializeError("Missing 'type' field in response");
    }

    int type_id = j["type"];
    RaftMessageType message_type = IntToMessageType(type_id);

    if (message_type == RaftMessageType::KInvalid) {
      return Status::ProtocolError("Unknown response type: " +
                                   std::to_string(type_id));
    }

    switch (message_type) {
      case RaftMessageType::KRequestVoteResponse: {
        if (!j.contains("term") || !j.contains("vote_granted")) {
          return Status::DeSerializeError(
              "Missing required fields for RequestVoteResponse");
        }

        uint32_t term = j["term"];
        bool vote_granted = j["vote_granted"];

        RequestVoteResponse* vote_res =
            new RequestVoteResponse(term, vote_granted);
        res = *vote_res;
        delete vote_res;
        break;
      }

      case RaftMessageType::KAppendEntriesResponse: {
        if (!j.contains("term") || !j.contains("success")) {
          return Status::DeSerializeError(
              "Missing required fields for AppendEntriesResponse");
        }

        uint32_t term = j["term"];
        bool success = j["success"];

        AppendEntriesResponse* append_res =
            new AppendEntriesResponse(term, success);
        res = *append_res;
        delete append_res;
        break;
      }

      case RaftMessageType::KInstallSnapshowResponse: {
        if (!j.contains("term")) {
          return Status::DeSerializeError(
              "Missing required fields for InstallSnapshotResponse");
        }

        uint32_t term = j["term"];

        InstallSnapshotResponse* snapshot_res =
            new InstallSnapshotResponse(term);
        res = *snapshot_res;
        delete snapshot_res;
        break;
      }

      default:
        return Status::ProtocolError("Unsupported response type");
    }

    return Status::OK();

  } catch (const nlohmann::json::parse_error& e) {
    return Status::DeSerializeError("JSON parse error: " + std::string(e.what()));
  } catch (const std::exception& e) {
    return Status::DeSerializeError("Deserialization error: " +
                              std::string(e.what()));
  }
}
