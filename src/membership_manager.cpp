#include "raft_node_impl.h"

#include <nlohmann/json.hpp>

using namespace rollingraft;

// Helper: serialize node list to JSON string
static std::string NodesToJson(const std::vector<NodeId>& nodes) {
  nlohmann::json j = nodes;
  return j.dump();
}

// Helper: parse node list from JSON string
static std::vector<NodeId> JsonToNodes(const std::string& json_str) {
  auto j = nlohmann::json::parse(json_str);
  std::vector<NodeId> nodes;
  for (const auto& item : j) {
    nodes.push_back(item.get<NodeId>());
  }
  return nodes;
}

void RaftNode::RaftNodeImpl::ApplyConfigChangeLocked(const std::string& cmd) {
  // Guard: any exit path must clear pending_config_change_.
  struct PendingGuard {
    bool* flag;
    ~PendingGuard() {
      if (flag) *flag = false;
    }
  } guard{&pending_config_change_};

  // Parse config change command
  // Format: CONFIG_CHANGE:ADD:node_id:addr
  //         CONFIG_CHANGE:REMOVE:node_id
  //         CONFIG_CHANGE:JOINT:old_nodes_json:new_nodes_json
  //         CONFIG_CHANGE:FINALIZE:new_nodes_json

  if (cmd.starts_with("CONFIG_CHANGE:JOINT:")) {
    // Joint consensus phase 1: enter Cold,Cnew transitional configuration
    size_t pos1 = strlen("CONFIG_CHANGE:JOINT:");
    size_t pos2 = cmd.find(':', pos1);
    if (pos2 == std::string::npos) {
      LOG_ERROR("Invalid JOINT config change command: {}", cmd);
      return;
    }

    std::string old_nodes_json = cmd.substr(pos1, pos2 - pos1);
    std::string new_nodes_json = cmd.substr(pos2 + 1);

    std::unique_lock<std::shared_mutex> config_lock(membership_mtx_);

    cluster_config_.old_nodes = JsonToNodes(old_nodes_json);
    cluster_config_.nodes = JsonToNodes(new_nodes_json);
    cluster_config_.is_joint = true;
    cluster_config_.version++;

    // Remove newly promoted voters from learners
    for (NodeId id : cluster_config_.nodes) {
      cluster_config_.learners.erase(
          std::remove(cluster_config_.learners.begin(),
                      cluster_config_.learners.end(), id),
          cluster_config_.learners.end());
    }

    LOG_INFO("Node {} applied JOINT config (old={}, new={}, version {})",
             server_id_, old_nodes_json, new_nodes_json,
             cluster_config_.version);

    // If we are the leader, automatically propose FINALIZE after JOINT
    // is applied. This ensures the cluster transitions out of joint mode.
    if (role_ == RaftNodeRole::LEADER) {
      std::string finalize_cmd =
          "CONFIG_CHANGE:FINALIZE:" + NodesToJson(cluster_config_.nodes);
      auto [idx, status] = log_.Append(current_term_, finalize_cmd);
      if (status.ok()) {
        if (log_persister_) {
          auto entry_opt = log_.GetEntry(idx);
          if (entry_opt) {
            log_persister_->Append(*entry_opt);
          }
        }
        LOG_INFO("Node {} auto-proposed FINALIZE at index {}", server_id_,
                 idx);
        BroadcastAppendEntriesLocked();
      }
    }
    return;
  }

  if (cmd.starts_with("CONFIG_CHANGE:FINALIZE:")) {
    // Joint consensus phase 2: commit Cnew, exit joint mode
    size_t pos = strlen("CONFIG_CHANGE:FINALIZE:");
    std::string new_nodes_json = cmd.substr(pos);

    std::unique_lock<std::shared_mutex> config_lock(membership_mtx_);

    cluster_config_.nodes = JsonToNodes(new_nodes_json);
    cluster_config_.old_nodes.clear();
    cluster_config_.is_joint = false;
    cluster_config_.version++;

    // Ensure no voter is still in learners
    for (NodeId id : cluster_config_.nodes) {
      cluster_config_.learners.erase(
          std::remove(cluster_config_.learners.begin(),
                      cluster_config_.learners.end(), id),
          cluster_config_.learners.end());
    }

    LOG_INFO("Node {} applied FINALIZE config (new={}, version {})",
             server_id_, new_nodes_json, cluster_config_.version);
    return;
  }

  if (cmd.starts_with("CONFIG_CHANGE:ADD_LEARNER:")) {
    size_t pos1 = strlen("CONFIG_CHANGE:ADD_LEARNER:");
    size_t pos2 = cmd.find(':', pos1);
    if (pos2 == std::string::npos) {
      LOG_ERROR("Invalid ADD_LEARNER config change command: {}", cmd);
      return;
    }

    NodeId id = std::stoll(cmd.substr(pos1, pos2 - pos1));
    NodeAddr addr = cmd.substr(pos2 + 1);

    std::unique_lock<std::shared_mutex> config_lock(membership_mtx_);

    if (!cluster_config_.Contains(id)) {
      cluster_config_.learners.push_back(id);
      cluster_config_.version++;

      if (id != server_id_ && peer_map_.find(id) == peer_map_.end()) {
        peer_map_[id] = addr;
        peer_addrs_.push_back(addr);

        if (role_ == RaftNodeRole::LEADER) {
          next_index_[id] = log_.GetLastLogInfo().first + 1;
          match_index_[id] = 0;
          SetPeerReplicationLagMetricLocked(id);
        }
      }

      LOG_INFO("Node {} applied AddLearner for {} (config version {})",
               server_id_, id, cluster_config_.version);
    }
    return;
  }

  if (cmd.starts_with("CONFIG_CHANGE:PROMOTE:")) {
    size_t pos = strlen("CONFIG_CHANGE:PROMOTE:");
    NodeId id = std::stoll(cmd.substr(pos));

    std::unique_lock<std::shared_mutex> config_lock(membership_mtx_);

    // Remove from learners
    cluster_config_.learners.erase(
        std::remove(cluster_config_.learners.begin(),
                    cluster_config_.learners.end(), id),
        cluster_config_.learners.end());

    if (!cluster_config_.IsVoter(id)) {
      cluster_config_.nodes.push_back(id);
      cluster_config_.version++;

      LOG_INFO("Node {} applied PromoteLearner for {} (config version {})",
               server_id_, id, cluster_config_.version);
    }
    return;
  }

  if (cmd.starts_with("CONFIG_CHANGE:ADD:")) {
    // Legacy single-step add (converted to joint consensus internally)
    size_t pos1 = strlen("CONFIG_CHANGE:ADD:");
    size_t pos2 = cmd.find(':', pos1);
    if (pos2 == std::string::npos) {
      LOG_ERROR("Invalid ADD config change command: {}", cmd);
      return;
    }

    NodeId id = std::stoll(cmd.substr(pos1, pos2 - pos1));
    NodeAddr addr = cmd.substr(pos2 + 1);

    std::unique_lock<std::shared_mutex> config_lock(membership_mtx_);

    if (!cluster_config_.Contains(id)) {
      cluster_config_.nodes.push_back(id);
      cluster_config_.version++;

      // Also remove from learners if promoting
      cluster_config_.learners.erase(
          std::remove(cluster_config_.learners.begin(),
                      cluster_config_.learners.end(), id),
          cluster_config_.learners.end());

      if (id != server_id_ && peer_map_.find(id) == peer_map_.end()) {
        peer_map_[id] = addr;
        peer_addrs_.push_back(addr);

        if (role_ == RaftNodeRole::LEADER) {
          next_index_[id] = log_.GetLastLogInfo().first + 1;
          match_index_[id] = 0;
          SetPeerReplicationLagMetricLocked(id);
        }
      }

      LOG_INFO("Node {} applied AddNode for {} (config version {})",
               server_id_, id, cluster_config_.version);
    }
    return;
  }

  if (cmd.starts_with("CONFIG_CHANGE:REMOVE:")) {
    // Legacy single-step remove (converted to joint consensus internally)
    size_t pos = strlen("CONFIG_CHANGE:REMOVE:");
    NodeId id = std::stoll(cmd.substr(pos));

    std::unique_lock<std::shared_mutex> config_lock(membership_mtx_);

    cluster_config_.nodes.erase(std::remove(cluster_config_.nodes.begin(),
                                            cluster_config_.nodes.end(), id),
                                cluster_config_.nodes.end());
    cluster_config_.learners.erase(
        std::remove(cluster_config_.learners.begin(),
                    cluster_config_.learners.end(), id),
        cluster_config_.learners.end());
    cluster_config_.version++;

    peer_map_.erase(id);
    next_index_.erase(id);
    match_index_.erase(id);
    if (metrics_) {
      auto labels = metrics_node_label_;
      labels["peer_id"] = std::to_string(id);
      metrics_->RemoveGauge("raft_transport_peer_lag_entries", labels);
    }

    peer_addrs_.erase(std::remove_if(peer_addrs_.begin(), peer_addrs_.end(),
                                     [id, this](const NodeAddr& a) {
                                       return ParseNodeId(a) == id;
                                     }),
                      peer_addrs_.end());

    LOG_INFO("Node {} applied RemoveNode for {} (config version {})",
             server_id_, id, cluster_config_.version);

    if (id == server_id_) {
      LOG_INFO("Node {} removed from cluster, stopping", server_id_);
      timer_->SetTimeout(std::chrono::milliseconds(0), [this]() { Stop(); });
    }
    return;
  }
}
