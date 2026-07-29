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
  // Guard: any exit path must clear group_->pending_config_change_.
  struct PendingGuard {
    bool* flag;
    ~PendingGuard() {
      if (flag) {
        *flag = false;
      }
    }
  } guard{&group_->pending_config_change_};

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

    std::unique_lock<std::shared_mutex> config_lock(group_->membership_mtx_);

    group_->cluster_config_.old_nodes = JsonToNodes(old_nodes_json);
    group_->cluster_config_.nodes = JsonToNodes(new_nodes_json);
    group_->cluster_config_.is_joint = true;
    group_->cluster_config_.version++;

    // Remove newly promoted voters from learners
    for (NodeId id : group_->cluster_config_.nodes) {
      group_->cluster_config_.learners.erase(
          std::remove(group_->cluster_config_.learners.begin(),
                      group_->cluster_config_.learners.end(), id),
          group_->cluster_config_.learners.end());
    }

    LOG_INFO("Node {} applied JOINT config (old={}, new={}, version {})", group_->server_id_,
             old_nodes_json, new_nodes_json, group_->cluster_config_.version);

    // If we are the leader, automatically propose FINALIZE after JOINT
    // is applied. This ensures the cluster transitions out of joint mode.
    if (group_->role_ == RaftNodeRole::LEADER) {
      std::string finalize_cmd =
          "CONFIG_CHANGE:FINALIZE:" + NodesToJson(group_->cluster_config_.nodes);
      auto [idx, status] = group_->log_.Append(group_->current_term_, finalize_cmd);
      if (status.ok()) {
        if (log_persister_) {
          auto entry_opt = group_->log_.GetEntry(idx);
          if (entry_opt) {
            log_persister_->Append(*entry_opt);
          }
        }
        LOG_INFO("Node {} auto-proposed FINALIZE at index {}", group_->server_id_, idx);
        BroadcastAppendEntriesLocked();
      }
    }
    return;
  }

  if (cmd.starts_with("CONFIG_CHANGE:FINALIZE:")) {
    // Joint consensus phase 2: commit Cnew, exit joint mode
    size_t pos = strlen("CONFIG_CHANGE:FINALIZE:");
    std::string new_nodes_json = cmd.substr(pos);

    std::unique_lock<std::shared_mutex> config_lock(group_->membership_mtx_);

    group_->cluster_config_.nodes = JsonToNodes(new_nodes_json);
    group_->cluster_config_.old_nodes.clear();
    group_->cluster_config_.is_joint = false;
    group_->cluster_config_.version++;

    // Ensure no voter is still in learners
    for (NodeId id : group_->cluster_config_.nodes) {
      group_->cluster_config_.learners.erase(
          std::remove(group_->cluster_config_.learners.begin(),
                      group_->cluster_config_.learners.end(), id),
          group_->cluster_config_.learners.end());
    }

    LOG_INFO("Node {} applied FINALIZE config (new={}, version {})", group_->server_id_,
             new_nodes_json, group_->cluster_config_.version);
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

    std::unique_lock<std::shared_mutex> config_lock(group_->membership_mtx_);

    if (!group_->cluster_config_.Contains(id)) {
      group_->cluster_config_.learners.push_back(id);
      group_->cluster_config_.version++;

      if (id != group_->server_id_ && group_->peer_map_.find(id) == group_->peer_map_.end()) {
        group_->peer_map_[id] = addr;
        group_->peer_addrs_.push_back(addr);

        if (group_->role_ == RaftNodeRole::LEADER) {
          group_->next_index_[id] = group_->log_.GetLastLogInfo().first + 1;
          group_->match_index_[id] = 0;
          SetPeerReplicationLagMetricLocked(id);
        }
      }

      LOG_INFO("Node {} applied AddLearner for {} (config version {})", group_->server_id_, id,
               group_->cluster_config_.version);
    }
    return;
  }

  if (cmd.starts_with("CONFIG_CHANGE:PROMOTE:")) {
    size_t pos = strlen("CONFIG_CHANGE:PROMOTE:");
    NodeId id = std::stoll(cmd.substr(pos));

    std::unique_lock<std::shared_mutex> config_lock(group_->membership_mtx_);

    // Remove from learners
    group_->cluster_config_.learners.erase(std::remove(group_->cluster_config_.learners.begin(),
                                                       group_->cluster_config_.learners.end(), id),
                                           group_->cluster_config_.learners.end());

    if (!group_->cluster_config_.IsVoter(id)) {
      group_->cluster_config_.nodes.push_back(id);
      group_->cluster_config_.version++;

      LOG_INFO("Node {} applied PromoteLearner for {} (config version {})", group_->server_id_, id,
               group_->cluster_config_.version);
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

    std::unique_lock<std::shared_mutex> config_lock(group_->membership_mtx_);

    if (!group_->cluster_config_.Contains(id)) {
      group_->cluster_config_.nodes.push_back(id);
      group_->cluster_config_.version++;

      // Also remove from learners if promoting
      group_->cluster_config_.learners.erase(
          std::remove(group_->cluster_config_.learners.begin(),
                      group_->cluster_config_.learners.end(), id),
          group_->cluster_config_.learners.end());

      if (id != group_->server_id_ && group_->peer_map_.find(id) == group_->peer_map_.end()) {
        group_->peer_map_[id] = addr;
        group_->peer_addrs_.push_back(addr);

        if (group_->role_ == RaftNodeRole::LEADER) {
          group_->next_index_[id] = group_->log_.GetLastLogInfo().first + 1;
          group_->match_index_[id] = 0;
          SetPeerReplicationLagMetricLocked(id);
        }
      }

      LOG_INFO("Node {} applied AddNode for {} (config version {})", group_->server_id_, id,
               group_->cluster_config_.version);
    }
    return;
  }

  if (cmd.starts_with("CONFIG_CHANGE:REMOVE:")) {
    // Legacy single-step remove (converted to joint consensus internally)
    size_t pos = strlen("CONFIG_CHANGE:REMOVE:");
    NodeId id = std::stoll(cmd.substr(pos));

    std::unique_lock<std::shared_mutex> config_lock(group_->membership_mtx_);

    group_->cluster_config_.nodes.erase(
        std::remove(group_->cluster_config_.nodes.begin(), group_->cluster_config_.nodes.end(), id),
        group_->cluster_config_.nodes.end());
    group_->cluster_config_.learners.erase(std::remove(group_->cluster_config_.learners.begin(),
                                                       group_->cluster_config_.learners.end(), id),
                                           group_->cluster_config_.learners.end());
    group_->cluster_config_.version++;

    group_->peer_map_.erase(id);
    group_->next_index_.erase(id);
    group_->match_index_.erase(id);
    if (metrics_) {
      auto labels = group_->metrics_node_label_;
      labels["peer_id"] = std::to_string(id);
      infra_->metrics_->RemoveGauge("raft_transport_peer_lag_entries", labels);
    }

    group_->peer_addrs_.erase(
        std::remove_if(group_->peer_addrs_.begin(), group_->peer_addrs_.end(),
                       [id](const NodeAddr& a) { return RaftGroup::ParseNodeId(a) == id; }),
        group_->peer_addrs_.end());

    LOG_INFO("Node {} applied RemoveNode for {} (config version {})", group_->server_id_, id,
             group_->cluster_config_.version);

    if (id == group_->server_id_) {
      LOG_INFO("Node {} removed from cluster, stopping", group_->server_id_);
      infra_->timer_->SetTimeout(std::chrono::milliseconds(0),
                                 [weak_self = weak_from_this(), this]() {
                                   auto keep_alive = weak_self.lock();
                                   if (!keep_alive) {
                                     return;
                                   }
                                   Stop();
                                 });
    }
    return;
  }
}
