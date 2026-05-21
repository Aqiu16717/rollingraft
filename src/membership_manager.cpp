#include "raft_node_impl.h"

using namespace rollingraft;

void RaftNode::RaftNodeImpl::ApplyConfigChangeLocked(const std::string& cmd) {
  // Guard: any exit path must clear pending_config_change_.
  struct PendingGuard {
    bool* flag;
    ~PendingGuard() {
      if (flag) *flag = false;
    }
  } guard{&pending_config_change_};

  // Parse config change command
  // Format: CONFIG_CHANGE:ADD:node_id:addr  or  CONFIG_CHANGE:REMOVE:node_id

  if (cmd.find("CONFIG_CHANGE:ADD:") == 0) {
    // Parse ADD command
    size_t pos1 = strlen("CONFIG_CHANGE:ADD:");
    size_t pos2 = cmd.find(':', pos1);
    if (pos2 == std::string::npos) {
      LOG_ERROR("Invalid ADD config change command: {}", cmd);
      return;
    }

    NodeId id = std::stoll(cmd.substr(pos1, pos2 - pos1));
    NodeAddr addr = cmd.substr(pos2 + 1);

    std::unique_lock<std::shared_mutex> config_lock(membership_mtx_);

    // Add to config if not already present
    if (!cluster_config_.Contains(id)) {
      cluster_config_.nodes.push_back(id);
      cluster_config_.version++;

      // Update peer map if not already present
      if (id != server_id_ && peer_map_.find(id) == peer_map_.end()) {
        peer_map_[id] = addr;
        peer_addrs_.push_back(addr);

        // Initialize leader state if leader
        if (role_ == RaftNodeRole::LEADER) {
          next_index_[id] = log_.GetLastLogInfo().first + 1;
          match_index_[id] = 0;
        }
      }

      LOG_INFO("Node {} applied AddNode for {} (config version {})", server_id_,
               id, cluster_config_.version);
    }

  } else if (cmd.find("CONFIG_CHANGE:REMOVE:") == 0) {
    // Parse REMOVE command
    size_t pos = strlen("CONFIG_CHANGE:REMOVE:");
    NodeId id = std::stoll(cmd.substr(pos));

    std::unique_lock<std::shared_mutex> config_lock(membership_mtx_);

    // Remove from config
    cluster_config_.nodes.erase(std::remove(cluster_config_.nodes.begin(),
                                            cluster_config_.nodes.end(), id),
                                cluster_config_.nodes.end());
    cluster_config_.version++;

    // Remove from peer map
    peer_map_.erase(id);
    next_index_.erase(id);
    match_index_.erase(id);

    // Remove from peer_addrs_
    peer_addrs_.erase(std::remove_if(peer_addrs_.begin(), peer_addrs_.end(),
                                     [id, this](const NodeAddr& a) {
                                       return ParseNodeId(a) == id;
                                     }),
                      peer_addrs_.end());

    LOG_INFO("Node {} applied RemoveNode for {} (config version {})",
             server_id_, id, cluster_config_.version);

    // If we removed ourselves, stop
    if (id == server_id_) {
      LOG_INFO("Node {} removed from cluster, stopping", server_id_);
      // Schedule stop (can't hold lock during Stop)
      timer_->SetTimeout(std::chrono::milliseconds(0), [this]() { Stop(); });
    }
  }
}
