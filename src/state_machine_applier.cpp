#include "raft_node_impl.h"

using namespace rollingraft;

namespace {
const std::vector<double> kLatencyBuckets = {
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0};
}  // namespace

void RaftNode::RaftNodeImpl::ApplyCommittedLocked() {
  while (last_applied_ < commit_index_) {
    ++last_applied_;

    auto entry_opt = log_.GetEntry(last_applied_);
    if (!entry_opt) {
      LOG_ERROR("Node {} failed to get log entry {}", server_id_,
                last_applied_);
      continue;
    }

    const auto& entry = *entry_opt;

    // Check if this is a config change command
    if (entry.data_.find("CONFIG_CHANGE:") == 0) {
      ApplyConfigChangeLocked(entry.data_);

      // Still need to callback for proposals
      auto it = pending_proposals_.find(last_applied_);
      if (it != pending_proposals_.end()) {
        if (metrics_) {
          auto latency = std::chrono::duration<double>(
              std::chrono::steady_clock::now() - it->second.propose_time).count();
          metrics_->GetHistogram("raft_proposal_latency_seconds", kLatencyBuckets,
                                 {{"node_id", std::to_string(server_id_)}})
              .Observe(latency);
        }
        ApplyResult result;
        result.success = true;
        result.applied_index = last_applied_;
        it->second.callback(result);
        pending_proposals_.erase(it);
      }
      continue;
    }

    // Apply to StateMachine
    auto result = state_machine_->Apply(
        std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(entry.data_.data()),
            entry.data_.size()),
        last_applied_);

    // Callback to waiting users
    auto it = pending_proposals_.find(last_applied_);
    if (it != pending_proposals_.end()) {
      if (metrics_) {
        auto latency = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - it->second.propose_time).count();
        metrics_->GetHistogram("raft_proposal_latency_seconds", kLatencyBuckets,
                               {{"node_id", std::to_string(server_id_)}})
            .Observe(latency);
      }
      it->second.callback(result);
      pending_proposals_.erase(it);
    }
  }

  if (metrics_) {
    metrics_
        ->GetGauge("raft_applied_index",
                   {{"node_id", std::to_string(server_id_)}})
        .Set(static_cast<double>(last_applied_));
  }

  // Check if any pending reads can be completed.
  // Acquire membership_mtx_ (shared) + applier_mtx_ for pending_reads_.
  {
    std::shared_lock<std::shared_mutex> lock_m(membership_mtx_);
    std::lock_guard<std::mutex> lock_a(applier_mtx_);
    ProcessPendingReadsLocked();
  }
}

void RaftNode::RaftNodeImpl::BroadcastReadIndexHeartbeatsLocked(
    uint64_t read_id) {
  if (metrics_) {
    metrics_
        ->GetCounter("raft_readindex_heartbeats_sent_total",
                     {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }

  // Send empty AppendEntries (heartbeats) to all peers
  for (const auto& [peer_id, addr] : peer_map_) {
    (void)addr;

    AppendEntriesRequest req;
    req.term_ = current_term_;
    req.leader_id_ = server_id_;
    req.prev_log_index_ = next_index_[peer_id] - 1;
    req.prev_log_term_ = GetLogTermLocked(req.prev_log_index_);
    req.leader_commit_ = commit_index_;
    req.correlation_id_ =
        next_correlation_id_.fetch_add(1, std::memory_order_relaxed);
    // Empty entries = heartbeat

    std::string data;
    auto status = protocol_->SerializeRequest(req, data);
    if (!status.ok()) {
      LOG_ERROR("Failed to serialize heartbeat: {}", status.ToString());
      continue;
    }

    auto it_addr = peer_map_.find(peer_id);
    if (it_addr == peer_map_.end()) continue;

    network_->SendRpc(
        peer_id, it_addr->second, data, req.correlation_id_,
        std::chrono::milliseconds(config_.rpc_timeout_ms),
        [this, peer_id, read_id](const std::string& resp, bool success,
                                 const std::string& error) {
          if (!success) {
            LOG_WARN("ReadIndex heartbeat to {} failed: {}", peer_id, error);
            return;
          }

          AppendEntriesResponse response;
          auto status = protocol_->DeserializeResponse(resp, response);
          if (!status.ok()) {
            LOG_ERROR("Failed to deserialize heartbeat response: {}",
                      status.ToString());
            return;
          }

          if (response.success_) {
            // Acquire locks in hierarchy order for ReadIndex ack handling.
            std::lock_guard<std::mutex> lock_r(replication_mtx_);
            std::shared_lock<std::shared_mutex> lock_m(membership_mtx_);
            std::lock_guard<std::mutex> lock_a(applier_mtx_);
            HandleReadIndexAckLocked(peer_id, read_id);
          }
        });
  }

  // Mark heartbeats as sent
  auto it = pending_reads_.find(read_id);
  if (it != pending_reads_.end()) {
    it->second.heartbeats_sent = true;
  }
}

void RaftNode::RaftNodeImpl::HandleReadIndexAckLocked(NodeId from,
                                                      uint64_t read_id) {
  if (metrics_) {
    metrics_
        ->GetCounter("raft_readindex_acks_received_total",
                     {{"node_id", std::to_string(server_id_)}})
        .Increment();
  }

  auto it = pending_reads_.find(read_id);
  if (it == pending_reads_.end()) return;

  auto& read_req = it->second;
  read_req.acks.insert(from);

  // Check if we have majority
  uint32_t majority = cluster_config_.GetMajority();
  if (read_req.acks.size() >= majority) {
    LOG_INFO("ReadIndex {} received majority acks ({}/{})", read_id,
             read_req.acks.size(), cluster_config_.nodes.size());

    // Check if read_index is already applied
    if (last_applied_ >= read_req.read_index) {
      // Can complete immediately
      auto callback = std::move(read_req.callback);
      if (metrics_) {
        auto latency = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - read_req.start_time).count();
        metrics_->GetHistogram("raft_readindex_latency_seconds", kLatencyBuckets,
                               {{"node_id", std::to_string(server_id_)}})
            .Observe(latency);
      }
      pending_reads_.erase(it);
      // RAII guard ensures lock is reacquired even if callback throws
      struct LockReacquireGuard {
        std::mutex& m;
        bool need_relock = true;
        ~LockReacquireGuard() {
          if (need_relock) m.lock();
        }
      } guard{applier_mtx_};
      applier_mtx_.unlock();
      callback();
      guard.need_relock = false;
      applier_mtx_.lock();
    }
    // Otherwise, will be completed when log is applied
  }
}

void RaftNode::RaftNodeImpl::ProcessPendingReadsLocked() {
  std::vector<uint64_t> completed_reads;

  for (auto& [read_id, read_req] : pending_reads_) {
    // Check if we have majority acks and log is applied
    uint32_t majority = cluster_config_.GetMajority();
    if (read_req.acks.size() >= majority &&
        last_applied_ >= read_req.read_index) {
      completed_reads.push_back(read_id);
    }
  }

  // Complete the reads (outside the loop to avoid iterator invalidation)
  for (uint64_t read_id : completed_reads) {
    auto it = pending_reads_.find(read_id);
    if (it != pending_reads_.end()) {
      auto callback = std::move(it->second.callback);
      if (metrics_) {
        auto latency = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - it->second.start_time).count();
        metrics_->GetHistogram("raft_readindex_latency_seconds", kLatencyBuckets,
                               {{"node_id", std::to_string(server_id_)}})
            .Observe(latency);
      }
      pending_reads_.erase(it);

      if (metrics_) {
        metrics_
            ->GetCounter("raft_readindex_completed_total",
                         {{"node_id", std::to_string(server_id_)}})
            .Increment();
      }

      LOG_DEBUG("Completing ReadIndex {}", read_id);
      // RAII guard ensures lock is reacquired even if callback throws
      struct LockReacquireGuard {
        std::mutex& m;
        bool need_relock = true;
        ~LockReacquireGuard() {
          if (need_relock) m.lock();
        }
      } guard{applier_mtx_};
      applier_mtx_.unlock();
      callback();
      guard.need_relock = false;
      applier_mtx_.lock();
    }
  }
}
