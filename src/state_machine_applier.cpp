#include "raft_node_impl.h"

using namespace rollingraft;

namespace {
const std::vector<double> kLatencyBuckets = {0.001, 0.005, 0.01, 0.025, 0.05, 0.1,
                                             0.25,  0.5,   1.0,  2.5,   5.0,  10.0};
}  // namespace

void RaftNode::RaftNodeImpl::ApplyCommittedLocked() {
  while (group_->last_enqueued_ < group_->commit_index_) {
    ++group_->last_enqueued_;

    auto entry_opt = group_->log_.GetEntry(group_->last_enqueued_);
    if (!entry_opt) {
      LOG_ERROR("Node {} failed to get log entry {} — log corruption, aborting", group_->server_id_,
                group_->last_enqueued_);
      std::abort();
    }

    const auto& entry = *entry_opt;

    // Config changes are applied synchronously (under current locks) because
    // ApplyConfigChangeLocked() modifies group_->cluster_config_, group_->pending_config_change_,
    // and may append new log entries — all of which require lock protection.
    if (entry.data_.find("CONFIG_CHANGE:") == 0) {
      ApplyConfigChangeLocked(entry.data_);

      // Callback synchronously for config changes (rare, keep client responsive)
      auto it = group_->pending_proposals_.find(group_->last_enqueued_);
      if (it != group_->pending_proposals_.end()) {
        if (metrics_) {
          auto latency = std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                                       it->second.propose_time)
                             .count();
          metrics_
              ->GetHistogram("raft_proposal_latency_seconds", kLatencyBuckets,
                             group_->metrics_node_label_)
              .Observe(latency);
        }
        ApplyResult result;
        result.success = true;
        result.applied_index = group_->last_enqueued_;
        it->second.callback(result);
        group_->pending_proposals_.erase(it);
      }

      // Enqueue a dummy task so the apply thread updates group_->last_applied_ in order
      RaftGroup::ApplyTask task;
      task.index = group_->last_enqueued_;
      task.is_config_change = true;
      {
        std::lock_guard<std::mutex> lock(group_->apply_queue_mtx_);
        group_->apply_queue_.push_back(std::move(task));
      }
      group_->apply_queue_cv_.notify_one();
      continue;
    }

    // Regular entries: move callback from group_->pending_proposals_ and enqueue for
    // async apply. The apply thread will call group_->state_machine_->Apply().
    auto it = group_->pending_proposals_.find(group_->last_enqueued_);
    std::function<void(const ApplyResult&)> cb;
    std::optional<std::chrono::steady_clock::time_point> propose_time;
    if (it != group_->pending_proposals_.end()) {
      cb = std::move(it->second.callback);
      propose_time = it->second.propose_time;
      group_->pending_proposals_.erase(it);
    }

    // Check if this proposal has session info
    uint64_t session_id = 0;
    uint64_t seq_num = 0;
    auto sit = group_->proposal_sessions_.find(group_->last_enqueued_);
    if (sit != group_->proposal_sessions_.end()) {
      session_id = sit->second.first;
      seq_num = sit->second.second;
      group_->proposal_sessions_.erase(sit);
    }

    RaftGroup::ApplyTask task;
    task.index = group_->last_enqueued_;
    task.data = entry.data_;
    task.callback = std::move(cb);
    task.propose_time = propose_time;
    task.session_id = session_id;
    task.seq_num = seq_num;

    {
      std::lock_guard<std::mutex> lock(group_->apply_queue_mtx_);
      group_->apply_queue_.push_back(std::move(task));
    }
    group_->apply_queue_cv_.notify_one();
  }
}

void RaftNode::RaftNodeImpl::ApplyLoop() {
  while (group_->apply_running_.load(std::memory_order_acquire)) {
    std::unique_lock<std::mutex> lock(group_->apply_queue_mtx_);
    group_->apply_queue_cv_.wait(lock, [this] {
      return !group_->apply_queue_.empty() ||
             !group_->apply_running_.load(std::memory_order_acquire);
    });

    if (!group_->apply_running_.load(std::memory_order_acquire)) break;

    // Batch consume up to 64 entries to amortize lock overhead.
    const size_t kBatchSize = 64;
    std::vector<RaftGroup::ApplyTask> batch;
    batch.reserve(std::min(group_->apply_queue_.size(), kBatchSize));
    while (!group_->apply_queue_.empty() && batch.size() < kBatchSize) {
      batch.push_back(std::move(group_->apply_queue_.front()));
      group_->apply_queue_.pop_front();
    }
    lock.unlock();

    Index new_last_applied = group_->last_applied_.load(std::memory_order_acquire);

    for (auto& task : batch) {
      // Skip entries that have been superseded by a snapshot.
      // This can happen if HandleInstallSnapshot() was called while the
      // apply thread was processing a batch.
      if (task.index <= new_last_applied) {
        if (task.callback) {
          ApplyResult result;
          result.success = true;
          result.applied_index = task.index;
          task.callback(result);
        }
        continue;
      }

      new_last_applied = task.index;

      if (task.is_config_change) {
        // Config change already applied synchronously in ApplyCommittedLocked.
        // Just update group_->last_applied_ (no state_machine apply needed).
        continue;
      }

      // Apply to StateMachine
      auto result = group_->state_machine_->Apply(
          std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(task.data.data()),
                                   task.data.size()),
          task.index);

      // Cache result in session manager if this was a session-based proposal
      if (task.session_id != 0 && group_->session_manager_) {
        SessionResult sr;
        sr.success = result.success;
        sr.response = result.response;
        sr.applied_index = result.applied_index;
        sr.error_message = result.error_message;
        group_->session_manager_->RecordResult(task.session_id, task.seq_num, sr);
      }

      // Record proposal latency
      if (task.callback) {
        if (metrics_ && task.propose_time.has_value()) {
          auto latency =
              std::chrono::duration<double>(std::chrono::steady_clock::now() - *task.propose_time)
                  .count();
          metrics_
              ->GetHistogram("raft_proposal_latency_seconds", kLatencyBuckets,
                             group_->metrics_node_label_)
              .Observe(latency);
        }
        task.callback(result);
      }
    }

    // Publish group_->last_applied_ so readers (ProcessPendingReadsLocked,
    // GetLastAppliedIndex, snapshot logic) see the update.
    group_->last_applied_.store(new_last_applied, std::memory_order_release);

    // Process pending reads that may now be satisfied.
    {
      std::lock_guard<std::mutex> lock_a(group_->applier_mtx_);
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      ProcessPendingReadsLocked();
    }

    // Update metrics
    if (metrics_) {
      infra_->metrics_->GetGauge("raft_applied_index", group_->metrics_node_label_)
          .Set(static_cast<double>(new_last_applied));
    }
  }
}

void RaftNode::RaftNodeImpl::BroadcastReadIndexHeartbeatsLocked(uint64_t read_id) {
  if (metrics_) {
    metrics_->GetCounter("raft_readindex_heartbeats_sent_total", group_->metrics_node_label_)
        .Increment();
  }

  // Heartbeat coalescing: skip peers that received a heartbeat recently
  // (within heartbeat_interval_ms). Their acks are already in-flight and
  // will be counted by HandleAppendEntriesResponse.
  auto now = std::chrono::steady_clock::now();
  std::vector<NodeId> peers_to_send;
  std::vector<NodeId> peers_to_skip;
  for (const auto& [peer_id, addr] : group_->peer_map_) {
    (void)addr;
    auto it = group_->last_heartbeat_sent_.find(peer_id);
    if (it != group_->last_heartbeat_sent_.end()) {
      auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second).count();
      auto rc = infra_->runtime_config_->Get();
      if (elapsed >= 0 && static_cast<uint32_t>(elapsed) < rc.heartbeat_interval_ms) {
        peers_to_skip.push_back(peer_id);
        continue;
      }
    }
    peers_to_send.push_back(peer_id);
  }

  uint32_t majority = group_->cluster_config_.GetMajority();
  // Leader counts itself as 1 ack, so we need at least majority-1 peers.
  if (peers_to_send.size() + 1 < majority && !peers_to_skip.empty()) {
    size_t needed = majority - 1 - peers_to_send.size();
    for (size_t i = 0; i < needed && i < peers_to_skip.size(); ++i) {
      peers_to_send.push_back(peers_to_skip[i]);
    }
  }

  if (!peers_to_skip.empty() && metrics_) {
    metrics_->GetCounter("raft_heartbeat_coalesced_total", group_->metrics_node_label_)
        .Increment(peers_to_skip.size());
  }

  // Send empty AppendEntries (heartbeats) only to selected peers
  for (NodeId peer_id : peers_to_send) {
    AppendEntriesRequest req;
    req.group_id = group_->group_id_;
    req.term_ = group_->current_term_;
    req.leader_id_ = group_->server_id_;
    req.prev_log_index_ = group_->next_index_[peer_id] - 1;
    req.prev_log_term_ = GetLogTermLocked(req.prev_log_index_);
    req.leader_commit_ = group_->commit_index_;
    req.correlation_id_ = next_correlation_id_.fetch_add(1, std::memory_order_relaxed);
    // Empty entries = heartbeat

    std::string data;
    auto status = infra_->protocol_->SerializeRequest(req, data);
    if (!status.ok()) {
      LOG_ERROR("Failed to serialize heartbeat: {}", status.ToString());
      continue;
    }

    auto it_addr = group_->peer_map_.find(peer_id);
    if (it_addr == group_->peer_map_.end()) continue;

    infra_->network_->SendRpc(
        peer_id, it_addr->second, data, req.correlation_id_,
        std::chrono::milliseconds(infra_->runtime_config_->Get().rpc_timeout_ms),
        [this, peer_id, read_id](const std::string& resp, bool success, const std::string& error) {
          if (!success) {
            LOG_WARN("ReadIndex heartbeat to {} failed: {}", peer_id, error);
            return;
          }

          AppendEntriesResponse response;
          auto status = infra_->protocol_->DeserializeResponse(resp, response);
          if (!status.ok()) {
            LOG_ERROR("Failed to deserialize heartbeat response: {}", status.ToString());
            return;
          }

          if (response.success_) {
            // Acquire locks in hierarchy order for ReadIndex ack handling.
            std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
            std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
            std::lock_guard<std::mutex> lock_a(group_->applier_mtx_);
            HandleReadIndexAckLocked(peer_id, read_id);
          }
        });
  }

  // Mark heartbeats as sent (even if all coalesced, regular heartbeats
  // in-flight will deliver acks via HandleAppendEntriesResponse).
  auto it = group_->pending_reads_.find(read_id);
  if (it != group_->pending_reads_.end()) {
    it->second.heartbeats_sent = true;
  }
}

void RaftNode::RaftNodeImpl::HandleReadIndexAckLocked(NodeId from, uint64_t read_id) {
  if (metrics_) {
    metrics_->GetCounter("raft_readindex_acks_received_total", group_->metrics_node_label_)
        .Increment();
  }

  auto it = group_->pending_reads_.find(read_id);
  if (it == group_->pending_reads_.end()) return;

  auto& read_req = it->second;
  read_req.acks.insert(from);

  // For lease reads, acks are already verified via quorum; skip majority check
  if (!read_req.heartbeats_sent) {
    // Still check if log is applied so we can complete early
    if (group_->last_applied_.load(std::memory_order_acquire) >= read_req.read_index) {
      ProcessPendingReadsLocked();
    }
    return;
  }

  // Check if we have majority
  uint32_t majority = group_->cluster_config_.GetMajority();
  if (read_req.acks.size() >= majority) {
    LOG_INFO("ReadIndex {} received majority acks ({}/{})", read_id, read_req.acks.size(),
             group_->cluster_config_.nodes.size());

    // Check if read_index is already applied
    if (group_->last_applied_.load(std::memory_order_acquire) >= read_req.read_index) {
      // Can complete immediately
      auto callback = std::move(read_req.callback);
      if (metrics_) {
        auto latency =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - read_req.start_time)
                .count();
        metrics_
            ->GetHistogram("raft_readindex_latency_seconds", kLatencyBuckets,
                           group_->metrics_node_label_)
            .Observe(latency);
      }
      group_->pending_reads_.erase(it);
      // RAII guard ensures lock is reacquired even if callback throws
      struct LockReacquireGuard {
        std::mutex& m;
        bool need_relock = true;
        ~LockReacquireGuard() {
          if (need_relock) m.lock();
        }
      } guard{group_->applier_mtx_};
      group_->applier_mtx_.unlock();
      callback();
      guard.need_relock = false;
      group_->applier_mtx_.lock();
    }
    // Otherwise, will be completed when log is applied
  }
}

void RaftNode::RaftNodeImpl::ProcessPendingReadsLocked() {
  std::vector<uint64_t> completed_reads;

  for (auto& [read_id, read_req] : group_->pending_reads_) {
    // Check if we have majority acks (or lease read) and log is applied
    uint32_t majority = group_->cluster_config_.GetMajority();
    bool acks_ok = read_req.heartbeats_sent ? read_req.acks.size() >= majority
                                            : true;  // Lease read: acks already verified via quorum
    if (acks_ok && group_->last_applied_.load(std::memory_order_acquire) >= read_req.read_index) {
      completed_reads.push_back(read_id);
    }
  }

  // Complete the reads (outside the loop to avoid iterator invalidation)
  for (uint64_t read_id : completed_reads) {
    auto it = group_->pending_reads_.find(read_id);
    if (it != group_->pending_reads_.end()) {
      auto callback = std::move(it->second.callback);
      if (metrics_) {
        auto latency =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - it->second.start_time)
                .count();
        metrics_
            ->GetHistogram("raft_readindex_latency_seconds", kLatencyBuckets,
                           group_->metrics_node_label_)
            .Observe(latency);
      }
      group_->pending_reads_.erase(it);

      if (metrics_) {
        metrics_->GetCounter("raft_readindex_completed_total", group_->metrics_node_label_)
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
      } guard{group_->applier_mtx_};
      group_->applier_mtx_.unlock();
      callback();
      guard.need_relock = false;
      group_->applier_mtx_.lock();
    }
  }
}
