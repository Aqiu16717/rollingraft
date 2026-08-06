#include <fstream>

#include "json_protocol.h"
#include "nlohmann/json.hpp"
#include "raft_node_impl.h"

using namespace rollingraft;

void RaftNode::RaftNodeImpl::HandleIncomingRpc(NodeId /*from*/, const std::string& data,
                                               std::string& response) {
  // First, peek at the message type to dispatch to the correct handler
  // We need to deserialize based on the type field in the JSON
  try {
    // Parse just enough to get the type
    auto j = nlohmann::json::parse(data);
    if (!j.contains("type")) {
      LOG_ERROR("Received RPC without type field");
      return;
    }

    int type_id = j["type"];
    auto message_type = static_cast<RaftMessageType>(type_id);

    switch (message_type) {
      case RaftMessageType::KRequestVoteRequest: {
        RequestVoteRequest req;
        auto status = infra_->protocol_->DeserializeRequest(data, req);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize RequestVoteRequest: {}", status.ToString());
          return;
        }
        RequestVoteResponse resp;
        resp.correlation_id_ = req.correlation_id_;
        resp.group_id = req.group_id;
        HandleRequestVote(req, resp);
        status = infra_->protocol_->SerializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to serialize RequestVoteResponse: {}", status.ToString());
          return;
        }
        break;
      }

      case RaftMessageType::KAppendEntriesRequest: {
        AppendEntriesRequest req;
        auto status = infra_->protocol_->DeserializeRequest(data, req);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize AppendEntriesRequest: {}", status.ToString());
          return;
        }
        AppendEntriesResponse resp;
        resp.correlation_id_ = req.correlation_id_;
        resp.group_id = req.group_id;
        HandleAppendEntries(req, resp);
        status = infra_->protocol_->SerializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to serialize AppendEntriesResponse: {}", status.ToString());
          return;
        }
        break;
      }

      case RaftMessageType::KInstallSnapshotRequest: {
        InstallSnapshotRequest req;
        auto status = infra_->protocol_->DeserializeRequest(data, req);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize InstallSnapshotRequest: {}", status.ToString());
          return;
        }
        InstallSnapshotResponse resp;
        resp.correlation_id_ = req.correlation_id_;
        resp.group_id = req.group_id;
        HandleInstallSnapshot(req, resp);
        status = infra_->protocol_->SerializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to serialize InstallSnapshotResponse: {}", status.ToString());
          return;
        }
        break;
      }

      case RaftMessageType::KClientRequest: {
        ClientRequest req;
        auto status = infra_->protocol_->DeserializeRequest(data, req);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize ClientRequest: {}", status.ToString());
          return;
        }
        ClientResponse resp;
        resp.correlation_id_ = req.correlation_id_;
        resp.group_id = req.group_id;
        HandleClientRequest(req, resp);
        status = infra_->protocol_->SerializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to serialize ClientResponse: {}", status.ToString());
          return;
        }
        break;
      }

      case RaftMessageType::KPreVoteRequest: {
        PreVoteRequest req;
        auto status = infra_->protocol_->DeserializeRequest(data, req);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize PreVoteRequest: {}", status.ToString());
          return;
        }
        PreVoteResponse resp;
        resp.correlation_id_ = req.correlation_id_;
        resp.group_id = req.group_id;
        HandlePreVote(req, resp);
        status = infra_->protocol_->SerializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to serialize PreVoteResponse: {}", status.ToString());
          return;
        }
        break;
      }

      case RaftMessageType::KReadIndexRequest: {
        ReadIndexRequest req;
        auto status = infra_->protocol_->DeserializeRequest(data, req);
        if (!status.ok()) {
          LOG_ERROR("Failed to deserialize ReadIndexRequest: {}", status.ToString());
          return;
        }
        ReadIndexResponse resp;
        resp.correlation_id_ = req.correlation_id_;
        resp.group_id = req.group_id;
        HandleReadIndexRequest(req, resp);
        status = infra_->protocol_->SerializeResponse(resp, response);
        if (!status.ok()) {
          LOG_ERROR("Failed to serialize ReadIndexResponse: {}", status.ToString());
          return;
        }
        break;
      }

      default:
        LOG_ERROR("Unknown message type: {}", type_id);
        break;
    }

  } catch (const std::exception& e) {
    LOG_ERROR("Failed to handle incoming RPC: {}", e.what());
  }
}

void RaftNode::RaftNodeImpl::HandleRequestVote(const RequestVoteRequest& req,
                                               RequestVoteResponse& resp) {
  std::lock_guard<std::mutex> lock(group_->election_mtx_);

  resp.term_ = group_->current_term_;
  resp.vote_granted_ = false;

  if (metrics_) {
    metrics_->GetCounter("raft_requestvote_received_total", group_->metrics_node_label_)
        .Increment();
  }

  // If request term is higher, revert to Follower
  if (req.term_ > group_->current_term_) {
    BecomeFollowerLocked(req.term_);
    resp.term_ = group_->current_term_;
  }

  // Reject stale term requests
  if (req.term_ < group_->current_term_) {
    LOG_DEBUG("Node {} reject vote: req.term {} < {}", group_->server_id_, req.term_,
              group_->current_term_);
    return;
  }

  // Joint consensus / membership: only cluster members can vote
  {
    std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
    if (!group_->cluster_config_.IsVoter(req.candidate_id_)) {
      LOG_DEBUG("Node {} reject vote: candidate {} is not a voter", group_->server_id_,
                req.candidate_id_);
      return;
    }
  }

  // Check if log is at least as up-to-date
  auto [last_index, last_term] = group_->log_.GetLastLogInfo();

  bool log_is_up_to_date = (req.last_log_term_ > last_term) ||
                           (req.last_log_term_ == last_term && req.last_log_index_ >= last_index);

  if (!log_is_up_to_date) {
    LOG_DEBUG("Node {} reject vote: candidate log not up-to-date", group_->server_id_);
    return;
  }

  // Leader stickiness (CheckQuorum): if we have heard from a valid leader
  // within the election timeout, do not grant vote to another candidate.
  // This prevents disruptive nodes from triggering unnecessary elections.
  if (group_->check_quorum_enabled_) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - group_->last_leader_contact_)
            .count();
    auto cfg = infra_->runtime_config_->Get();
    if (elapsed >= 0 && static_cast<uint32_t>(elapsed) < cfg.election_timeout_ms) {
      LOG_INFO("Node {} reject vote: leader contact {}ms ago (< {}ms)", group_->server_id_, elapsed,
               cfg.election_timeout_ms);
      return;
    }
  }

  // Check if already voted
  if (group_->voted_for_ == -1 || group_->voted_for_ == req.candidate_id_) {
    group_->voted_for_ = req.candidate_id_;
    if (metrics_) {
      infra_->metrics_->GetCounter("raft_votes_granted_total", group_->metrics_node_label_)
          .Increment();
    }
    resp.vote_granted_ = true;

    // Reset election timer
    ResetElectionTimerLocked();

    // Persist state
    if (persister_) {
      auto persist_status = persister_->SaveState(CurrentPersistentStateLocked());
      if (!persist_status.ok()) {
        LOG_ERROR("Node {} failed to persist vote: {}", group_->server_id_,
                  persist_status.GetMessage());
        resp.vote_granted_ = false;
        return;
      }
    }

    LOG_INFO("Node {} voted for {} at term {}", group_->server_id_, req.candidate_id_,
             group_->current_term_);
  }
}

void RaftNode::RaftNodeImpl::HandlePreVote(const PreVoteRequest& req, PreVoteResponse& resp) {
  std::lock_guard<std::mutex> lock(group_->election_mtx_);

  resp.term_ = group_->current_term_;
  resp.vote_granted_ = false;

  // Pre-vote semantics (etcd-style):
  // - Candidate sends term = current_term + 1 (but does not persist it)
  // - Receiver rejects if its own term >= req.term (already knows more)
  // - Receiver rejects if candidate log is not up-to-date
  // - Receiver rejects if it has heard from a valid leader recently
  // - Otherwise grants pre-vote (does NOT modify voted_for or persist)

  if (req.term_ <= group_->current_term_) {
    LOG_DEBUG("Node {} reject PreVote: req.term {} <= {}", group_->server_id_, req.term_,
              group_->current_term_);
    return;
  }

  // Learners cannot become leaders
  {
    std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
    if (!group_->cluster_config_.IsVoter(req.candidate_id_)) {
      LOG_DEBUG("Node {} reject PreVote: candidate {} is not a voter", group_->server_id_,
                req.candidate_id_);
      return;
    }
  }

  // Check if log is at least as up-to-date
  auto [last_index, last_term] = group_->log_.GetLastLogInfo();

  bool log_is_up_to_date = (req.last_log_term_ > last_term) ||
                           (req.last_log_term_ == last_term && req.last_log_index_ >= last_index);

  if (!log_is_up_to_date) {
    LOG_DEBUG("Node {} reject PreVote: candidate log not up-to-date", group_->server_id_);
    return;
  }

  // Leader stickiness: if we have heard from a valid leader within election
  // timeout, reject pre-vote (even though req.term > current_term).
  if (group_->check_quorum_enabled_) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - group_->last_leader_contact_)
            .count();
    auto cfg = infra_->runtime_config_->Get();
    if (elapsed >= 0 && static_cast<uint32_t>(elapsed) < cfg.election_timeout_ms) {
      LOG_INFO("Node {} reject PreVote: leader contact {}ms ago (< {}ms)", group_->server_id_,
               elapsed, cfg.election_timeout_ms);
      return;
    }
  }

  resp.vote_granted_ = true;
  LOG_INFO("Node {} granted PreVote to {} at term {} (req_term={})", group_->server_id_,
           req.candidate_id_, group_->current_term_, req.term_);
}

void RaftNode::RaftNodeImpl::HandleAppendEntries(const AppendEntriesRequest& req,
                                                 AppendEntriesResponse& resp) {
  // Phase 1: Election state check under group_->election_mtx_ only.
  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);

    RecordActivityLocked();

    if (metrics_) {
      metrics_->GetCounter("raft_appendentries_received_total", group_->metrics_node_label_)
          .Increment();
    }

    resp.term_ = group_->current_term_;
    resp.success_ = false;
    resp.conflict_index_ = 0;
    resp.entries_count_ = 0;

    // If leader term is higher, revert to Follower
    if (req.term_ > group_->current_term_) {
      BecomeFollowerLocked(req.term_);
      resp.term_ = group_->current_term_;
    }

    // Reject stale term leader
    if (req.term_ < group_->current_term_) {
      LOG_DEBUG("Node {} reject AppendEntries: req.term {} < {}", group_->server_id_, req.term_,
                group_->current_term_);
      return;
    }

    // Update leader info
    group_->leader_id_ = req.leader_id_;
    {
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      auto it = group_->peer_map_.find(group_->leader_id_);
      if (it != group_->peer_map_.end()) {
        group_->leader_addr_ = it->second;
      }
    }

    // Record leader contact time for CheckQuorum leader stickiness
    group_->last_leader_contact_ = std::chrono::steady_clock::now();

    // Reset election timer
    ResetElectionTimerLocked();
  }

  // Phase 2: Replication work.  ApplyCommittedLocked() acquires
  // group_->membership_mtx_ + group_->applier_mtx_ internally; we must not hold them here
  // to avoid double-lock deadlock.
  {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);

    // Fast-forward hint: if prev_log is deeper than our snapshot boundary,
    // the leader's entries are already covered by our snapshot. Tell it to
    // skip ahead to our first log index instead of matching spuriously
    // against compacted-away indices.
    Index first_index = group_->log_.GetFirstIndex();
    if (req.prev_log_index_ > 0 && req.prev_log_index_ + 1 < first_index) {
      LOG_DEBUG("Node {} fast-forward AppendEntries: prev_log_index {} < first_index {}",
                group_->server_id_, req.prev_log_index_, first_index);
      resp.conflict_index_ = first_index;
      return;
    }

    // Check if prev_log matches
    if (req.prev_log_index_ > 0) {
      Term prev_term = GetLogTermLocked(req.prev_log_index_);
      if (prev_term != req.prev_log_term_) {
        LOG_DEBUG("Node {} log mismatch at index {}: local={}, remote={}", group_->server_id_,
                  req.prev_log_index_, prev_term, req.prev_log_term_);
        resp.conflict_index_ = req.prev_log_index_;
        return;
      }
    }

    // Append log entries
    if (!req.entries_.empty()) {
      // Check for conflicts and truncate
      for (const auto& entry : req.entries_) {
        Term existing_term = GetLogTermLocked(entry.index_);
        if (existing_term != 0 && existing_term != entry.term_) {
          LOG_INFO("Node {} truncating log from index {}", group_->server_id_, entry.index_);
          group_->log_.TruncateSuffix(entry.index_);
          // Keep the durable log consistent with the in-memory one;
          // otherwise truncated divergent entries resurrect on restart.
          if (log_persister_) {
            auto trunc_status = log_persister_->TruncateSuffix(entry.index_);
            if (!trunc_status.ok()) {
              LOG_ERROR("Node {} failed to persist log truncation from {}: {}", group_->server_id_,
                        entry.index_, trunc_status.ToString());
            }
          }
          break;
        }
      }

      // Append new entries
      for (const auto& entry : req.entries_) {
        // Skip entries already covered by our snapshot/compacted prefix.
        if (entry.index_ < group_->log_.GetFirstIndex()) {
          continue;
        }
        Term existing_term = GetLogTermLocked(entry.index_);
        if (existing_term != 0 && existing_term == entry.term_) {
          continue;  // Already have this entry, skip duplicate
        }
        // Continuity guard: entries must extend the log contiguously. A gap
        // means the prev_log check passed spuriously — reject so the leader
        // re-syncs instead of grafting entries at wrong positions
        // (RaftLog::Append assigns indices itself).
        auto [last_index, _t] = group_->log_.GetLastLogInfo();
        Index expected = (last_index == 0) ? group_->log_.GetFirstIndex() : last_index + 1;
        if (entry.index_ != expected) {
          LOG_ERROR("Node {} reject non-contiguous entry {} (expected {}), log corruption risk",
                    group_->server_id_, entry.index_, expected);
          resp.conflict_index_ = expected;
          return;
        }
        auto [idx, status] = group_->log_.Append(entry.term_, entry.data_);
        (void)idx;
        if (!status.ok()) {
          LOG_ERROR("Node {} failed to append entry: {}", group_->server_id_, status.ToString());
          return;
        }

        // Persist log entry (async)
        if (log_persister_) {
          log_persister_->Append(entry);
        }
      }
      resp.entries_count_ = req.entries_.size();
      LOG_INFO("Node {} appended {} entries", group_->server_id_, req.entries_.size());
    }

    // Update commit_index
    if (req.leader_commit_ > group_->commit_index_) {
      auto [last_index, _] = group_->log_.GetLastLogInfo();
      group_->commit_index_ = std::min(req.leader_commit_, last_index);
      ApplyCommittedLocked();
    }

    resp.success_ = true;
  }
}

void RaftNode::RaftNodeImpl::HandleInstallSnapshot(const InstallSnapshotRequest& req,
                                                   InstallSnapshotResponse& resp) {
  // Phase 1: Election state check under group_->election_mtx_ only.
  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);

    resp.term_ = group_->current_term_;

    // Term check: reject stale leader
    if (req.term_ < group_->current_term_) {
      LOG_DEBUG("Node {} reject InstallSnapshot: req.term {} < {}", group_->server_id_, req.term_,
                group_->current_term_);
      return;
    }

    // Higher term: revert to follower
    if (req.term_ > group_->current_term_) {
      LOG_INFO("Node {} term {} < {}, reverting to Follower", group_->server_id_,
               group_->current_term_, req.term_);
      BecomeFollowerLocked(req.term_);
      resp.term_ = group_->current_term_;
    }

    // Update leader info
    group_->leader_id_ = req.leader_id_;
    {
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      auto it = group_->peer_map_.find(group_->leader_id_);
      if (it != group_->peer_map_.end()) {
        group_->leader_addr_ = it->second;
      }
    }

    // Reset election timer (we have a valid leader)
    ResetElectionTimerLocked();

    if (metrics_) {
      metrics_->GetCounter("raft_snapshots_received_total", group_->metrics_node_label_)
          .Increment();
    }
  }

  // Phase 2: Snapshot + replication + applier work.
  {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    std::lock_guard<std::mutex> lock_a(group_->applier_mtx_);

    // Handle snapshot chunk — write to temp file to avoid OOM on large
    // snapshots (P0 Phase 2 streaming fix).
    if (req.offset_ == 0) {
      // New snapshot transfer: (re)start receive state and a fresh temp file.
      // The path includes group_id so concurrent receives in different
      // multi-raft groups on this node never collide.
      group_->snapshot_recv_index_ = req.last_included_index_;
      group_->snapshot_recv_term_ = req.last_included_term_;
      group_->snapshot_recv_expected_offset_ = 0;
      std::string base_dir = group_->config_.data_dir.empty() ? "/tmp" : group_->config_.data_dir;
      group_->snapshot_temp_path_ =
          base_dir + "/rollingraft_snapshot_" + std::to_string(group_->server_id_) + "_g" +
          std::to_string(group_->group_id_) + "_" + std::to_string(req.last_included_index_) + "_" +
          std::to_string(req.last_included_term_);
      std::ofstream ofs(group_->snapshot_temp_path_, std::ios::binary | std::ios::trunc);
      if (!ofs) {
        LOG_ERROR("Node {} failed to create snapshot temp file: {}", group_->server_id_,
                  group_->snapshot_temp_path_);
        return;
      }
      LOG_INFO("Node {} starting snapshot receive: index={}, term={}", group_->server_id_,
               req.last_included_index_, req.last_included_term_);
    } else {
      // Continuation chunks must belong to the in-progress transfer and
      // arrive at the expected offset. Anything else means two transfers
      // interleaved (e.g. leader change mid-send); mixing their chunks would
      // corrupt the restored state machine. Reset and drop — the leader's
      // retry machinery will restart the transfer from offset 0.
      if (group_->snapshot_temp_path_.empty() ||
          req.last_included_index_ != group_->snapshot_recv_index_ ||
          req.last_included_term_ != group_->snapshot_recv_term_ ||
          req.offset_ != group_->snapshot_recv_expected_offset_) {
        LOG_WARN(
            "Node {} dropping snapshot chunk from stale transfer: idx={}/{} term={}/{} "
            "offset={}/{}",
            group_->server_id_, req.last_included_index_, group_->snapshot_recv_index_,
            req.last_included_term_, group_->snapshot_recv_term_, req.offset_,
            group_->snapshot_recv_expected_offset_);
        if (!group_->snapshot_temp_path_.empty()) {
          if (std::remove(group_->snapshot_temp_path_.c_str()) != 0) {
            LOG_WARN("Node {} failed to remove temp snapshot file: {}", group_->server_id_,
                     group_->snapshot_temp_path_);
          }
          group_->snapshot_temp_path_.clear();
        }
        group_->snapshot_recv_index_ = 0;
        group_->snapshot_recv_term_ = 0;
        group_->snapshot_recv_expected_offset_ = 0;
        return;
      }
    }

    // Append chunk to temp file
    {
      std::ofstream ofs(group_->snapshot_temp_path_, std::ios::binary | std::ios::app);
      if (!ofs) {
        LOG_ERROR("Node {} failed to open snapshot temp file: {}", group_->server_id_,
                  group_->snapshot_temp_path_);
        return;
      }
      ofs.write(req.data_.data(), req.data_.size());
      if (!ofs) {
        LOG_ERROR("Node {} failed to write snapshot chunk to temp file", group_->server_id_);
        return;
      }
    }
    group_->snapshot_recv_expected_offset_ += req.data_.size();
    LOG_DEBUG("Node {} received snapshot chunk: offset={}, size={}, done={}", group_->server_id_,
              req.offset_, req.data_.size(), req.done_);

    // Final chunk: restore state machine and persist
    if (req.done_) {
      // Transfer is over on every path below — reset receive state.
      group_->snapshot_recv_index_ = 0;
      group_->snapshot_recv_term_ = 0;
      group_->snapshot_recv_expected_offset_ = 0;
      try {
        // Get file size for logging
        std::ifstream ifs_size(group_->snapshot_temp_path_, std::ios::binary | std::ios::ate);
        auto file_size = static_cast<int64_t>(ifs_size.tellg());

        LOG_INFO("Node {} restoring from snapshot: {} bytes, up to index {} term {}",
                 group_->server_id_, file_size, req.last_included_index_, req.last_included_term_);

        // Restore state machine via streaming interface
        auto restore_ifs = std::make_shared<std::ifstream>();
        auto restore_initialized = std::make_shared<bool>(false);
        auto restore_provider = [&](std::string& chunk) -> bool {
          if (!*restore_initialized) {
            restore_ifs->open(group_->snapshot_temp_path_, std::ios::binary);
            *restore_initialized = true;
          }
          if (!*restore_ifs) {
            return false;
          }
          constexpr size_t kChunkSize = 64 * 1024;
          chunk.resize(kChunkSize);
          restore_ifs->read(chunk.data(), kChunkSize);
          auto bytes_read = restore_ifs->gcount();
          if (bytes_read <= 0) {
            restore_ifs->close();
            *restore_initialized = false;
            return false;
          }
          chunk.resize(bytes_read);
          return true;
        };

        if (!group_->state_machine_->RestoreStream(restore_provider)) {
          LOG_ERROR("Node {} failed to restore from snapshot", group_->server_id_);
          if (std::remove(group_->snapshot_temp_path_.c_str()) != 0) {
            LOG_WARN("Node {} failed to remove temp snapshot file: {}", group_->server_id_,
                     group_->snapshot_temp_path_);
          }
          group_->snapshot_temp_path_.clear();
          return;
        }

        // Update log: discard all entries covered by snapshot
        uint64_t old_first_index = group_->log_.GetFirstIndex();
        group_->log_.SetStartIndex(req.last_included_index_ + 1);
        group_->last_snapshot_index_ = req.last_included_index_;
        group_->last_snapshot_term_ = req.last_included_term_;

        // Schedule async truncation of persisted log after releasing locks.
        // TruncatePrefix I/O can be slow; performing it asynchronously prevents
        // blocking the Raft event loop while holding manager locks.
        if (log_persister_) {
          NodeId my_id = group_->server_id_;
          log_persister_->TruncatePrefixAsync(req.last_included_index_ + 1, [my_id](Status status) {
            if (!status.ok()) {
              LOG_WARN("Node {} async truncate failed after snapshot: {}", my_id,
                       status.ToString());
            }
          });
        }

        if (metrics_) {
          auto labels = group_->metrics_node_label_;
          labels["trigger"] = "snapshot";
          metrics_->GetCounter("raft_log_compactions_total", labels).Increment();
          if (req.last_included_index_ >= old_first_index) {
            uint64_t compacted = req.last_included_index_ - old_first_index + 1;
            metrics_->GetCounter("raft_log_entries_compacted_total", group_->metrics_node_label_)
                .Increment(compacted);
          }
        }

        // Update indices
        group_->last_applied_.store(req.last_included_index_, std::memory_order_release);
        group_->commit_index_ = req.last_included_index_;

        // Clear async apply queue — entries covered by snapshot are obsolete
        {
          std::lock_guard<std::mutex> lock(group_->apply_queue_mtx_);
          group_->apply_queue_.clear();
          group_->last_enqueued_ = req.last_included_index_;
        }

        // Persist snapshot if persister available
        if (persister_) {
          auto persist_ifs = std::make_shared<std::ifstream>();
          auto persist_initialized = std::make_shared<bool>(false);
          auto persist_provider = [&](std::string& chunk) -> bool {
            if (!*persist_initialized) {
              persist_ifs->open(group_->snapshot_temp_path_, std::ios::binary);
              *persist_initialized = true;
            }
            if (!*persist_ifs) {
              return false;
            }
            constexpr size_t kChunkSize = 64 * 1024;
            chunk.resize(kChunkSize);
            persist_ifs->read(chunk.data(), kChunkSize);
            auto bytes_read = persist_ifs->gcount();
            if (bytes_read <= 0) {
              persist_ifs->close();
              *persist_initialized = false;
              return false;
            }
            chunk.resize(bytes_read);
            return true;
          };
          auto status = persister_->SaveSnapshotStream(persist_provider, req.last_included_index_,
                                                       req.last_included_term_);
          if (!status.ok()) {
            LOG_WARN("Node {} failed to persist snapshot: {}", group_->server_id_,
                     status.ToString());
            // Non-fatal: we can continue, snapshot will be resent if needed
          }
        }

        // Clean up temp file
        if (!group_->snapshot_temp_path_.empty()) {
          if (std::remove(group_->snapshot_temp_path_.c_str()) != 0) {
            LOG_WARN("Node {} failed to remove temp snapshot file: {}", group_->server_id_,
                     group_->snapshot_temp_path_);
          }
          group_->snapshot_temp_path_.clear();
        }

        LOG_INFO(
            "Node {} successfully restored from snapshot, log start={}, "
            "commit_index={}",
            group_->server_id_, group_->log_.GetFirstIndex(), group_->commit_index_);
      } catch (const std::exception& e) {
        // A throwing state machine must not crash the node: log, drop the
        // transfer, and let the leader's retry machinery restart it.
        LOG_ERROR("Node {} exception during snapshot restore: {}", group_->server_id_, e.what());
        if (!group_->snapshot_temp_path_.empty()) {
          if (std::remove(group_->snapshot_temp_path_.c_str()) != 0) {
            LOG_WARN("Node {} failed to remove temp snapshot file: {}", group_->server_id_,
                     group_->snapshot_temp_path_);
          }
          group_->snapshot_temp_path_.clear();
        }
      }
    }
  }
}

void RaftNode::RaftNodeImpl::HandleReadIndexRequest(const ReadIndexRequest& /*req*/,
                                                    ReadIndexResponse& resp) {
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);

  RecordActivityLocked();

  resp.term_ = group_->current_term_;

  if (group_->role_ == RaftNodeRole::LEADER) {
    resp.leader_valid_ = true;
    // group_->commit_index_ is updated under group_->replication_mtx_; acquire it for
    // a consistent read to avoid data races with TryCommitLocked().
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    resp.read_index_ = group_->commit_index_;
  } else {
    resp.leader_valid_ = false;
    resp.read_index_ = 0;
  }
}

void RaftNode::RaftNodeImpl::HandleClientRequest(const ClientRequest& req, ClientResponse& resp) {
  // Bridge pattern: group_->election_mtx_ -> group_->replication_mtx_
  std::unique_lock<std::mutex> lock_e(group_->election_mtx_);
  std::unique_lock<std::mutex> lock_r(group_->replication_mtx_);

  RecordActivityLocked();

  // Check if we are the leader
  if (group_->role_ != RaftNodeRole::LEADER) {
    resp.success = false;
    resp.error = "Not leader";
    resp.leader_id = group_->leader_id_;
    resp.leader_addr = group_->leader_addr_;
    return;
  }

  // For read-only requests, use ReadIndex for linearizable reads.
  // Release locks before calling ReadIndex to avoid deadlock (ReadIndex
  // acquires group_->election_mtx_ -> group_->replication_mtx_ -> group_->membership_mtx_ ->
  // group_->applier_mtx_).
  if (req.read_only) {
    lock_r.unlock();
    lock_e.unlock();

    std::promise<void> read_promise;
    auto future = read_promise.get_future();

    auto status = ReadIndex([&read_promise]() { read_promise.set_value(); });

    if (!status.ok()) {
      lock_e.lock();
      lock_r.lock();
      resp.success = false;
      resp.error = status.GetMessage();
      if (status.IsNotLeader()) {
        resp.leader_id = group_->leader_id_;
        resp.leader_addr = group_->leader_addr_;
      }
      return;
    }

    // Wait for ReadIndex with timeout (use rpc_timeout_ms)
    auto wait_status =
        future.wait_for(std::chrono::milliseconds(infra_->runtime_config_->Get().rpc_timeout_ms));

    lock_e.lock();
    lock_r.lock();

    if (wait_status != std::future_status::ready) {
      resp.success = false;
      resp.error = "ReadIndex timeout";
      return;
    }

    // ReadIndex completed: commit_index has been applied, safe to query.
    auto query_result = group_->state_machine_->Query(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(req.command.data()), req.command.size()));

    resp.success = query_result.success;
    resp.response = query_result.response;
    resp.error = query_result.error_message;
    resp.last_applied_index = query_result.applied_index;
    resp.leader_id = group_->server_id_;
    resp.leader_addr = group_->config_.listen_addr;
    return;
  }

  // Get or create client session for idempotency
  auto& session = group_->client_sessions_[req.client_id];
  session.last_active = std::chrono::steady_clock::now();

  // Case 1: Old request (seq < last_seq) - already executed, return cached
  if (req.seq < session.last_seq) {
    resp.success = true;
    resp.response = session.last_response;
    resp.last_applied_index = session.last_index;
    resp.leader_id = group_->server_id_;
    resp.leader_addr = group_->config_.listen_addr;
    LOG_INFO("Client {} seq {} is old (last={}), returning cached result", req.client_id, req.seq,
             session.last_seq);
    return;
  }

  // Case 2: Duplicate request (seq == last_seq) - return cached result
  if (req.seq == session.last_seq) {
    resp.success = true;
    resp.response = session.last_response;
    resp.last_applied_index = session.last_index;
    resp.leader_id = group_->server_id_;
    resp.leader_addr = group_->config_.listen_addr;
    LOG_INFO("Client {} seq {} is duplicate, returning cached result", req.client_id, req.seq);
    return;
  }

  // Case 3: New request (seq > last_seq) - execute normally.
  // Release group_->election_mtx_ before waiting so that AppendEntries responses
  // (which need group_->election_mtx_) can make progress and trigger commit.
  lock_e.unlock();
  auto result = ProposeAndWaitLocked(req.command, lock_r);
  lock_e.lock();

  // Update session cache if successful
  auto& session2 = group_->client_sessions_[req.client_id];
  if (result.success) {
    session2.last_seq = req.seq;
    session2.last_response = result.response;
    session2.last_index = result.applied_index;
    session2.last_term = group_->current_term_;
  }

  resp.success = result.success;
  resp.response = result.response;
  resp.error = result.error_message;
  resp.last_applied_index = result.applied_index;
  resp.leader_id = group_->server_id_;
  resp.leader_addr = group_->config_.listen_addr;
}
