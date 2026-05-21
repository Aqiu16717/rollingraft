#include <atomic>

#include "asio_timer_service.h"
#include "nlohmann/json.hpp"
#include "raft_node_impl.h"

using namespace rollingraft;

RaftNode::RaftNodeImpl::RaftNodeImpl(
    const RaftNodeConfig& config, std::shared_ptr<StateMachine> state_machine,
    std::unique_ptr<NetworkTransport> network,
    std::unique_ptr<TimerService> timer, std::shared_ptr<Persister> persister,
    std::unique_ptr<Protocol> protocol)
    : config_(config),
      state_machine_(std::move(state_machine)),
      network_(std::move(network)),
      timer_(std::move(timer)),
      persister_(std::move(persister)),
      protocol_(std::move(protocol)) {
  server_id_ = config.node_id;
  peer_addrs_ = config.peers;

  // Build peer map
  bool has_explicit_peer_ids = !config.peer_node_ids.empty();
  if (has_explicit_peer_ids && config.peer_node_ids.size() != peer_addrs_.size()) {
    throw std::invalid_argument("peer_node_ids size must match peers size");
  }
  for (size_t i = 0; i < peer_addrs_.size(); ++i) {
    NodeId peer_id = has_explicit_peer_ids ? config.peer_node_ids[i] : ParseNodeId(peer_addrs_[i]);
    if (peer_id < 0) {
      throw std::invalid_argument("Cannot determine peer_id for address: " + peer_addrs_[i]);
    }
    peer_map_[peer_id] = peer_addrs_[i];
  }

  if (!state_machine_) {
    throw std::invalid_argument("StateMachine cannot be null");
  }
  if (!network_) {
    throw std::invalid_argument("NetworkTransport cannot be null");
  }
  if (!timer_) {
    throw std::invalid_argument("TimerService cannot be null");
  }

  // Initialize metrics if enabled
  if (config.metrics_enabled) {
    metrics_ = std::make_unique<MetricsRegistry>();
  }

  // Initialize runtime config with defaults from RaftNodeConfig
  {
    RuntimeConfig::Values defaults;
    defaults.election_timeout_ms = config.election_timeout_ms;
    defaults.heartbeat_interval_ms = config.heartbeat_interval_ms;
    defaults.max_entries_per_append = config.max_entries_per_append;
    defaults.rpc_timeout_ms = config.rpc_timeout_ms;
    defaults.snapshot_threshold_entries = config.snapshot_threshold_entries;
    defaults.snapshot_threshold_bytes = config.snapshot_threshold_bytes;
    defaults.snapshot_check_interval_ms = config.snapshot_check_interval_ms;
    defaults.max_retry_attempts = config.max_retry_attempts;
    defaults.base_retry_delay_ms = config.base_retry_delay_ms;
    defaults.max_retry_delay_ms = config.max_retry_delay_ms;
    defaults.log_retention_entries = config.log_retention_entries;
    defaults.propose_timeout_ms = config.propose_timeout_ms;
    defaults.max_snapshot_size_bytes = config.max_snapshot_size_bytes;
    runtime_config_ = std::make_unique<RuntimeConfig>(defaults);
  }

  // Configure JSON logging if enabled
  if (config.json_logging) {
    Logger* logger = LoggerFactory::Instance().GetLogger();
    if (logger) {
      logger->ConfigureJsonMode(true, server_id_);
    }
  }

  // Initialize cluster config from peers
  cluster_config_.nodes.push_back(server_id_);
  for (size_t i = 0; i < peer_addrs_.size(); ++i) {
    NodeId peer_id = has_explicit_peer_ids ? config.peer_node_ids[i]
                                           : ParseNodeId(peer_addrs_[i]);
    if (peer_id >= 0) {
      cluster_config_.nodes.push_back(peer_id);
    }
  }
  cluster_config_.version = 1;

  LOG_INFO("RaftNodeImpl created for node {}", server_id_);
}

RaftNode::RaftNodeImpl::~RaftNodeImpl() {
  if (state_ == NodeState::kRunning) {
    Stop();
  }
}

Status RaftNode::RaftNodeImpl::Start() {
  NodeState expected = NodeState::kInitialized;
  if (!state_.compare_exchange_strong(expected, NodeState::kRunning)) {
    return Status::Error("Already started or stopped");
  }

  LOG_INFO("Starting RaftNode {} on {}...", config_.node_id,
           config_.listen_addr);

  // 1. Initialize persistence
  if (persister_) {
    auto status = persister_->Open(config_.data_dir);
    if (!status.ok()) {
      state_ = NodeState::kInitialized;
      return status;
    }

    // Restore persistent state
    PersistentState state;
    if (persister_->LoadState(state).ok()) {
      current_term_ = state.current_term;
      voted_for_ = state.voted_for;
      LOG_INFO("Restored state: term={}, voted_for={}", current_term_,
               voted_for_);
    }

    // Initialize and start LogPersister
    LogPersistenceConfig log_config;
    log_config.batch_size = config_.max_entries_per_append;
    log_config.batch_interval_ms = config_.heartbeat_interval_ms / 2;
    log_config.data_dir = config_.data_dir;

    // Wire ASIO executor for async truncation if using AsioTimerService
    if (auto* asio_timer = dynamic_cast<AsioTimerService*>(timer_.get())) {
      if (auto* io = asio_timer->GetIoContext()) {
        log_config.executor = [io](std::function<void()> fn) {
          asio::post(*io, std::move(fn));
        };
      }
    }

    log_persister_ = std::make_unique<LogPersister>(persister_, log_config);
    log_persister_->Start();

    // Restore log entries from disk
    auto restored_entries = log_persister_->Restore(log_.GetFirstIndex());
    for (const auto& entry : restored_entries) {
      log_.AppendLogEntry(entry);
    }

    // All restored entries are already durably persisted
    flushed_index_ = log_.LastLogIndex();
  }

  // 2. Initialize network layer
  auto handler = [this](NodeId from, const std::string& req,
                        std::string& resp) {
    HandleIncomingRpc(from, req, resp);
  };

  auto status = network_->Initialize(config_.listen_addr, handler);
  if (!status.ok()) {
    if (persister_) persister_->Close();
    state_ = NodeState::kInitialized;
    return status;
  }

  status = network_->Start();
  if (!status.ok()) {
    if (persister_) persister_->Close();
    state_ = NodeState::kInitialized;
    return status;
  }

  // 3. Start timer service
  timer_->Start();

  // 4. Start metrics HTTP server
  if (metrics_ && !config_.metrics_addr.empty()) {
    MetricsHttpServer::TlsConfig tls_config;
    tls_config.enabled = config_.tls_enabled;
    tls_config.cert_file = config_.tls_cert_file;
    tls_config.key_file = config_.tls_key_file;
    tls_config.ca_file = config_.tls_ca_file;
    metrics_server_ = std::make_unique<MetricsHttpServer>(
        config_.metrics_addr, metrics_.get(), tls_config, config_.admin_token);
    metrics_server_->SetStatusProvider([this]() -> std::string {
      // Lock hierarchy: election_mtx_ -> replication_mtx_ -> membership_mtx_
      // -> applier_mtx_. All accessed state must be protected.
      std::lock_guard<std::mutex> lock_e(election_mtx_);
      std::lock_guard<std::mutex> lock_r(replication_mtx_);
      std::shared_lock<std::shared_mutex> lock_m(membership_mtx_);
      std::lock_guard<std::mutex> lock_a(applier_mtx_);

      nlohmann::json j;
      j["node_id"] = server_id_;
      j["role"] = RaftNodeRoleToString(role_);
      j["term"] = current_term_;
      j["leader_id"] = leader_id_;
      j["leader_addr"] = leader_addr_;
      j["commit_index"] = commit_index_;
      j["last_log_index"] = log_.LastLogIndex();
      j["running"] = IsRunning();

      nlohmann::json config_json;
      config_json["version"] = cluster_config_.version;
      nlohmann::json nodes = nlohmann::json::array();
      for (NodeId node_id : cluster_config_.nodes) {
        nlohmann::json node_json;
        node_json["id"] = node_id;
        auto it = peer_map_.find(node_id);
        if (it != peer_map_.end()) {
          node_json["addr"] = it->second;
        } else if (node_id == server_id_) {
          node_json["addr"] = config_.listen_addr;
        } else {
          node_json["addr"] = nullptr;
        }
        nodes.push_back(node_json);
      }
      config_json["nodes"] = nodes;
      j["cluster_config"] = config_json;
      j["last_applied"] = last_applied_;

      return j.dump();
    });

    // Control plane handlers (#19 API implementation)
    metrics_server_->SetAddMemberHandler(
        [this](int32_t node_id, const std::string& addr) -> std::string {
          if (node_id < 0 || addr.empty()) {
            nlohmann::json j;
            j["error"] = "BAD_REQUEST";
            j["message"] = "Invalid node_id or addr";
            return j.dump();
          }
          auto status = AddNode(static_cast<NodeId>(node_id), addr);
          nlohmann::json j;
          if (status.ok()) {
            j["status"] = "accepted";
            j["message"] = "Configuration change proposed";
          } else {
            j["error"] = "NOT_LEADER";
            j["message"] = status.GetMessage();
            if (!GetLeaderAddr().empty()) {
              j["leader_hint"] = GetLeaderAddr();
            }
          }
          return j.dump();
        });

    metrics_server_->SetRemoveMemberHandler(
        [this](int32_t node_id) -> std::string {
          if (node_id < 0) {
            nlohmann::json j;
            j["error"] = "BAD_REQUEST";
            j["message"] = "Invalid node_id";
            return j.dump();
          }
          auto status = RemoveNode(static_cast<NodeId>(node_id));
          nlohmann::json j;
          if (status.ok()) {
            j["status"] = "accepted";
            j["message"] = "Node removal proposed";
          } else {
            j["error"] = "NOT_LEADER";
            j["message"] = status.GetMessage();
            if (!GetLeaderAddr().empty()) {
              j["leader_hint"] = GetLeaderAddr();
            }
          }
          return j.dump();
        });

    metrics_server_->SetTriggerSnapshotHandler([this]() -> std::string {
      auto status = TriggerSnapshot();
      nlohmann::json j;
      if (status.ok()) {
        j["status"] = "triggered";
        j["message"] = "Snapshot creation initiated";
      } else {
        j["error"] = "NOT_LEADER";
        j["message"] = status.GetMessage();
      }
      return j.dump();
    });

    metrics_server_->SetTransferLeadershipHandler(
        [this](int32_t target_id) -> std::string {
          nlohmann::json j;
          if (target_id < 0) {
            j["error"] = "BAD_REQUEST";
            j["message"] = "Invalid target_node_id";
            return j.dump();
          }
          auto status = TransferLeadershipTo(static_cast<NodeId>(target_id));
          if (status.ok()) {
            j["status"] = "initiated";
            j["message"] = "Leadership transfer initiated";
          } else {
            j["error"] = status.IsNotLeader() ? "NOT_LEADER" : "ERROR";
            j["message"] = status.GetMessage();
            if (!GetLeaderAddr().empty()) {
              j["leader_hint"] = GetLeaderAddr();
            }
          }
          return j.dump();
        });

    metrics_server_->SetConfigProvider([this]() -> std::string {
      return runtime_config_ ? runtime_config_->ToJson()
                             : "{\"error\":\"runtime_config_not_initialized\"}";
    });

    metrics_server_->SetConfigUpdater([this](const std::string& json) -> std::string {
      if (!runtime_config_) {
        return "{\"error\":\"runtime_config_not_initialized\"}";
      }
      auto status = runtime_config_->UpdateFromJson(json);
      if (status.ok()) {
        return "{\"status\":\"updated\",\"message\":\"Configuration updated successfully\"}";
      }
      nlohmann::json j;
      j["error"] = "INVALID_CONFIG";
      j["message"] = status.GetMessage();
      return j.dump();
    });

    // Wire EventBus → SSE broadcast
    event_bus_.SubscribeAll([this](const RaftEvent& event) {
      if (!metrics_server_) return;
      nlohmann::json j;
      j["event"] = event.Name();
      j["node_id"] = server_id_;
      j["timestamp_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
      // TODO: serialize event-specific fields based on type
      metrics_server_->BroadcastEvent(j.dump());
    });

    metrics_server_->Start();
  }

  // 5. Enter Follower state
  {
    std::lock_guard<std::mutex> lock_e(election_mtx_);
    BecomeFollowerLocked(current_term_);
  }

  LOG_INFO("RaftNode {} started successfully", config_.node_id);

  NodeLifecycleEvent started_event;
  started_event.node_id = server_id_;
  started_event.state = NodeLifecycleEvent::State::kStarted;
  started_event.timestamp = std::chrono::steady_clock::now();
  event_bus_.Publish(started_event);

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::Stop() {
  NodeState expected = NodeState::kRunning;
  if (!state_.compare_exchange_strong(expected, NodeState::kStopping)) {
    if (state_ == NodeState::kStopped) {
      return Status::OK();  // Already stopped
    }
    return Status::Error("Node not running");
  }

  LOG_INFO("Stopping RaftNode {}...", config_.node_id);

  // 1. Stop metrics server
  if (metrics_server_) {
    metrics_server_->Stop();
    metrics_server_.reset();
  }

  // 2. Stop timers. election_mtx_ must be held when mutating
  // election_timer_ to avoid racing with OnElectionTimeout on the
  // io_context thread.
  {
    std::lock_guard<std::mutex> lock_e(election_mtx_);
    CancelElectionTimerLocked();
  }
  StopHeartbeatTimerLocked();
  StopSnapshotCheckTimerLocked();

  // 3. Stop TimerService
  if (timer_) {
    timer_->Stop();
  }

  // 4. Stop NetworkTransport
  if (network_) {
    network_->Stop();
  }

  // 5. Stop LogPersister (flushes remaining entries)
  if (log_persister_) {
    log_persister_->Stop();
  }

  // 6. Clean up pending proposals
  std::vector<std::pair<Index, std::function<void(const ApplyResult&)>>>
      callbacks_to_run;
  {
    std::lock_guard<std::mutex> lock_r(replication_mtx_);
    for (auto& [id, proposal] : pending_proposals_) {
      callbacks_to_run.emplace_back(id, std::move(proposal.callback));
    }
    pending_proposals_.clear();
  }
  for (auto& [id, callback] : callbacks_to_run) {
    ApplyResult result;
    result.success = false;
    result.error_message = "Node stopped";
    callback(result);
  }

  state_ = NodeState::kStopped;

  NodeLifecycleEvent stopped_event;
  stopped_event.node_id = server_id_;
  stopped_event.state = NodeLifecycleEvent::State::kStopped;
  stopped_event.timestamp = std::chrono::steady_clock::now();
  event_bus_.Publish(stopped_event);

  LOG_INFO("RaftNode {} stopped", config_.node_id);
  return Status::OK();
}

bool RaftNode::RaftNodeImpl::IsLeader() const {
  std::lock_guard<std::mutex> lock(election_mtx_);
  return role_ == RaftNodeRole::LEADER;
}

RaftNodeRole RaftNode::RaftNodeImpl::GetRole() const {
  std::lock_guard<std::mutex> lock(election_mtx_);
  return role_;
}

Term RaftNode::RaftNodeImpl::CurrentTerm() const {
  std::lock_guard<std::mutex> lock(election_mtx_);
  return current_term_;
}

std::string RaftNode::RaftNodeImpl::GetLeaderAddr() const {
  std::lock_guard<std::mutex> lock(election_mtx_);
  return leader_addr_;
}

Index RaftNode::RaftNodeImpl::GetCommitIndex() const {
  std::lock_guard<std::mutex> lock(replication_mtx_);
  return commit_index_;
}

void RaftNode::RaftNodeImpl::SetRoleChangeCallback(
    std::function<void(RaftNodeRole, uint64_t)> cb) {
  role_change_callback_ = std::move(cb);
}

void RaftNode::RaftNodeImpl::SetLeaderChangeCallback(
    std::function<void(NodeId, std::string)> cb) {
  leader_change_callback_ = std::move(cb);
}

Status RaftNode::RaftNodeImpl::Propose(
    const std::string& command,
    std::function<void(const ApplyResult&)> callback) {
  // Bridge pattern: election_mtx_ -> replication_mtx_
  std::lock_guard<std::mutex> lock_e(election_mtx_);
  std::lock_guard<std::mutex> lock_r(replication_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (role_ != RaftNodeRole::LEADER) {
    if (metrics_) {
      metrics_
          ->GetCounter("raft_propose_total",
                       {{"node_id", std::to_string(server_id_)},
                        {"result", "rejected_not_leader"}})
          .Increment();
    }
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  // Append to local log
  auto [index, status] = log_.Append(current_term_, command);
  if (!status.ok()) {
    return status;
  }

  // Persist log entry (async with callback)
  if (log_persister_) {
    auto entry_opt = log_.GetEntry(index);
    if (entry_opt) {
      log_persister_->Append(*entry_opt, [this, index](Status s) {
        if (!s.ok()) {
          LOG_WARN("Node {} log persistence failed for index {}: {}",
                   server_id_, index, s.ToString());
          if (log_persister_ && !log_persister_->IsHealthy()) {
            // Disk failure: step down. Runs on persister thread with no
            // locks held, so acquiring election_mtx_ is safe.
            std::lock_guard<std::mutex> lock_e(election_mtx_);
            if (role_ == RaftNodeRole::LEADER) {
              LOG_ERROR("Node {} stepping down due to disk failure",
                        server_id_);
              BecomeFollowerLocked(current_term_);
            }
          }
          return;
        }
        // Commit/broadcast: runs on persister thread with no locks held.
        std::lock_guard<std::mutex> lock_e(election_mtx_);
        std::lock_guard<std::mutex> lock_r(replication_mtx_);
        if (!IsRunning()) {
          return;
        }
        if (role_ != RaftNodeRole::LEADER) {
          return;
        }
        if (index > flushed_index_) {
          flushed_index_ = index;
        }
        // Retry commit now that this entry is durable
        TryCommitLocked();
        // Replicate to followers
        BroadcastAppendEntriesLocked();
      });
    }
  } else {
    // No persistence configured (test path) — treat as immediately flushed
    flushed_index_ = std::max(flushed_index_, index);
  }

  // Record pending proposal
  PendingProposal proposal;
  proposal.index = index;
  proposal.callback = std::move(callback);
  proposal.propose_time = std::chrono::steady_clock::now();
  pending_proposals_[index] = std::move(proposal);

  if (metrics_) {
    metrics_
        ->GetCounter(
            "raft_propose_total",
            {{"node_id", std::to_string(server_id_)}, {"result", "accepted"}})
        .Increment();
  }

  // Trigger log replication only if no persister (otherwise callback triggers
  // it)
  if (!log_persister_) {
    BroadcastAppendEntriesLocked();
  }

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::ProposeBatch(
    const std::vector<std::string>& commands,
    std::function<void(const std::vector<ApplyResult>& results)> callback) {
  // Bridge pattern: election_mtx_ -> replication_mtx_
  std::lock_guard<std::mutex> lock_e(election_mtx_);
  std::lock_guard<std::mutex> lock_r(replication_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  if (commands.empty()) {
    return Status::Error("Empty batch");
  }

  // Append all commands to the log atomically under the same term
  std::vector<Index> indices;
  indices.reserve(commands.size());

  for (const auto& command : commands) {
    auto [index, status] = log_.Append(current_term_, command);
    if (!status.ok()) {
      // Rollback: truncate all entries appended so far in this batch
      if (!indices.empty()) {
        log_.TruncateSuffix(indices.front());
      }
      return status;
    }
    indices.push_back(index);
  }

  // Shared state for collecting individual results
  auto results = std::make_shared<std::vector<ApplyResult>>(commands.size());
  auto remaining = std::make_shared<std::atomic<size_t>>(commands.size());

  // Register individual callbacks that feed into the batch callback.
  // The last callback to fire posts the batch completion to the timer
  // service thread to avoid invoking user code under lock.
  for (size_t i = 0; i < commands.size(); ++i) {
    Index index = indices[i];

    // Persist log entry (async with callback)
    if (log_persister_) {
      auto entry_opt = log_.GetEntry(index);
      if (entry_opt) {
        log_persister_->Append(*entry_opt, [this, index](Status s) {
          if (!s.ok()) {
            LOG_WARN("Node {} log persistence failed for index {}: {}",
                     server_id_, index, s.ToString());
            if (log_persister_ && !log_persister_->IsHealthy()) {
              // Disk failure: step down. Runs on persister thread with no
              // locks held, so acquiring election_mtx_ is safe.
              std::lock_guard<std::mutex> lock_e(election_mtx_);
              if (role_ == RaftNodeRole::LEADER) {
                LOG_ERROR("Node {} stepping down due to disk failure",
                          server_id_);
                BecomeFollowerLocked(current_term_);
              }
            }
            return;
          }
          // Commit/broadcast: runs on persister thread with no locks held.
          std::lock_guard<std::mutex> lock_e(election_mtx_);
          std::lock_guard<std::mutex> lock_r(replication_mtx_);
          if (!IsRunning()) {
            return;
          }
          if (role_ != RaftNodeRole::LEADER) {
            return;
          }
          if (index > flushed_index_) {
            flushed_index_ = index;
          }
          TryCommitLocked();
          BroadcastAppendEntriesLocked();
        });
      }
    }

    PendingProposal proposal;
    proposal.index = index;
    proposal.callback = [i, results, remaining, callback,
                         timer = timer_.get()](const ApplyResult& result) {
      (*results)[i] = result;
      if (remaining->fetch_sub(1, std::memory_order_acq_rel) == 1) {
        // Last one: post batch completion to timer thread (outside lock)
        timer->SetTimeout(std::chrono::milliseconds(0),
                          [results, callback]() { callback(*results); });
      }
    };
    proposal.propose_time = std::chrono::steady_clock::now();
    pending_proposals_[index] = std::move(proposal);
  }

  // If no persister, treat as immediately flushed and trigger replication
  if (!log_persister_) {
    flushed_index_ = std::max(flushed_index_, indices.back());
    BroadcastAppendEntriesLocked();
    // For single-node clusters, no followers will respond; try commit now
    TryCommitLocked();
  }

  return Status::OK();
}

ApplyResult RaftNode::RaftNodeImpl::ProposeAndWaitLocked(
    const std::string& command, std::unique_lock<std::mutex>& lock_r) {
  // PRECONDITION: caller holds replication_mtx_ (via lock_r).
  // election_mtx_ may or may not be held; this method only accesses
  // replication state (log_, pending_proposals_, flushed_index_).
  // This method unlocks replication_mtx_ while waiting for commit.

  // Use promise/future for synchronous wait
  std::promise<ApplyResult> promise;
  auto future = promise.get_future();

  // Create callback that will set the promise value
  auto callback = [&promise](const ApplyResult& result) {
    promise.set_value(result);
  };

  // Append to local log
  auto [index, status] = log_.Append(current_term_, command);
  if (!status.ok()) {
    ApplyResult error_result;
    error_result.success = false;
    error_result.error_message = status.GetMessage();
    return error_result;
  }

  // Persist log entry synchronously before replication
  if (log_persister_) {
    auto entry_opt = log_.GetEntry(index);
    if (entry_opt) {
      auto flush_status = log_persister_->AppendSync(*entry_opt);
      if (!flush_status.ok()) {
        // Disk failure: let async persister callback handle step-down
        // (Phase 5 migrated persister callbacks to fine-grained locks).
        LOG_ERROR("Node {} disk unhealthy, rejecting command", server_id_);
        ApplyResult error_result;
        error_result.success = false;
        error_result.error_message = flush_status.GetMessage();
        return error_result;
      }
      flushed_index_ = std::max(flushed_index_, index);
    }
  } else {
    flushed_index_ = std::max(flushed_index_, index);
  }

  // Record pending proposal
  PendingProposal proposal;
  proposal.index = index;
  proposal.callback = std::move(callback);
  proposal.propose_time = std::chrono::steady_clock::now();
  pending_proposals_[index] = std::move(proposal);

  // Trigger log replication
  BroadcastAppendEntriesLocked();

  // Unlock replication_mtx_ while waiting to allow commit progress.
  // NOTE: election_mtx_ remains held, which blocks election timeouts.
  // This is a known limitation of synchronous propose.
  struct ReacquireGuard {
    std::unique_lock<std::mutex>& lock;
    bool need_relock = true;
    ~ReacquireGuard() {
      if (need_relock && !lock.owns_lock()) {
        lock.lock();
      }
    }
  } guard{lock_r};

  lock_r.unlock();

  // Wait for commit and apply with timeout
  auto cfg = runtime_config_->Get();
  auto wait_status = future.wait_for(
      std::chrono::milliseconds(cfg.propose_timeout_ms));

  lock_r.lock();
  guard.need_relock = false;

  if (wait_status == std::future_status::timeout) {
    if (metrics_) {
      metrics_
          ->GetCounter(
              "raft_propose_total",
              {{"node_id", std::to_string(server_id_)}, {"result", "timeout"}})
          .Increment();
    }
    // Remove pending proposal on timeout
    pending_proposals_.erase(index);
    ApplyResult timeout_result;
    timeout_result.success = false;
    timeout_result.error_message = "Command execution timeout";
    return timeout_result;
  }

  return future.get();
}

Status RaftNode::RaftNodeImpl::ReadIndex(std::function<void()> callback) {
  // Phase 1: Election state check under election_mtx_ only.
  std::lock_guard<std::mutex> lock_e(election_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  // Phase 2: ReadIndex work under full hierarchy.
  {
    std::lock_guard<std::mutex> lock_r(replication_mtx_);
    std::shared_lock<std::shared_mutex> lock_m(membership_mtx_);
    std::lock_guard<std::mutex> lock_a(applier_mtx_);

    // Create pending read request
    uint64_t read_id = next_read_id_++;
    PendingReadIndex read_req;
    read_req.read_index = commit_index_;
    read_req.callback = std::move(callback);
    read_req.start_time = std::chrono::steady_clock::now();
    read_req.acks.insert(server_id_);  // Leader acknowledges itself

    pending_reads_[read_id] = std::move(read_req);

    LOG_INFO("Node {} ReadIndex request {} at commit_index {}", server_id_,
             read_id, commit_index_);

    if (metrics_) {
      metrics_
          ->GetCounter("raft_readindex_total",
                       {{"node_id", std::to_string(server_id_)}})
          .Increment();
    }

    // Send heartbeats to confirm leadership
    BroadcastReadIndexHeartbeatsLocked(read_id);
  }

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::AddNode(NodeId id, const NodeAddr& addr) {
  // Bridge pattern: election_mtx_ -> replication_mtx_ -> membership_mtx_
  std::lock_guard<std::mutex> lock_e(election_mtx_);
  std::lock_guard<std::mutex> lock_r(replication_mtx_);
  std::unique_lock<std::shared_mutex> lock_m(membership_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  // Check if node already exists
  if (cluster_config_.Contains(id)) {
    return Status::Error("Node already in cluster");
  }

  // Single-node-change safety: reject if another change is in flight.
  if (pending_config_change_) {
    return Status::Error(
        "A membership change is already in progress; wait for it to commit");
  }

  // Create config change entry as a special command
  std::string cmd = "CONFIG_CHANGE:ADD:" + std::to_string(id) + ":" + addr;

  // Propose as normal log entry
  auto [index, status] = log_.Append(current_term_, cmd);
  if (!status.ok()) {
    return status;
  }

  // Persist log entry synchronously for configuration changes
  if (log_persister_) {
    auto entry_opt = log_.GetEntry(index);
    if (entry_opt) {
      auto flush_status = log_persister_->AppendSync(*entry_opt);
      if (!flush_status.ok()) {
        LOG_ERROR("Node {} failed to persist AddNode log entry: {}", server_id_,
                  flush_status.GetMessage());
        return flush_status;
      }
    }
  }

  // Mark pending so no other membership change can be proposed.
  pending_config_change_ = true;

  // Add to peer map immediately (optimistic) so the leader starts
  // replicating to the new node right away.
  peer_map_[id] = addr;
  next_index_[id] = log_.GetLastLogInfo().first + 1;
  match_index_[id] = 0;

  LOG_INFO("Node {} proposing AddNode for {} at index {}", server_id_, id,
           index);

  // Trigger replication
  BroadcastAppendEntriesLocked();

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::RemoveNode(NodeId id) {
  // Bridge pattern: election_mtx_ -> replication_mtx_ -> membership_mtx_
  std::lock_guard<std::mutex> lock_e(election_mtx_);
  std::unique_lock<std::mutex> lock_r(replication_mtx_);
  std::unique_lock<std::shared_mutex> lock_m(membership_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  // Check if node exists
  if (!cluster_config_.Contains(id)) {
    return Status::Error("Node not in cluster");
  }

  // Single-node-change safety: reject if another change is in flight.
  if (pending_config_change_) {
    return Status::Error(
        "A membership change is already in progress; wait for it to commit");
  }

  // Prevent removing ourselves while leader
  // (We should step down first)
  if (id == server_id_) {
    LOG_WARN("Node {} removing itself from cluster - will step down", id);
  }

  // Create config change entry
  std::string cmd = "CONFIG_CHANGE:REMOVE:" + std::to_string(id);

  // Propose as normal log entry
  auto [index, status] = log_.Append(current_term_, cmd);
  if (!status.ok()) {
    return status;
  }

  // Persist log entry synchronously for configuration changes
  if (log_persister_) {
    auto entry_opt = log_.GetEntry(index);
    if (entry_opt) {
      auto flush_status = log_persister_->AppendSync(*entry_opt);
      if (!flush_status.ok()) {
        LOG_ERROR("Node {} failed to persist RemoveNode log entry: {}",
                  server_id_, flush_status.GetMessage());
        return flush_status;
      }
    }
  }

  // Mark pending so no other membership change can be proposed.
  pending_config_change_ = true;

  // Remove from peer map immediately (optimistic)
  peer_map_.erase(id);
  next_index_.erase(id);
  match_index_.erase(id);

  // Remove from peer_addrs_
  peer_addrs_.erase(std::remove_if(peer_addrs_.begin(), peer_addrs_.end(),
                                   [id, this](const NodeAddr& a) {
                                     return ParseNodeId(a) == id;
                                   }),
                    peer_addrs_.end());

  LOG_INFO("Node {} proposing RemoveNode for {} at index {}", server_id_, id,
           index);

  // Trigger replication
  BroadcastAppendEntriesLocked();

  // If removing ourselves, step down.
  // Must drop downstream locks before calling BecomeFollowerLocked,
  // which acquires replication_mtx_ + snapshot_mtx_ internally.
  if (id == server_id_) {
    lock_m.unlock();
    lock_r.unlock();
    BecomeFollowerLocked(current_term_);
  }

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::TransferLeadershipTo(NodeId target_id) {
  std::lock_guard<std::mutex> lock_e(election_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(leader_id_, leader_addr_);
  }

  if (target_id == server_id_) {
    return Status::Error("Cannot transfer leadership to self");
  }

  if (peer_map_.find(target_id) == peer_map_.end()) {
    return Status::Error("Target node not in cluster");
  }

  // Send one final AppendEntries to target to ensure its log is up-to-date
  {
    std::lock_guard<std::mutex> lock_r(replication_mtx_);
    SendAppendEntriesToPeerLocked(target_id);
  }

  LOG_INFO("Node {} transferring leadership to {}, stepping down", server_id_,
           target_id);

  // Step down. BecomeFollowerLocked acquires replication_mtx_ + snapshot_mtx_
  // internally, so we must not hold them here.
  BecomeFollowerLocked(current_term_);

  return Status::OK();
}

ClusterConfig RaftNode::RaftNodeImpl::GetConfig() const {
  std::shared_lock<std::shared_mutex> lock(membership_mtx_);
  return cluster_config_;
}

uint64_t RaftNode::RaftNodeImpl::GetLogTermLocked(uint64_t index) {
  if (index == 0) return 0;
  return log_.GetLogTerm(index);
}

NodeId RaftNode::RaftNodeImpl::ParseNodeId(const NodeAddr& addr) {
  // Simple parsing: extract port number as ID from address
  // In production, should use configured node_id mapping
  auto pos = addr.find(':');
  if (pos == std::string::npos) return -1;
  try {
    return static_cast<NodeId>(std::stoi(addr.substr(pos + 1)));
  } catch (...) {
    return -1;
  }
}
