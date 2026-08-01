#include <atomic>

#include "rollingraft/log_persister.h"

#include "asio_timer_service.h"
#include "nlohmann/json.hpp"
#include "raft_node_impl.h"

using namespace rollingraft;

RaftNode::RaftNodeImpl::RaftNodeImpl(const RaftNodeConfig& config,
                                     std::shared_ptr<StateMachine> state_machine,
                                     std::shared_ptr<SharedNodeInfra> infra,
                                     std::shared_ptr<Persister> persister, uint64_t group_id,
                                     bool manage_network)
    : group_(std::make_shared<RaftGroup>(group_id, config, std::move(state_machine))),
      infra_(std::move(infra)),
      persister_(std::move(persister)),
      manage_network_(manage_network) {
  if (!infra_) {
    throw std::invalid_argument("SharedNodeInfra cannot be null");
  }
  if (!infra_->network_) {
    throw std::invalid_argument("NetworkTransport cannot be null");
  }
  if (!infra_->timer_) {
    throw std::invalid_argument("TimerService cannot be null");
  }
  if (!infra_->protocol_) {
    throw std::invalid_argument("Protocol cannot be null");
  }

  if (persister_) {
    persister_->SetCompressionType(
        static_cast<Persister::CompressionType>(config.compression_type));
  }

  // Initialize metrics if enabled. In multi-raft mode the store shares one
  // registry across all groups, so only create it once — overwriting it would
  // leave earlier groups holding a dangling cached metrics_ pointer.
  if (config.metrics_enabled && !infra_->metrics_) {
    infra_->metrics_ = std::make_unique<MetricsRegistry>();
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
    defaults.leader_lease_enabled = config.leader_lease_enabled;
    defaults.transport_batching_enabled = config.transport_batching_enabled;
    infra_->runtime_config_ = std::make_unique<RuntimeConfig>(defaults);
  }

  // Cache raw pointers into infra_ so the rest of the implementation can
  // keep using network_->, timer_->, metrics_->, etc.
  network_ = infra_->network_.get();
  timer_ = infra_->timer_.get();
  protocol_ = infra_->protocol_.get();
  metrics_ = infra_->metrics_.get();
  metrics_server_ = infra_->metrics_server_.get();
  runtime_config_ = infra_->runtime_config_.get();

  // Configure JSON logging if enabled
  if (config.json_logging) {
    Logger* logger = LoggerFactory::Instance().GetLogger();
    if (logger) {
      logger->ConfigureJsonMode(true, group_->server_id_);
    }
  }

  LOG_INFO("RaftNodeImpl created for node {}", group_->server_id_);
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

  LOG_INFO("Starting RaftNode {} on {}...", group_->config_.node_id, group_->config_.listen_addr);

  // 1. Initialize persistence
  if (persister_) {
    auto status = persister_->Open(group_->config_.data_dir);
    if (!status.ok()) {
      state_ = NodeState::kInitialized;
      return status;
    }

    // Restore persistent state
    PersistentState state;
    if (persister_->LoadState(state).ok()) {
      group_->current_term_ = state.current_term;
      group_->voted_for_ = state.voted_for;
      LOG_INFO("Restored state: term={}, voted_for={}", group_->current_term_, group_->voted_for_);
    }

    // Restore snapshot if exists
    if (persister_->HasSnapshot()) {
      uint64_t snapshot_index = 0;
      uint64_t snapshot_term = 0;
      std::string snapshot_data;
      auto status = persister_->LoadSnapshot(snapshot_data, snapshot_index, snapshot_term);
      if (status.ok() && !snapshot_data.empty()) {
        std::vector<uint8_t> snapshot_bytes(snapshot_data.begin(), snapshot_data.end());
        if (group_->state_machine_->Restore(snapshot_bytes)) {
          group_->last_snapshot_index_ = snapshot_index;
          group_->last_snapshot_term_ = snapshot_term;
          group_->log_.SetStartIndex(snapshot_index + 1);
          LOG_INFO("Restored snapshot: index={}, term={}", snapshot_index, snapshot_term);
        } else {
          LOG_ERROR("Failed to restore snapshot from persister");
          // Continue without snapshot — log entries may be able to rebuild
          // state, but this is a degraded path.
        }
      } else {
        LOG_WARN("Snapshot exists but could not be loaded: {}", status.GetMessage());
      }
    }

    // Initialize and start LogPersister
    LogPersistenceConfig log_config;
    log_config.batch_size = group_->config_.max_entries_per_append;
    auto rc = infra_->runtime_config_->Get();
    log_config.batch_interval_ms = rc.heartbeat_interval_ms / 2;
    log_config.data_dir = group_->config_.data_dir;

    // Wire ASIO executor for async truncation if using AsioTimerService
    if (auto* asio_timer = dynamic_cast<AsioTimerService*>(timer_)) {
      if (auto* io = asio_timer->GetIoContext()) {
        log_config.executor = [io](std::function<void()> fn) { asio::post(*io, std::move(fn)); };
      }
    }

    log_persister_ = std::make_unique<LogPersister>(persister_, log_config, metrics_);
    log_persister_->Start();

    // Restore log entries from disk.
    // Use the log's current first index (which may have been set by snapshot
    // restore above) to avoid loading entries already covered by snapshot.
    auto restored_entries = log_persister_->Restore(group_->log_.GetFirstIndex());
    if (!restored_entries.empty()) {
      // Ensure group_->log_.start_index_ matches the first restored entry's index.
      // This handles the case where TruncatePrefix deleted entries but
      // start_index_ was not persisted.
      if (restored_entries[0].index_ != group_->log_.GetFirstIndex()) {
        group_->log_.SetStartIndex(restored_entries[0].index_);
      }
      for (const auto& entry : restored_entries) {
        group_->log_.AppendLogEntry(entry);
      }
    }

    // All restored entries are already durably persisted
    group_->flushed_index_ = group_->log_.LastLogIndex();
  }

  if (manage_network_) {
    // 2. Initialize network layer
    auto handler = [this](NodeId from, const std::string& req, std::string& resp) {
      HandleIncomingRpc(from, req, resp);
    };

    auto status = infra_->network_->Initialize(group_->config_.listen_addr, handler);
    if (!status.ok()) {
      if (persister_) {
        persister_->Close();
      }
      state_ = NodeState::kInitialized;
      return status;
    }

    infra_->network_->SetBatchingEnabled(infra_->runtime_config_->Get().transport_batching_enabled);

    status = infra_->network_->Start();
    if (!status.ok()) {
      if (persister_) {
        persister_->Close();
      }
      state_ = NodeState::kInitialized;
      return status;
    }

    // Set up transport layer metrics callback
    if (metrics_) {
      infra_->network_->SetConnectionCallback(
          [this](NodeId peer_id, const NodeAddr& /*addr*/, bool connected) {
            infra_->metrics_
                ->GetGauge("raft_transport_peer_connected", {{"peer_id", std::to_string(peer_id)}})
                .Set(connected ? 1.0 : 0.0);
            metrics_
                ->GetCounter("raft_transport_connections_total",
                             {{"peer_id", std::to_string(peer_id)},
                              {"status", connected ? "connected" : "disconnected"}})
                .Increment();
          });
      infra_->network_->SetPeerStateCallback([this](NodeId peer_id, int state) {
        infra_->metrics_->GetGauge("transport_peer_state", {{"peer_id", std::to_string(peer_id)}})
            .Set(static_cast<double>(state));
      });
    }

    // 3. Start timer service
    infra_->timer_->Start();

    // 3a. For the legacy single-group path, drive group-local timeouts from a
    // single coarse tick instead of registering three independent timers.
    if (manage_network_) {
      tick_timer_ =
          infra_->timer_->SetInterval(std::chrono::milliseconds(10), [this]() { OnStoreTick(); });
    }

    // 4. Start metrics HTTP server. One server per store: only the first
    // group to start creates it. Groups are distinguished by the group_id
    // label in metrics and status JSON, not by per-group servers.
    if (metrics_ && !group_->config_.metrics_addr.empty() && !infra_->metrics_server_) {
      MetricsHttpServer::TlsConfig tls_config;
      tls_config.enabled = group_->config_.tls_enabled;
      tls_config.cert_file = group_->config_.tls_cert_file;
      tls_config.key_file = group_->config_.tls_key_file;
      tls_config.ca_file = group_->config_.tls_ca_file;
      infra_->metrics_server_ = std::make_unique<MetricsHttpServer>(
          group_->config_.metrics_addr, metrics_, tls_config, group_->config_.admin_token);
      metrics_server_ = infra_->metrics_server_.get();

      // Status/admin handlers and the SSE event broadcast capture this group,
      // so they are only wired in the legacy single-group path. In multi-raft
      // mode the server is shared store infrastructure: it outlives individual
      // groups and is stopped by RaftStore::Stop(). /metrics works off the
      // shared registry; store-level status/admin providers are a follow-up.
      if (manage_network_) {
        infra_->metrics_server_->SetStatusProvider([this]() -> std::string {
          // Lock hierarchy: group_->election_mtx_ -> group_->replication_mtx_ ->
          // group_->membership_mtx_
          // -> group_->applier_mtx_. All accessed state must be protected.
          std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
          std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
          std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
          std::lock_guard<std::mutex> lock_a(group_->applier_mtx_);

          nlohmann::json j;
          j["node_id"] = group_->server_id_;
          j["group_id"] = group_->group_id_;
          j["role"] = RaftNodeRoleToString(group_->role_);
          j["term"] = group_->current_term_;
          j["leader_id"] = group_->leader_id_;
          j["leader_addr"] = group_->leader_addr_;
          j["commit_index"] = group_->commit_index_;
          j["last_log_index"] = group_->log_.LastLogIndex();
          j["running"] = IsRunning();

          nlohmann::json config_json;
          config_json["version"] = group_->cluster_config_.version;
          nlohmann::json nodes = nlohmann::json::array();
          for (NodeId node_id : group_->cluster_config_.nodes) {
            nlohmann::json node_json;
            node_json["id"] = node_id;
            auto it = group_->peer_map_.find(node_id);
            if (it != group_->peer_map_.end()) {
              node_json["addr"] = it->second;
            } else if (node_id == group_->server_id_) {
              node_json["addr"] = group_->config_.listen_addr;
            } else {
              node_json["addr"] = nullptr;
            }
            nodes.push_back(node_json);
          }
          config_json["nodes"] = nodes;
          j["cluster_config"] = config_json;
          j["last_applied"] = group_->last_applied_.load(std::memory_order_acquire);

          return j.dump();
        });

        // Control plane handlers (#19 API implementation)
        infra_->metrics_server_->SetAddMemberHandler(
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

        infra_->metrics_server_->SetRemoveMemberHandler([this](int32_t node_id) -> std::string {
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

        infra_->metrics_server_->SetTriggerSnapshotHandler([this]() -> std::string {
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

        infra_->metrics_server_->SetTransferLeadershipHandler(
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

        infra_->metrics_server_->SetConfigProvider([this]() -> std::string {
          return runtime_config_ ? infra_->runtime_config_->ToJson()
                                 : "{\"error\":\"runtime_config_not_initialized\"}";
        });

        infra_->metrics_server_->SetConfigUpdater([this](const std::string& json) -> std::string {
          if (!runtime_config_) {
            return "{\"error\":\"runtime_config_not_initialized\"}";
          }
          auto old_cfg = infra_->runtime_config_->Get();
          auto status = infra_->runtime_config_->UpdateFromJson(json);
          if (status.ok()) {
            auto new_cfg = infra_->runtime_config_->Get();
            if (new_cfg.transport_batching_enabled != old_cfg.transport_batching_enabled) {
              infra_->network_->SetBatchingEnabled(new_cfg.transport_batching_enabled);
            }
            return "{\"status\":\"updated\",\"message\":\"Configuration updated successfully\"}";
          }
          nlohmann::json j;
          j["error"] = "INVALID_CONFIG";
          j["message"] = status.GetMessage();
          return j.dump();
        });

        // Wire EventBus → SSE broadcast
        event_bus_.SubscribeAll([this](const RaftEvent& event) {
          if (!metrics_server_) {
            return;
          }
          nlohmann::json j;
          j["event"] = event.Name();
          j["node_id"] = group_->server_id_;
          j["timestamp_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now().time_since_epoch())
                                  .count();

          // Serialize event-specific fields based on type
          if (event.Is<NodeRoleChangedEvent>()) {
            const auto& e = event.As<NodeRoleChangedEvent>();
            j["old_role"] = RaftNodeRoleToString(e.old_role);
            j["new_role"] = RaftNodeRoleToString(e.new_role);
            j["term"] = e.term;
          } else if (event.Is<LeaderChangedEvent>()) {
            const auto& e = event.As<LeaderChangedEvent>();
            j["old_leader_id"] = e.old_leader_id;
            j["new_leader_id"] = e.new_leader_id;
            j["new_leader_addr"] = e.new_leader_addr;
            j["term"] = e.term;
          } else if (event.Is<MembershipChangedEvent>()) {
            const auto& e = event.As<MembershipChangedEvent>();
            j["change_type"] =
                (e.change_type == MembershipChangedEvent::ChangeType::kAdd) ? "add" : "remove";
            j["target_node_id"] = e.target_node_id;
            j["target_node_addr"] = e.target_node_addr;
            j["config_version"] = e.config_version;
            j["term"] = e.term;
          } else if (event.Is<LogCompactedEvent>()) {
            const auto& e = event.As<LogCompactedEvent>();
            j["snapshot_index"] = e.snapshot_index;
            j["snapshot_term"] = e.snapshot_term;
            j["bytes_reclaimed"] = e.bytes_reclaimed;
            j["log_size_before"] = e.log_size_before;
            j["log_size_after"] = e.log_size_after;
          } else if (event.Is<SnapshotInstalledEvent>()) {
            const auto& e = event.As<SnapshotInstalledEvent>();
            j["from_leader_id"] = e.from_leader_id;
            j["snapshot_index"] = e.snapshot_index;
            j["snapshot_term"] = e.snapshot_term;
            j["bytes_received"] = e.bytes_received;
            j["success"] = e.success;
            if (!e.error_message.empty()) {
              j["error_message"] = e.error_message;
            }
          } else if (event.Is<ProposalCommittedEvent>()) {
            const auto& e = event.As<ProposalCommittedEvent>();
            j["index"] = e.index;
            j["term"] = e.term;
          } else if (event.Is<ProposalAppliedEvent>()) {
            const auto& e = event.As<ProposalAppliedEvent>();
            j["index"] = e.index;
            j["term"] = e.term;
            j["success"] = e.success;
          } else if (event.Is<ElectionTimeoutEvent>()) {
            const auto& e = event.As<ElectionTimeoutEvent>();
            j["current_term"] = e.current_term;
            j["current_role"] = RaftNodeRoleToString(e.current_role);
          } else if (event.Is<NodeLifecycleEvent>()) {
            const auto& e = event.As<NodeLifecycleEvent>();
            j["state"] = (e.state == NodeLifecycleEvent::State::kStarted) ? "started" : "stopped";
          }

          infra_->metrics_server_->BroadcastEvent(j.dump());
        });
      }

      infra_->metrics_server_->Start();
    }
  }

  // 5. Enter Follower state
  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
    BecomeFollowerLocked(group_->current_term_);
  }

  // Start async apply thread
  group_->apply_running_.store(true, std::memory_order_release);
  group_->apply_thread_ = std::thread(&RaftNodeImpl::ApplyLoop, this);

  LOG_INFO("RaftNode {} started successfully", group_->config_.node_id);

  NodeLifecycleEvent started_event;
  started_event.node_id = group_->server_id_;
  started_event.state = NodeLifecycleEvent::State::kStarted;
  started_event.timestamp = std::chrono::steady_clock::now();
  event_bus_.Publish(started_event);

  return Status::OK();
}

void RaftNode::RaftNodeImpl::DoGracefulShutdown() {
  // 1. Stop metrics server. In multi-raft mode (!manage_network_) the server
  // is shared store infrastructure owned by RaftStore::Stop, not by any group.
  if (manage_network_ && metrics_server_) {
    infra_->metrics_server_->Stop();
    infra_->metrics_server_.reset();
    metrics_server_ = nullptr;
  }

  // 2. Stop timers. group_->election_mtx_ must be held when mutating
  // election timer state to avoid racing with OnStoreTick on the io_context thread.
  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
    CancelElectionTimerLocked();
  }
  {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    StopHeartbeatTimerLocked();
  }
  {
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    StopSnapshotCheckTimerLocked();
  }

  // 2a. Cancel the legacy single-group tick timer before stopping the service.
  if (manage_network_ && tick_timer_ != 0 && timer_) {
    timer_->CancelTimer(tick_timer_);
    tick_timer_ = 0;
  }

  // 3. Stop TimerService (only if this wrapper owns the shared infra)
  if (manage_network_ && timer_) {
    infra_->timer_->Stop();
  }

  // 4. Stop NetworkTransport (only if this wrapper owns the shared infra)
  if (manage_network_ && network_) {
    auto status = infra_->network_->Stop();
    if (!status.ok()) {
      LOG_WARN("NetworkTransport stop failed: {}", status.ToString());
    }
  }

  // 5. Stop LogPersister (flushes remaining entries)
  if (log_persister_) {
    log_persister_->Stop();
  }

  // 6. Stop async apply thread and drain remaining queue
  group_->apply_running_.store(false, std::memory_order_release);
  group_->apply_queue_cv_.notify_all();
  if (group_->apply_thread_.joinable()) {
    group_->apply_thread_.join();
  }
  // Drain any tasks that weren't processed
  std::deque<RaftGroup::ApplyTask> remaining;
  {
    std::lock_guard<std::mutex> lock(group_->apply_queue_mtx_);
    remaining = std::move(group_->apply_queue_);
  }
  for (auto& task : remaining) {
    if (task.callback) {
      ApplyResult result;
      result.success = false;
      result.error_message = "Node stopped";
      task.callback(result);
    }
  }

  // 7. Clean up pending proposals
  std::vector<std::pair<Index, std::function<void(const ApplyResult&)>>> callbacks_to_run;
  {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    for (auto& [id, proposal] : group_->pending_proposals_) {
      callbacks_to_run.emplace_back(id, std::move(proposal.callback));
    }
    group_->pending_proposals_.clear();
  }
  for (auto& [id, callback] : callbacks_to_run) {
    ApplyResult result;
    result.success = false;
    result.error_message = "Node stopped";
    callback(result);
  }

  state_ = NodeState::kStopped;

  NodeLifecycleEvent stopped_event;
  stopped_event.node_id = group_->server_id_;
  stopped_event.state = NodeLifecycleEvent::State::kStopped;
  stopped_event.timestamp = std::chrono::steady_clock::now();
  event_bus_.Publish(stopped_event);

  LOG_INFO("RaftNode {} stopped", group_->config_.node_id);
}

void RaftNode::RaftNodeImpl::ForceShutdown() {
  LOG_WARN("Force shutdown RaftNode {}...", group_->config_.node_id);

  // Force-set state to stopped. Any in-flight callbacks or threads
  // will see kStopped on their next IsRunning() check.
  state_ = NodeState::kStopped;

  // Best-effort cleanup: release resources without blocking. The shared
  // metrics server is store-owned in multi-raft mode (!manage_network_).
  if (manage_network_) {
    infra_->metrics_server_.reset();
    metrics_server_ = nullptr;
  }
  {
    std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
    CancelElectionTimerLocked();
  }
  {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    StopHeartbeatTimerLocked();
  }
  {
    std::lock_guard<std::mutex> lock_s(group_->snapshot_mtx_);
    StopSnapshotCheckTimerLocked();
  }

  if (manage_network_ && tick_timer_ != 0 && timer_) {
    timer_->CancelTimer(tick_timer_);
    tick_timer_ = 0;
  }

  // Note: We intentionally do NOT call infra_->timer_->Stop() or infra_->network_->Stop()
  // here because their Stop() joins the io threads, and those joins may be exactly
  // what is hanging. The keep-alive held by the shutdown thread in Stop() keeps
  // this group alive until the detached DoGracefulShutdown eventually returns.

  if (log_persister_) {
    log_persister_->Stop();
  }

  // Clear pending proposals without invoking callbacks (force shutdown).
  {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    group_->pending_proposals_.clear();
  }

  NodeLifecycleEvent stopped_event;
  stopped_event.node_id = group_->server_id_;
  stopped_event.state = NodeLifecycleEvent::State::kStopped;
  stopped_event.timestamp = std::chrono::steady_clock::now();
  event_bus_.Publish(stopped_event);

  LOG_WARN("RaftNode {} force-stopped", group_->config_.node_id);
}

Status RaftNode::RaftNodeImpl::Stop() {
  NodeState expected = NodeState::kRunning;
  if (!state_.compare_exchange_strong(expected, NodeState::kStopping)) {
    if (state_ == NodeState::kStopped) {
      return Status::OK();  // Already stopped
    }
    return Status::Error("Node not running");
  }

  LOG_INFO("Stopping RaftNode {}...", group_->config_.node_id);

  if (group_->config_.shutdown_timeout_ms == 0) {
    // No timeout — block indefinitely (legacy behavior)
    DoGracefulShutdown();
    return Status::OK();
  }

  // Run graceful shutdown in a separate thread with timeout. The thread
  // holds a keep-alive on this group and a shared_ptr to the done flag:
  // if the timeout fires we detach and return, and the detached thread must
  // not touch the caller's (already destroyed) stack frame or a freed group.
  auto done = std::make_shared<std::atomic<bool>>(false);
  std::thread shutdown_thread([keep_alive = shared_from_this(), done]() {
    keep_alive->DoGracefulShutdown();
    done->store(true, std::memory_order_release);
  });

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(group_->config_.shutdown_timeout_ms);

  while (!done->load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() >= deadline) {
      LOG_ERROR("RaftNode {} graceful shutdown timed out after {}ms, forcing stop",
                group_->config_.node_id, group_->config_.shutdown_timeout_ms);
      shutdown_thread.detach();
      ForceShutdown();
      return Status::Error("Shutdown timeout");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  shutdown_thread.join();
  return Status::OK();
}

bool RaftNode::RaftNodeImpl::IsLeader() const {
  std::lock_guard<std::mutex> lock(group_->election_mtx_);
  return group_->role_ == RaftNodeRole::LEADER;
}

RaftNodeRole RaftNode::RaftNodeImpl::GetRole() const {
  std::lock_guard<std::mutex> lock(group_->election_mtx_);
  return group_->role_;
}

Term RaftNode::RaftNodeImpl::CurrentTerm() const {
  std::lock_guard<std::mutex> lock(group_->election_mtx_);
  return group_->current_term_;
}

std::string RaftNode::RaftNodeImpl::GetLeaderAddr() const {
  std::lock_guard<std::mutex> lock(group_->election_mtx_);
  return group_->leader_addr_;
}

Index RaftNode::RaftNodeImpl::GetCommitIndex() const {
  std::lock_guard<std::mutex> lock(group_->replication_mtx_);
  return group_->commit_index_;
}

void RaftNode::RaftNodeImpl::SetRoleChangeCallback(std::function<void(RaftNodeRole, uint64_t)> cb) {
  group_->role_change_callback_ = std::move(cb);
}

void RaftNode::RaftNodeImpl::SetLeaderChangeCallback(std::function<void(NodeId, std::string)> cb) {
  group_->leader_change_callback_ = std::move(cb);
}

Status RaftNode::RaftNodeImpl::Propose(const std::string& command,
                                       std::function<void(const ApplyResult&)> callback,
                                       uint64_t session_id, uint64_t seq_num) {
  // Bridge pattern: group_->election_mtx_ -> group_->replication_mtx_
  std::unique_lock<std::mutex> lock_e(group_->election_mtx_);
  std::unique_lock<std::mutex> lock_r(group_->replication_mtx_);

  RecordActivityLocked();

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (group_->role_ != RaftNodeRole::LEADER) {
    if (metrics_) {
      auto labels = group_->metrics_node_label_;
      labels["result"] = "rejected_not_leader";
      metrics_->GetCounter("raft_propose_total", labels).Increment();
    }
    return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
  }

  // Client session deduplication
  if (session_id != 0 && group_->session_manager_) {
    SessionResult cached;
    if (group_->session_manager_->IsDuplicate(session_id, seq_num, cached)) {
      // Return cached result asynchronously via callback
      ApplyResult result;
      result.success = cached.success;
      result.response = cached.response;
      result.applied_index = cached.applied_index;
      result.error_message = cached.error_message;
      if (callback) {
        // Post callback to avoid invoking under lock
        auto cb = std::move(callback);
        lock_r.unlock();
        lock_e.unlock();
        cb(result);
      }
      if (metrics_) {
        auto labels = group_->metrics_node_label_;
        labels["result"] = "deduplicated";
        metrics_->GetCounter("raft_propose_total", labels).Increment();
      }
      return Status::OK();
    }
  }

  // Append to local log
  auto [index, status] = group_->log_.Append(group_->current_term_, command);
  if (!status.ok()) {
    return status;
  }

  // Track session info for this proposal if applicable
  if (session_id != 0) {
    group_->proposal_sessions_[index] = {session_id, seq_num};
  }

  // Persist log entry (async with callback)
  if (log_persister_) {
    auto entry_opt = group_->log_.GetEntry(index);
    if (entry_opt) {
      log_persister_->Append(*entry_opt, [weak_self = weak_from_this(), index](Status s) {
        auto self = weak_self.lock();
        if (!self) {
          return;
        }
        self->OnLogEntryPersisted(index, s);
      });
    }
  } else {
    // No persistence configured (test path) — treat as immediately flushed
    group_->flushed_index_ = std::max(group_->flushed_index_, index);
  }

  // Record pending proposal
  PendingProposal proposal;
  proposal.index = index;
  proposal.callback = std::move(callback);
  proposal.propose_time = std::chrono::steady_clock::now();
  group_->pending_proposals_[index] = std::move(proposal);

  if (metrics_) {
    auto labels = group_->metrics_node_label_;
    labels["result"] = "accepted";
    metrics_->GetCounter("raft_propose_total", labels).Increment();
  }

  // Broadcast immediately — replication is decoupled from persistence (T2)
  BroadcastAppendEntriesLocked();

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::ProposeBatch(
    const std::vector<std::string>& commands,
    std::function<void(const std::vector<ApplyResult>& results)> callback) {
  // Bridge pattern: group_->election_mtx_ -> group_->replication_mtx_
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);

  RecordActivityLocked();

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (group_->role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
  }

  if (commands.empty()) {
    return Status::Error("Empty batch");
  }

  // Append all commands to the log atomically under the same term
  std::vector<Index> indices;
  indices.reserve(commands.size());

  for (const auto& command : commands) {
    auto [index, status] = group_->log_.Append(group_->current_term_, command);
    if (!status.ok()) {
      // Rollback: truncate all entries appended so far in this batch
      if (!indices.empty()) {
        group_->log_.TruncateSuffix(indices.front());
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
      auto entry_opt = group_->log_.GetEntry(index);
      if (entry_opt) {
        log_persister_->Append(*entry_opt, [weak_self = weak_from_this(), index](Status s) {
          auto self = weak_self.lock();
          if (!self) {
            return;
          }
          self->OnLogEntryPersisted(index, s);
        });
      }
    }

    PendingProposal proposal;
    proposal.index = index;
    proposal.callback = [i, results, remaining, callback,
                         timer = timer_](const ApplyResult& result) {
      (*results)[i] = result;
      if (remaining->fetch_sub(1, std::memory_order_acq_rel) == 1) {
        // Last one: post batch completion to timer thread (outside lock)
        timer->SetTimeout(std::chrono::milliseconds(0),
                          [results, callback]() { callback(*results); });
      }
    };
    proposal.propose_time = std::chrono::steady_clock::now();
    group_->pending_proposals_[index] = std::move(proposal);
  }

  // Broadcast immediately — replication is decoupled from persistence (T2)
  if (!log_persister_) {
    group_->flushed_index_ = std::max(group_->flushed_index_, indices.back());
  }
  BroadcastAppendEntriesLocked();
  // For single-node clusters, no followers will respond; try commit now
  TryCommitLocked();

  return Status::OK();
}

void RaftNode::RaftNodeImpl::OnLogEntryPersisted(Index index, Status status) {
  if (!status.ok()) {
    LOG_WARN("Node {} log persistence failed for index {}: {}", group_->server_id_, index,
             status.ToString());
    if (log_persister_ && !log_persister_->IsHealthy()) {
      // Disk failure: step down. Runs on persister thread with no locks held,
      // so acquiring group_->election_mtx_ is safe.
      std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
      if (group_->role_ == RaftNodeRole::LEADER) {
        LOG_ERROR("Node {} stepping down due to disk failure", group_->server_id_);
        BecomeFollowerLocked(group_->current_term_);
      }
    }
    return;
  }
  // Commit: runs on persister thread with no locks held.
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
  if (!IsRunning()) {
    return;
  }
  if (group_->role_ != RaftNodeRole::LEADER) {
    return;
  }
  if (index > group_->flushed_index_) {
    group_->flushed_index_ = index;
  }
  // Retry commit now that this entry is durable
  TryCommitLocked();
}

ApplyResult RaftNode::RaftNodeImpl::ProposeAndWaitLocked(const std::string& command,
                                                         std::unique_lock<std::mutex>& lock_r) {
  // PRECONDITION: caller holds group_->replication_mtx_ (via lock_r).
  // group_->election_mtx_ may or may not be held; this method only accesses
  // replication state (group_->log_, group_->pending_proposals_, group_->flushed_index_).
  // This method unlocks group_->replication_mtx_ while waiting for commit.

  // Use promise/future for synchronous wait
  std::promise<ApplyResult> promise;
  auto future = promise.get_future();

  // Create callback that will set the promise value
  auto callback = [&promise](const ApplyResult& result) { promise.set_value(result); };

  // Append to local log
  auto [index, status] = group_->log_.Append(group_->current_term_, command);
  if (!status.ok()) {
    ApplyResult error_result;
    error_result.success = false;
    error_result.error_message = status.GetMessage();
    return error_result;
  }

  // Persist log entry synchronously before replication
  if (log_persister_) {
    auto entry_opt = group_->log_.GetEntry(index);
    if (entry_opt) {
      auto flush_status = log_persister_->AppendSync(*entry_opt);
      if (!flush_status.ok()) {
        // Disk failure: let async persister callback handle step-down
        // (Phase 5 migrated persister callbacks to fine-grained locks).
        LOG_ERROR("Node {} disk unhealthy, rejecting command", group_->server_id_);
        ApplyResult error_result;
        error_result.success = false;
        error_result.error_message = flush_status.GetMessage();
        return error_result;
      }
      group_->flushed_index_ = std::max(group_->flushed_index_, index);
    }
  } else {
    group_->flushed_index_ = std::max(group_->flushed_index_, index);
  }

  // Record pending proposal
  PendingProposal proposal;
  proposal.index = index;
  proposal.callback = std::move(callback);
  proposal.propose_time = std::chrono::steady_clock::now();
  group_->pending_proposals_[index] = std::move(proposal);

  // Trigger log replication
  BroadcastAppendEntriesLocked();

  // Unlock group_->replication_mtx_ while waiting to allow commit progress.
  // NOTE: group_->election_mtx_ remains held, which blocks election timeouts.
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
  auto cfg = infra_->runtime_config_->Get();
  auto wait_status = future.wait_for(std::chrono::milliseconds(cfg.propose_timeout_ms));

  lock_r.lock();
  guard.need_relock = false;

  if (wait_status == std::future_status::timeout) {
    if (metrics_) {
      auto it = group_->pending_proposals_.find(index);
      if (it != group_->pending_proposals_.end()) {
        auto latency = std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                                     it->second.propose_time)
                           .count();
        metrics_
            ->GetHistogram("raft_proposal_latency_seconds",
                           std::vector<double>{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
                                               2.5, 5.0, 10.0},
                           group_->metrics_node_label_)
            .Observe(latency);
      }
      auto labels = group_->metrics_node_label_;
      labels["result"] = "timeout";
      metrics_->GetCounter("raft_propose_total", labels).Increment();
    }
    // Remove pending proposal on timeout
    group_->pending_proposals_.erase(index);
    ApplyResult timeout_result;
    timeout_result.success = false;
    timeout_result.error_message = "Command execution timeout";
    return timeout_result;
  }

  return future.get();
}

Status RaftNode::RaftNodeImpl::ReadIndex(std::function<void()> callback) {
  // Phase 1: Election state check under group_->election_mtx_ only.
  std::unique_lock<std::mutex> lock_e(group_->election_mtx_);

  RecordActivityLocked();

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (group_->role_ != RaftNodeRole::LEADER) {
    // Follower: forward to leader via ReadIndexRequest RPC
    if (group_->leader_id_ < 0 || group_->leader_addr_.empty()) {
      return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
    }

    // Capture leader state before releasing lock. RpcCall is synchronous
    // and may block for rpc_timeout_ms; holding group_->election_mtx_ would stall
    // heartbeat handling and election timers.
    NodeId leader_id = group_->leader_id_;
    std::string leader_addr = group_->leader_addr_;
    lock_e.unlock();

    ReadIndexRequest req;
    ReadIndexResponse resp;
    auto status = RpcCall(leader_addr, req, resp,
                          std::chrono::milliseconds(infra_->runtime_config_->Get().rpc_timeout_ms));
    if (!status.ok()) {
      return Status::Error("READINDEX_FORWARD",
                           "Failed to forward ReadIndex to leader: " + status.ToString());
    }

    if (!resp.leader_valid_) {
      return Status::NotLeader(leader_id, leader_addr);
    }

    // Re-acquire lock to check term and enqueue read
    std::lock_guard<std::mutex> lock_e2(group_->election_mtx_);
    if (resp.term_ > group_->current_term_) {
      BecomeFollowerLocked(resp.term_);
      return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
    }

    // Enqueue pending read with leader's commit index
    {
      std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
      std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
      std::lock_guard<std::mutex> lock_a(group_->applier_mtx_);

      uint64_t read_id = group_->next_read_id_++;
      PendingReadIndex read_req;
      read_req.read_index = resp.read_index_;
      read_req.callback = std::move(callback);
      read_req.start_time = std::chrono::steady_clock::now();
      read_req.heartbeats_sent = false;  // No heartbeats needed for follower reads
      group_->pending_reads_[read_id] = std::move(read_req);

      if (metrics_) {
        infra_->metrics_->GetCounter("raft_readindex_total", group_->metrics_node_label_)
            .Increment();
      }

      // Try to complete immediately if log already applied
      ProcessPendingReadsLocked();
    }

    return Status::OK();
  }

  // Phase 2: ReadIndex work under full hierarchy (leader path).
  {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    std::shared_lock<std::shared_mutex> lock_m(group_->membership_mtx_);
    std::lock_guard<std::mutex> lock_a(group_->applier_mtx_);

    // Create pending read request
    uint64_t read_id = group_->next_read_id_++;
    PendingReadIndex read_req;
    read_req.read_index = group_->commit_index_;
    read_req.callback = std::move(callback);
    read_req.start_time = std::chrono::steady_clock::now();

    // Check if leader lease is valid (O(1) timestamp check)
    auto cfg = infra_->runtime_config_->Get();
    auto now = std::chrono::steady_clock::now();
    bool lease_valid = cfg.leader_lease_enabled && now < group_->leader_lease_expiry_;

    if (lease_valid) {
      // Lease read: skip heartbeat broadcast, acks already verified via quorum
      read_req.heartbeats_sent = false;
      LOG_INFO("Node {} ReadIndex request {} at commit_index {} (lease read)", group_->server_id_,
               read_id, group_->commit_index_);
      if (metrics_) {
        metrics_->GetCounter("raft_readindex_lease_total", group_->metrics_node_label_).Increment();
      }
    } else {
      // Fallback to normal ReadIndex with heartbeat broadcast
      read_req.heartbeats_sent = true;
      read_req.acks.insert(group_->server_id_);  // Leader acknowledges itself
      LOG_INFO("Node {} ReadIndex request {} at commit_index {} (heartbeat)", group_->server_id_,
               read_id, group_->commit_index_);
    }

    group_->pending_reads_[read_id] = std::move(read_req);

    if (metrics_) {
      infra_->metrics_->GetCounter("raft_readindex_total", group_->metrics_node_label_).Increment();
    }

    if (lease_valid) {
      // Try to complete immediately if log already applied
      ProcessPendingReadsLocked();
    } else {
      // Send heartbeats to confirm leadership
      BroadcastReadIndexHeartbeatsLocked(read_id);
    }
  }

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::AddNode(NodeId id, const NodeAddr& addr) {
  // Bridge pattern: group_->election_mtx_ -> group_->replication_mtx_ -> group_->membership_mtx_
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
  std::unique_lock<std::shared_mutex> lock_m(group_->membership_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (group_->role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
  }

  // Check if node already exists
  if (group_->cluster_config_.Contains(id)) {
    return Status::Error("Node already in cluster");
  }

  // Single-node-change safety: reject if another change is in flight.
  if (group_->pending_config_change_) {
    return Status::Error("A membership change is already in progress; wait for it to commit");
  }

  // Joint consensus: build old and new configurations
  std::vector<NodeId> old_nodes = group_->cluster_config_.nodes;
  std::vector<NodeId> new_nodes = old_nodes;
  new_nodes.push_back(id);

  nlohmann::json j_old = old_nodes;
  nlohmann::json j_new = new_nodes;
  std::string cmd = "CONFIG_CHANGE:JOINT:" + j_old.dump() + ":" + j_new.dump();

  // Propose as normal log entry
  auto [index, status] = group_->log_.Append(group_->current_term_, cmd);
  if (!status.ok()) {
    return status;
  }

  // Persist log entry synchronously for configuration changes
  if (log_persister_) {
    auto entry_opt = group_->log_.GetEntry(index);
    if (entry_opt) {
      auto flush_status = log_persister_->AppendSync(*entry_opt);
      if (!flush_status.ok()) {
        LOG_ERROR("Node {} failed to persist AddNode log entry: {}", group_->server_id_,
                  flush_status.GetMessage());
        return flush_status;
      }
      group_->flushed_index_ = std::max(group_->flushed_index_, index);
    }
  } else {
    // No persistence configured (test path) — treat as immediately flushed
    group_->flushed_index_ = std::max(group_->flushed_index_, index);
  }

  // Mark pending so no other membership change can be proposed.
  group_->pending_config_change_ = true;

  // Add to peer map immediately (optimistic) so the leader starts
  // replicating to the new node right away.
  group_->peer_map_[id] = addr;
  group_->next_index_[id] = group_->log_.GetLastLogInfo().first + 1;
  group_->match_index_[id] = 0;
  SetPeerReplicationLagMetricLocked(id);

  LOG_INFO("Node {} proposing AddNode for {} at index {}", group_->server_id_, id, index);

  // Trigger replication
  BroadcastAppendEntriesLocked();

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::RemoveNode(NodeId id) {
  // Bridge pattern: group_->election_mtx_ -> group_->replication_mtx_ -> group_->membership_mtx_
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::unique_lock<std::mutex> lock_r(group_->replication_mtx_);
  std::unique_lock<std::shared_mutex> lock_m(group_->membership_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (group_->role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
  }

  // Check if node exists
  if (!group_->cluster_config_.Contains(id)) {
    return Status::Error("Node not in cluster");
  }

  // Single-node-change safety: reject if another change is in flight.
  if (group_->pending_config_change_) {
    return Status::Error("A membership change is already in progress; wait for it to commit");
  }

  // Self-removal is allowed: we propose the change and step down below once
  // it is on its way (see the BecomeFollowerLocked call at the end).
  if (id == group_->server_id_) {
    LOG_WARN("Node {} removing itself from cluster - will step down", id);
  }

  // Joint consensus: build old and new configurations
  std::vector<NodeId> old_nodes = group_->cluster_config_.nodes;
  std::vector<NodeId> new_nodes;
  for (NodeId nid : old_nodes) {
    if (nid != id) {
      new_nodes.push_back(nid);
    }
  }

  nlohmann::json j_old = old_nodes;
  nlohmann::json j_new = new_nodes;
  std::string cmd = "CONFIG_CHANGE:JOINT:" + j_old.dump() + ":" + j_new.dump();

  // Propose as normal log entry
  auto [index, status] = group_->log_.Append(group_->current_term_, cmd);
  if (!status.ok()) {
    return status;
  }

  // Persist log entry synchronously for configuration changes
  if (log_persister_) {
    auto entry_opt = group_->log_.GetEntry(index);
    if (entry_opt) {
      auto flush_status = log_persister_->AppendSync(*entry_opt);
      if (!flush_status.ok()) {
        LOG_ERROR("Node {} failed to persist RemoveNode log entry: {}", group_->server_id_,
                  flush_status.GetMessage());
        return flush_status;
      }
      group_->flushed_index_ = std::max(group_->flushed_index_, index);
    }
  } else {
    // No persistence configured (test path) — treat as immediately flushed
    group_->flushed_index_ = std::max(group_->flushed_index_, index);
  }

  // Mark pending so no other membership change can be proposed.
  group_->pending_config_change_ = true;

  // Remove from peer map immediately (optimistic)
  group_->peer_map_.erase(id);
  group_->next_index_.erase(id);
  group_->match_index_.erase(id);
  if (metrics_) {
    auto labels = group_->metrics_node_label_;
    labels["peer_id"] = std::to_string(id);
    infra_->metrics_->RemoveGauge("raft_transport_peer_lag_entries", labels);
  }

  // Remove from group_->peer_addrs_
  group_->peer_addrs_.erase(
      std::remove_if(group_->peer_addrs_.begin(), group_->peer_addrs_.end(),
                     [id](const NodeAddr& a) { return RaftGroup::ParseNodeId(a) == id; }),
      group_->peer_addrs_.end());

  LOG_INFO("Node {} proposing RemoveNode for {} at index {}", group_->server_id_, id, index);

  // Trigger replication
  BroadcastAppendEntriesLocked();

  // If removing ourselves, step down.
  // Must drop downstream locks before calling BecomeFollowerLocked,
  // which acquires group_->replication_mtx_ + group_->snapshot_mtx_ internally.
  if (id == group_->server_id_) {
    lock_m.unlock();
    lock_r.unlock();
    BecomeFollowerLocked(group_->current_term_);
  }

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::AddLearner(NodeId id, const NodeAddr& addr) {
  // Bridge pattern: group_->election_mtx_ -> group_->replication_mtx_ -> group_->membership_mtx_
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
  std::unique_lock<std::shared_mutex> lock_m(group_->membership_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (group_->role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
  }

  // Check if node already exists
  if (group_->cluster_config_.Contains(id)) {
    return Status::Error("Node already in cluster");
  }

  // Single-node-change safety: reject if another change is in flight.
  if (group_->pending_config_change_) {
    return Status::Error("A membership change is already in progress; wait for it to commit");
  }

  // Learners do not affect quorum, so no joint consensus needed.
  std::string cmd = "CONFIG_CHANGE:ADD_LEARNER:" + std::to_string(id) + ":" + addr;

  // Propose as normal log entry
  auto [index, status] = group_->log_.Append(group_->current_term_, cmd);
  if (!status.ok()) {
    return status;
  }

  // Persist log entry synchronously for configuration changes
  if (log_persister_) {
    auto entry_opt = group_->log_.GetEntry(index);
    if (entry_opt) {
      auto flush_status = log_persister_->AppendSync(*entry_opt);
      if (!flush_status.ok()) {
        LOG_ERROR("Node {} failed to persist AddLearner log entry: {}", group_->server_id_,
                  flush_status.GetMessage());
        return flush_status;
      }
      group_->flushed_index_ = std::max(group_->flushed_index_, index);
    }
  } else {
    // No persistence configured (test path) — treat as immediately flushed
    group_->flushed_index_ = std::max(group_->flushed_index_, index);
  }

  // Mark pending so no other membership change can be proposed.
  group_->pending_config_change_ = true;

  // Add to peer map immediately (optimistic) so the leader starts
  // replicating to the new node right away.
  group_->peer_map_[id] = addr;
  group_->next_index_[id] = group_->log_.GetLastLogInfo().first + 1;
  group_->match_index_[id] = 0;
  SetPeerReplicationLagMetricLocked(id);

  LOG_INFO("Node {} proposing AddLearner for {} at index {}", group_->server_id_, id, index);

  // Trigger replication
  BroadcastAppendEntriesLocked();

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::PromoteLearner(NodeId id) {
  // Bridge pattern: group_->election_mtx_ -> group_->replication_mtx_ -> group_->membership_mtx_
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);
  std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
  std::unique_lock<std::shared_mutex> lock_m(group_->membership_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (group_->role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
  }

  // Check if node is actually a learner
  if (!group_->cluster_config_.IsLearner(id)) {
    return Status::Error("Node is not a learner");
  }

  // Single-node-change safety: reject if another change is in flight.
  if (group_->pending_config_change_) {
    return Status::Error("A membership change is already in progress; wait for it to commit");
  }

  // Promotion changes quorum, so use joint consensus.
  std::vector<NodeId> old_nodes = group_->cluster_config_.nodes;
  std::vector<NodeId> new_nodes = old_nodes;
  new_nodes.push_back(id);

  nlohmann::json j_old = old_nodes;
  nlohmann::json j_new = new_nodes;
  std::string cmd = "CONFIG_CHANGE:JOINT:" + j_old.dump() + ":" + j_new.dump();

  // Propose as normal log entry
  auto [index, status] = group_->log_.Append(group_->current_term_, cmd);
  if (!status.ok()) {
    return status;
  }

  // Persist log entry synchronously for configuration changes
  if (log_persister_) {
    auto entry_opt = group_->log_.GetEntry(index);
    if (entry_opt) {
      auto flush_status = log_persister_->AppendSync(*entry_opt);
      if (!flush_status.ok()) {
        LOG_ERROR("Node {} failed to persist PromoteLearner log entry: {}", group_->server_id_,
                  flush_status.GetMessage());
        return flush_status;
      }
      group_->flushed_index_ = std::max(group_->flushed_index_, index);
    }
  } else {
    // No persistence configured (test path) — treat as immediately flushed
    group_->flushed_index_ = std::max(group_->flushed_index_, index);
  }

  // Mark pending so no other membership change can be proposed.
  group_->pending_config_change_ = true;

  LOG_INFO("Node {} proposing PromoteLearner for {} at index {}", group_->server_id_, id, index);

  // Trigger replication
  BroadcastAppendEntriesLocked();

  return Status::OK();
}

Status RaftNode::RaftNodeImpl::TransferLeadershipTo(NodeId target_id) {
  std::lock_guard<std::mutex> lock_e(group_->election_mtx_);

  if (!IsRunning()) {
    return Status::Error("Node not running");
  }

  if (group_->role_ != RaftNodeRole::LEADER) {
    return Status::NotLeader(group_->leader_id_, group_->leader_addr_);
  }

  if (target_id == group_->server_id_) {
    return Status::Error("Cannot transfer leadership to self");
  }

  if (group_->peer_map_.find(target_id) == group_->peer_map_.end()) {
    return Status::Error("Target node not in cluster");
  }

  // Send one final AppendEntries to target to ensure its log is up-to-date
  {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    SendAppendEntriesToPeerLocked(target_id);
  }

  LOG_INFO("Node {} transferring leadership to {}, stepping down", group_->server_id_, target_id);

  // Step down. BecomeFollowerLocked acquires group_->replication_mtx_ + group_->snapshot_mtx_
  // internally, so we must not hold them here.
  BecomeFollowerLocked(group_->current_term_);

  return Status::OK();
}

ClusterConfig RaftNode::RaftNodeImpl::GetConfig() const {
  std::shared_lock<std::shared_mutex> lock(group_->membership_mtx_);
  return group_->cluster_config_;
}

uint64_t RaftNode::RaftNodeImpl::GetLogTermLocked(uint64_t index) {
  if (index == 0) {
    return 0;
  }
  // The entry at the snapshot boundary is compacted away from the in-memory
  // log, but its term is known from snapshot metadata.
  if (index == group_->last_snapshot_index_) {
    return group_->last_snapshot_term_;
  }
  return group_->log_.GetLogTerm(index);
}

void RaftNode::RaftNodeImpl::UpdateLeaderLeaseMetricLocked() {
  if (!metrics_) {
    return;
  }
  auto cfg = infra_->runtime_config_->Get();
  bool valid = false;
  double remaining_seconds = 0.0;
  if (group_->role_ == RaftNodeRole::LEADER && cfg.leader_lease_enabled) {
    auto now = std::chrono::steady_clock::now();
    valid = now < group_->leader_lease_expiry_;
    auto remaining_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(group_->leader_lease_expiry_ - now)
            .count();
    remaining_seconds = std::max(0.0, remaining_ms / 1000.0);
  }
  infra_->metrics_->GetGauge("raft_leader_lease_seconds", group_->metrics_node_label_)
      .Set(remaining_seconds);
  infra_->metrics_->GetGauge("raft_leader_lease_valid", group_->metrics_node_label_)
      .Set(valid ? 1.0 : 0.0);
}

void RaftNode::RaftNodeImpl::SetPeerReplicationLagMetricLocked(NodeId peer_id) {
  if (!metrics_) {
    return;
  }
  auto [last_index, _] = group_->log_.GetLastLogInfo();
  Index match = 0;
  auto it = group_->match_index_.find(peer_id);
  if (it != group_->match_index_.end()) {
    match = it->second;
  }
  double lag = (last_index >= match) ? static_cast<double>(last_index - match) : 0.0;
  auto labels = group_->metrics_node_label_;
  labels["peer_id"] = std::to_string(peer_id);
  infra_->metrics_->GetGauge("raft_transport_peer_lag_entries", labels).Set(lag);
}
