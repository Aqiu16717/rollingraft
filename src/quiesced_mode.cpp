#include "raft_node_impl.h"

using namespace rollingraft;

void RaftNode::RaftNodeImpl::RecordActivityLocked() {
  last_activity_time_ = std::chrono::steady_clock::now();
  if (quiesced_.load(std::memory_order_acquire)) {
    ExitQuiescedLocked();
  }
}

bool RaftNode::RaftNodeImpl::ShouldEnterQuiescedLocked() const {
  if (!config_.quiesced_mode_enabled) return false;
  if (quiesced_.load(std::memory_order_acquire)) return false;

  auto now = std::chrono::steady_clock::now();
  auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - last_activity_time_).count();
  return elapsed >= 0 && static_cast<uint32_t>(elapsed) >= config_.quiesced_idle_threshold_ms;
}

void RaftNode::RaftNodeImpl::EnterQuiescedLocked() {
  if (quiesced_.exchange(true, std::memory_order_acq_rel)) return;
  consecutive_quiesced_timeouts_ = 0;

  LOG_INFO("Node {} entered quiesced mode (idle for {}ms)", server_id_,
           config_.quiesced_idle_threshold_ms);

  if (metrics_) {
    metrics_
        ->GetCounter("raft_quiesced_mode_entered_total", {{"node_id", std::to_string(server_id_)}})
        .Increment();
    metrics_->GetGauge("raft_quiesced_mode_active", {{"node_id", std::to_string(server_id_)}})
        .Set(1.0);
  }

  // If leader, reschedule heartbeat timer with quiesced interval
  if (role_ == RaftNodeRole::LEADER) {
    std::lock_guard<std::mutex> lock_r(replication_mtx_);
    StopHeartbeatTimerLocked();
    heartbeat_timer_ =
        timer_->SetInterval(std::chrono::milliseconds(config_.quiesced_heartbeat_interval_ms),
                            [this]() { OnHeartbeatTimeout(); });
  }
}

void RaftNode::RaftNodeImpl::ExitQuiescedLocked() {
  if (!quiesced_.exchange(false, std::memory_order_acq_rel)) return;
  consecutive_quiesced_timeouts_ = 0;

  LOG_INFO("Node {} exited quiesced mode", server_id_);

  if (metrics_) {
    metrics_
        ->GetCounter("raft_quiesced_mode_exited_total", {{"node_id", std::to_string(server_id_)}})
        .Increment();
    metrics_->GetGauge("raft_quiesced_mode_active", {{"node_id", std::to_string(server_id_)}})
        .Set(0.0);
  }

  // If leader, restore normal heartbeat interval
  if (role_ == RaftNodeRole::LEADER) {
    std::lock_guard<std::mutex> lock_r(replication_mtx_);
    StopHeartbeatTimerLocked();
    auto cfg = runtime_config_->Get();
    heartbeat_timer_ = timer_->SetInterval(std::chrono::milliseconds(cfg.heartbeat_interval_ms),
                                           [this]() { OnHeartbeatTimeout(); });
  }
}
