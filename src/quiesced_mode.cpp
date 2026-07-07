#include "raft_node_impl.h"

using namespace rollingraft;

void RaftNode::RaftNodeImpl::RecordActivityLocked() {
  group_->last_activity_time_ = std::chrono::steady_clock::now();
  if (group_->quiesced_.load(std::memory_order_acquire)) {
    ExitQuiescedLocked();
  }
}

bool RaftNode::RaftNodeImpl::ShouldEnterQuiescedLocked() const {
  if (!group_->config_.quiesced_mode_enabled) return false;
  if (group_->quiesced_.load(std::memory_order_acquire)) return false;

  auto now = std::chrono::steady_clock::now();
  auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - group_->last_activity_time_)
          .count();
  return elapsed >= 0 &&
         static_cast<uint32_t>(elapsed) >= group_->config_.quiesced_idle_threshold_ms;
}

void RaftNode::RaftNodeImpl::EnterQuiescedLocked() {
  if (group_->quiesced_.exchange(true, std::memory_order_acq_rel)) return;
  group_->consecutive_quiesced_timeouts_ = 0;

  LOG_INFO("Node {} entered quiesced mode (idle for {}ms)", group_->server_id_,
           group_->config_.quiesced_idle_threshold_ms);

  if (metrics_) {
    metrics_
        ->GetCounter("raft_quiesced_mode_entered_total",
                     group_->metrics_node_label_)
        .Increment();
    infra_->metrics_
        ->GetGauge("raft_quiesced_mode_active", group_->metrics_node_label_)
        .Set(1.0);
  }

  // If leader, reschedule heartbeat timer with quiesced interval
  if (group_->role_ == RaftNodeRole::LEADER) {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    StopHeartbeatTimerLocked();
    group_->heartbeat_timer_ = infra_->timer_->SetInterval(
        std::chrono::milliseconds(group_->config_.quiesced_heartbeat_interval_ms),
        [this]() { OnHeartbeatTimeout(); });
  }
}

void RaftNode::RaftNodeImpl::ExitQuiescedLocked() {
  if (!group_->quiesced_.exchange(false, std::memory_order_acq_rel)) return;
  group_->consecutive_quiesced_timeouts_ = 0;

  LOG_INFO("Node {} exited quiesced mode", group_->server_id_);

  if (metrics_) {
    metrics_
        ->GetCounter("raft_quiesced_mode_exited_total",
                     group_->metrics_node_label_)
        .Increment();
    infra_->metrics_
        ->GetGauge("raft_quiesced_mode_active", group_->metrics_node_label_)
        .Set(0.0);
  }

  // If leader, restore normal heartbeat interval
  if (group_->role_ == RaftNodeRole::LEADER) {
    std::lock_guard<std::mutex> lock_r(group_->replication_mtx_);
    StopHeartbeatTimerLocked();
    auto cfg = infra_->runtime_config_->Get();
    group_->heartbeat_timer_ = infra_->timer_->SetInterval(
        std::chrono::milliseconds(cfg.heartbeat_interval_ms), [this]() { OnHeartbeatTimeout(); });
  }
}
