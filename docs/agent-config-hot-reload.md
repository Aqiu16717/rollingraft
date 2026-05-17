# RollingRaft Configuration Hot Reload Design

> Version: 0.1.0-draft
> Status: Design Phase
> Target: Task #23 Configuration Hot Reload

## Overview

Allow runtime modification of Raft tuning parameters without restarting nodes. This enables agents to:

1. **Auto-tune** parameters based on workload (latency vs throughput)
2. **Adapt to network conditions** (increase timeout during high latency)
3. **Perform maintenance** (reduce heartbeat to minimize load during backup)
4. **A/B test** configurations across the cluster

## Configurable Parameters

| Parameter | Default | Min | Max | Dynamic? | Impact |
|-----------|---------|-----|-----|----------|--------|
| `election_timeout_ms` | 300 | 50 | 5000 | ✅ | Availability vs split votes |
| `heartbeat_interval_ms` | 50 | 10 | 1000 | ✅ | Replication latency vs CPU |
| `max_entries_per_append` | 100 | 1 | 10000 | ✅ | Batch size vs latency |
| `rpc_timeout_ms` | 500 | 100 | 10000 | ✅ | Fail detection speed |
| `snapshot_threshold_entries` | 10000 | 100 | 1000000 | ✅ | Log growth vs snapshot freq |
| `snapshot_threshold_bytes` | 10MB | 1MB | 1GB | ✅ | Storage vs recovery time |
| `snapshot_check_interval_ms` | 5000 | 1000 | 60000 | ✅ | Snapshot trigger frequency |
| `max_retry_attempts` | 5 | 1 | 100 | ✅ | Reliability vs load |
| `base_retry_delay_ms` | 10 | 1 | 1000 | ✅ | Retry aggressiveness |
| `max_retry_delay_ms` | 500 | 10 | 10000 | ✅ | Retry backoff cap |
| `log_retention_entries` | 0 | 0 | 100000 | ✅ | Storage vs history |

**Not dynamic (requires restart):**
- `node_id`, `listen_addr`, `peers`, `data_dir` — identity and topology
- `network_factory`, `timer_factory`, etc. — implementation wiring

## API Design

### GET /v1/config

Get current runtime configuration:

```json
{
  "election_timeout_ms": 300,
  "heartbeat_interval_ms": 50,
  "max_entries_per_append": 100,
  "rpc_timeout_ms": 500,
  "snapshot_threshold_entries": 10000,
  "snapshot_threshold_bytes": 10485760,
  "snapshot_check_interval_ms": 5000,
  "max_retry_attempts": 5,
  "base_retry_delay_ms": 10,
  "max_retry_delay_ms": 500,
  "log_retention_entries": 0
}
```

### PATCH /v1/config

Update specific parameters atomically:

**Request:**
```json
{
  "election_timeout_ms": 500,
  "heartbeat_interval_ms": 100
}
```

**Response 200:**
```json
{
  "status": "updated",
  "applied": {
    "election_timeout_ms": 500,
    "heartbeat_interval_ms": 100
  },
  "effective_at_ms": 1715932800000
}
```

**Response 400 (invalid value):**
```json
{
  "error": "INVALID_CONFIG",
  "message": "election_timeout_ms must be between 50 and 5000",
  "parameter": "election_timeout_ms",
  "requested_value": 10000,
  "allowed_range": [50, 5000]
}
```

**Response 403 (not leader):**
```json
{
  "error": "NOT_LEADER",
  "message": "Only the leader can modify cluster-wide timing parameters",
  "leader_hint": "127.0.0.1:8001"
}
```

### Validation Rules

1. **Range validation**: Every parameter has min/max bounds
2. **Cross-parameter validation**:
   - `heartbeat_interval_ms < election_timeout_ms` (must have room for at least 1 heartbeat)
   - `base_retry_delay_ms <= max_retry_delay_ms`
   - `rpc_timeout_ms >= heartbeat_interval_ms` (RPC timeout should cover at least one heartbeat)
3. **Leader-only restriction**: Timing parameters (`election_timeout_ms`, `heartbeat_interval_ms`) affect consensus behavior and should only be changed by the leader (to ensure cluster-wide consistency)
4. **Graceful transition**: Existing timers use old values; new timers use new values

## Implementation Design

### Config Store

Replace direct `RaftNodeConfig` field access with an atomic config snapshot:

```cpp
class RuntimeConfig {
 public:
  struct Values {
    uint32_t election_timeout_ms;
    uint32_t heartbeat_interval_ms;
    uint32_t max_entries_per_append;
    uint32_t rpc_timeout_ms;
    uint32_t snapshot_threshold_entries;
    uint32_t snapshot_threshold_bytes;
    uint32_t snapshot_check_interval_ms;
    uint32_t max_retry_attempts;
    uint32_t base_retry_delay_ms;
    uint32_t max_retry_delay_ms;
    uint32_t log_retention_entries;
  };

  // Get current config (thread-safe, atomic read)
  Values Get() const;

  // Update config (thread-safe, validates before applying)
  Status Update(const Values& partial_update);

  // Reset to defaults
  void Reset();

 private:
  mutable std::shared_mutex mtx_;
  Values values_;
  Values defaults_;
};
```

### Timer Transition Strategy

**Problem**: Changing `election_timeout_ms` while an election timer is running.

**Solution**: Lazy transition

```cpp
void ResetElectionTimerLocked() {
  auto config = runtime_config_.Get();
  
  // Cancel existing timer
  if (election_timer_ != 0) {
    timer_->CancelTimer(election_timer_);
  }
  
  // Compute randomized timeout using NEW config
  uint32_t timeout = config.election_timeout_ms +
                     random_offset_(0, config.election_timeout_ms);
  
  election_timer_ = timer_->StartTimer(timeout, [this]() {
    OnElectionTimeout();
  });
}
```

When config changes:
1. **Heartbeat timers**: Cancel and restart with new interval on next scheduled beat
2. **Election timers**: Let current timer expire; new timer uses new value
3. **Snapshot check timers**: Restart on next check cycle
4. **Retry delays**: New RPCs use new delays; in-flight RPCs keep old delays

### Config Propagation (Optional)

For cluster-wide consistency, config changes can be propagated via Raft log:

```cpp
// Internal config change command
struct ConfigChangeCommand {
  std::string parameter;
  uint64_t value;
  uint64_t timestamp_ms;
};
```

**Pros**: All nodes have identical config
**Cons**: Adds log entries for non-state-machine changes

**Recommendation**: v1 allows per-node config (simpler); v2 adds cluster-wide propagation.

## Agent Use Cases

### Auto-Tune Based on Latency

```python
# Agent monitors p99 latency via /metrics
latency_p99 = metrics.query('raft_propose_latency_p99')

if latency_p99 > 100:  # ms
    # Reduce batch size for lower latency
    client.patch('/v1/config', {
        'max_entries_per_append': 50
    })
elif latency_p99 < 10:
    # Increase batch size for throughput
    client.patch('/v1/config', {
        'max_entries_per_append': 200
    })
```

### Adapt to Network Partitions

```python
# Agent detects packet loss via /metrics
packet_loss = metrics.query('network_packet_loss_rate')

if packet_loss > 0.05:  # 5% loss
    # Be more lenient with timeouts
    client.patch('/v1/config', {
        'rpc_timeout_ms': 1000,
        'max_retry_attempts': 10
    })
```

### Maintenance Mode

```python
# Agent prepares for maintenance window
client.patch('/v1/config', {
    'heartbeat_interval_ms': 200,      # Reduce network load
    'snapshot_check_interval_ms': 60000  # Less frequent snapshots
})
```

## Testing

1. **Unit tests**: Validate range checks, cross-parameter validation
2. **Integration tests**: Change config during leader election, verify new values take effect
3. **Chaos tests**: Rapid config changes while under load (via deterministic test mode)
4. **Benchmark**: Measure performance before/after config changes
