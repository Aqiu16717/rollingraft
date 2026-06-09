# RollingRaft Operations Guide

**Version:** v0.1.0  
**Target Audience:** SREs, platform engineers, and operators running RollingRaft clusters  
**Prerequisites:** Familiarity with Raft consensus and Linux system administration

---

## Table of Contents

1. [Deployment Checklist](#1-deployment-checklist)
2. [Monitoring & Alerting](#2-monitoring--alerting)
3. [Troubleshooting](#3-troubleshooting)
4. [Maintenance Procedures](#4-maintenance-procedures)
5. [Security Hardening](#5-security-hardening)

---

## 1. Deployment Checklist

### 1.1 Pre-Deployment

- [ ] **Cluster sizing**: Use 3 or 5 nodes (odd number for quorum). 3-node for regional, 5-node for cross-AZ/DC.
- [ ] **Network**: All nodes must be mutually reachable on Raft port (TCP) and metrics port (TCP).
- [ ] **Clock sync**: Enable NTP/chrony on all nodes. Raft does not require tight clock sync, but logging and metrics do.
- [ ] **Storage**: Use local SSD for `data_dir`. Do not use NFS or shared storage for Raft state.
- [ ] **File descriptors**: Set `ulimit -n >= 65536` (each peer connection + clients + metrics requests consume FDs).
- [ ] **Firewall**: Open Raft port for node-to-node, metrics port for observability stack.

### 1.2 Configuration

- [ ] `node_id` is unique and stable across restarts.
- [ ] `peers` list is identical on all nodes (including all voting members).
- [ ] `data_dir` is on a dedicated disk with >= 20% free space at all times.
- [ ] `election_timeout_ms` > 2 × `heartbeat_interval_ms` (e.g., 300ms / 50ms for LAN).
- [ ] `shutdown_timeout_ms` is set (default 30s is reasonable).
- [ ] `metrics_enabled = true` and `metrics_addr` is set for observability.
- [ ] `admin_token` is set for production clusters (use a cryptographically random token).
- [ ] TLS is enabled for node-to-node traffic in untrusted networks (`tls_enabled = true`).

### 1.3 At Startup

- [ ] Start nodes one at a time and verify each joins the cluster.
- [ ] Check `/healthz` returns `{"status":"alive"}` on all nodes.
- [ ] Check `/readyz` returns `{"status":"ready"}` on at least one node (the leader).
- [ ] Verify `raft_role == 2` on exactly one node.
- [ ] Submit a test `Propose` and confirm it commits on all nodes (`raft_commit_index` advances).

---

## 2. Monitoring & Alerting

### 2.1 Critical Alerts (P1)

| Alert | PromQL | Recommended Threshold |
|-------|--------|----------------------|
| **No Leader Elected** | `max(raft_role) < 2` | > 30s |
| **Cluster Unavailable** | `up{job="rollingraft"} < 1` on majority | > 1m |
| **Follower Lag Critical** | `max(raft_commit_index) - min(raft_commit_index) > 10000` | > 5m |

### 2.2 Warning Alerts (P2)

| Alert | PromQL | Recommended Threshold |
|-------|--------|----------------------|
| **High Proposal Latency P99** | `histogram_quantile(0.99, sum(rate(raft_proposal_latency_seconds_bucket[5m])) by (le)) > 0.5` | > 5m |
| **Frequent Elections** | `rate(raft_leader_elected_total[5m]) > 0.05` | > 5m |
| **AE Failure Rate High** | `rate(raft_appendentries_failure_total[5m]) / rate(raft_appendentries_sent_total[5m]) > 0.1` | > 5m |
| **Dead Node Detected** | `rate(raft_dead_nodes_detected_total[5m]) > 0` | > 0 |

### 2.3 Info Alerts (P3)

| Alert | PromQL | Recommended Threshold |
|-------|--------|----------------------|
| **Snapshot Transfer Slow** | `rate(raft_snapshot_chunks_sent_total[5m]) < 1` while `rate(raft_snapshot_sends_started_total[5m]) > 0` | > 10m |
| **High Memory Growth** | `process_resident_memory_bytes` (external) growth > 20%/hour | > 1h |
| **Log Compaction Frequency** | `rate(raft_log_compactions_total[5m]) > 0.01` | > 1h |

### 2.4 Dashboards

See [public-api-guide.md Section 9.5](public-api-guide.md#95-grafana-dashboard-json-snippet) for a Grafana dashboard JSON snippet.

Recommended panels:
- Raft Role per node
- Current Term
- Commit Index / Applied Index
- Proposal Latency P50/P99
- AppendEntries sent/retries/failures
- Snapshot transfer progress
- Transport peer connection state

---

## 3. Troubleshooting

### 3.1 "Node Stuck in Candidate State"

**Symptoms:**
- `raft_role == 1` for extended period
- `raft_election_timeouts_total` increasing
- `raft_leader_elected_total` not increasing

**Diagnosis:**
```bash
# Check network connectivity between this node and peers
curl -s http://<node>:<metrics_port>/v1/status | jq .

# Verify peer addresses are correct
# Look for transport_peer_state == 0 or 3 (disconnected / failed)
```

**Common Causes & Fixes:**

| Cause | Check | Fix |
|-------|-------|-----|
| Network partition | `ping` / `telnet <peer> <raft_port>` | Fix firewall or routing |
| Wrong peer list | Compare `peers` across all nodes | Restart with corrected config |
| TLS misconfiguration | Check logs for TLS handshake errors | Verify cert/key/CA files and matching `tls_*` settings |
| Election timeout too short | `election_timeout_ms < RTT × 3` | Increase `election_timeout_ms` (WAN: 1000ms+) |
| Port conflict | `ss -tlnp` shows port already in use | Change `listen_addr` or kill conflicting process |

### 3.2 "Leader Election Loop"

**Symptoms:**
- Leader is elected briefly, then steps down
- `raft_checkquorum_stepdown_total` increasing
- `raft_elections_total` and `raft_leader_elected_total` both high

**Diagnosis:**
```bash
# Check CheckQuorum behavior
curl -s http://<leader>:<metrics_port>/metrics | grep raft_checkquorum_stepdown_total

# Check network stability
curl -s http://<leader>:<metrics_port>/metrics | grep raft_appendentries_failure_total
```

**Common Causes & Fixes:**

| Cause | Check | Fix |
|-------|-------|-----|
| Packet loss / high latency | Network monitoring, `mtr` to peers | Fix network or tune timeouts |
| Election timeout too aggressive | `election_timeout_ms` close to RTT | Increase to ≥ 3× RTT |
| CPU starvation | `top`, `mpstat` | Add CPU resources or reduce load |
| CheckQuorum too sensitive | `check_quorum_enabled = true` with unstable network | Disable `check_quorum_enabled` as temporary mitigation (risk: stale leader) |

### 3.3 "Commit Index Not Advancing"

**Symptoms:**
- `raft_commit_index` flat on leader
- Client Proposes timing out
- `raft_propose_total` increases but `raft_commits_total` does not

**Diagnosis:**
```bash
# On leader: check if quorum is connected
curl -s http://<leader>:<metrics_port>/metrics | grep transport_peer_state

# Check match_index vs commit_index
curl -s http://<leader>:<metrics_port>/v1/status | jq .

# Check AE failure rate
curl -s http://<leader>:<metrics_port>/metrics | grep raft_appendentries_failure_total
```

**Common Causes & Fixes:**

| Cause | Check | Fix |
|-------|-------|-----|
| Quorum lost (minority partition) | `transport_peer_state` shows disconnected | Restore network connectivity |
| Pipeline window blocked | `inflight_` full due to slow follower | Wait for follower to catch up or remove dead node |
| Slow follower disk | Follower I/O wait high | Add faster storage or add learner instead of voter |
| Corrupted log on follower | Follower logs show AE rejections | Remove and re-add follower (it will receive snapshot) |

### 3.4 "Snapshot Transfer Timeout"

**Symptoms:**
- New/slow follower stuck with old log index
- `raft_snapshot_sends_started_total` increases but `raft_snapshot_sends_completed_total` does not
- Follower repeatedly triggers snapshot install

**Diagnosis:**
```bash
# Check snapshot size
curl -s http://<leader>:<metrics_port>/metrics | grep raft_snapshot_chunks_sent_total

# Check max_snapshot_size_bytes
curl -s http://<leader>:<metrics_port>/v1/config | jq '.max_snapshot_size_bytes'

# Verify network bandwidth between leader and follower
```

**Common Causes & Fixes:**

| Cause | Check | Fix |
|-------|-------|-----|
| Snapshot exceeds `max_snapshot_size_bytes` | Logs show "snapshot too large" | Increase `max_snapshot_size_bytes` or compact more frequently |
| Network bandwidth insufficient | Large snapshot × slow link | Increase `rpc_timeout_ms` or use dedicated transfer network |
| Follower disk full | `df -h` on follower | Free disk space |
| Snapshot chunk corruption | Checksum mismatch in logs | Re-trigger snapshot or restart follower |

### 3.5 "High Memory Usage"

**Symptoms:**
- RSS growing steadily
- OOM kills
- `process_resident_memory_bytes` (external metric) climbing

**Diagnosis:**
```bash
# Check log size
du -sh <data_dir>/wal <data_dir>/state

# Check session manager state
curl -s http://<node>:<metrics_port>/metrics | grep -i session

# Check snapshot threshold
curl -s http://<node>:<metrics_port>/v1/config | jq '.snapshot_threshold_entries, .snapshot_threshold_bytes, .log_retention_entries'
```

**Common Causes & Fixes:**

| Cause | Check | Fix |
|-------|-------|-----|
| Log not compacted | `raft_commit_index - last_snapshot_index` large | Lower `snapshot_threshold_entries` or `snapshot_threshold_bytes` |
| Log retention too high | `log_retention_entries` set high | Reduce `log_retention_entries` (0 = delete all covered by snapshot) |
| Client sessions not expiring | `ttl_ms` too high or too many sessions | Reduce `ClientSessionManager` TTL or `max_sessions` |
| Large in-memory state machine | State machine heap profiling | Optimize state machine or shard data |
| WAL segments accumulating | Many uncleaned `.wal` files | Run `WALPersister::GarbageCollect()` via snapshot trigger |

---

## 4. Maintenance Procedures

### 4.1 Adding a Node

1. Prepare new node with same binary version and config template.
2. Start new node with empty `data_dir`.
3. On leader, call:
   ```bash
   curl -X POST http://<leader>:<metrics_port>/v1/members \
     -H "Authorization: Bearer <admin_token>" \
     -d '{"node_id": <new_id>, "addr": "<new_addr>"}'
   ```
4. Wait for new node to catch up (`raft_commit_index` matches cluster).
5. If added as learner, promote once caught up:
   ```bash
   curl -X POST http://<leader>:<metrics_port>/v1/members \
     -H "Authorization: Bearer <admin_token>" \
     -d '{"node_id": <new_id>, "promote": true}'
   ```

### 4.2 Removing a Node

1. Identify the node to remove (must not be the leader; transfer leadership first if needed).
2. On leader, call:
   ```bash
   curl -X DELETE http://<leader>:<metrics_port>/v1/members/<node_id> \
     -H "Authorization: Bearer <admin_token>"
   ```
3. Stop the removed node.
4. Update `peers` config on remaining nodes (for next restart).

### 4.3 Transferring Leadership

```bash
curl -X POST http://<current_leader>:<metrics_port>/v1/leadership/transfer \
  -H "Authorization: Bearer <admin_token>" \
  -d '{"target_node_id": <target_id>}'
```

Use before planned maintenance on the current leader.

### 4.4 Triggering Manual Snapshot

```bash
curl -X POST http://<leader>:<metrics_port>/v1/snapshot/trigger \
  -H "Authorization: Bearer <admin_token>"
```

Useful before:
- Removing a node (to reduce recovery time)
- Performing log forensic analysis
- Reclaiming disk space immediately

### 4.5 Updating Runtime Config

```bash
curl -X PATCH http://<leader>:<metrics_port>/v1/config \
  -H "Authorization: Bearer <admin_token>" \
  -H "Content-Type: application/json" \
  -d '{"heartbeat_interval_ms": 100, "transport_batching_enabled": false}'
```

Only certain parameters can be updated at runtime. See [public-api-guide.md Section 7](public-api-guide.md#7-runtime-configuration) for the list.

---

## 5. Security Hardening

### 5.1 Network

- Enable TLS for node-to-node traffic (`tls_enabled = true`).
- Use mTLS if nodes are in different security zones (`tls_ca_file` set).
- Restrict metrics port access to monitoring infrastructure (not public internet).
- Do not expose Raft port outside the cluster network.

### 5.2 Authentication

- Set a strong `admin_token` (≥ 32 random characters).
- Store `admin_token` in a secrets manager, not in config files.
- Rotate `admin_token` periodically by restarting nodes with new token.

### 5.3 Filesystem

- `data_dir` should be owned by the service user only (`chmod 700`).
- Disable swap or configure swap encryption to prevent state leakage.
- Use filesystem encryption (e.g., LUKS) for `data_dir` if required by compliance.

### 5.4 Backup

- Backup `data_dir` regularly. A consistent backup requires the node to be stopped.
- For hot backup, use snapshot + WAL copying, but be aware this is not officially supported.
- Test restore procedures in a staging environment quarterly.

---

## Quick Reference Card

```bash
# Health checks
curl http://<node>:9001/healthz
curl http://<node>:9001/readyz
curl http://<node>:9001/v1/status

# Metrics
curl http://<node>:9001/metrics

# Admin operations (requires token)
curl -H "Authorization: Bearer <token>" -X POST http://<leader>:9001/v1/members -d '{...}'
curl -H "Authorization: Bearer <token>" -X DELETE http://<leader>:9001/v1/members/<id>
curl -H "Authorization: Bearer <token>" -X POST http://<leader>:9001/v1/snapshot/trigger
curl -H "Authorization: Bearer <token>" -X POST http://<leader>:9001/v1/leadership/transfer -d '{"target_node_id":<id>}'
curl -H "Authorization: Bearer <token>" -X PATCH http://<leader>:9001/v1/config -d '{...}'
```
