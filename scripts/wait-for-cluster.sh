#!/bin/bash
# Wait for Raft cluster to elect a leader before running client tests

set -e

NODES=("$@")
TIMEOUT=${RAFT_CLUSTER_TIMEOUT:-30}
INTERVAL=${RAFT_CLUSTER_INTERVAL:-2}

if [ ${#NODES[@]} -eq 0 ]; then
  echo "Usage: $0 <node1> <node2> ..."
  echo "Example: $0 raft-node-1:8001 raft-node-2:8002 raft-node-3:8003"
  exit 1
fi

echo "Waiting for Raft cluster to be ready (timeout: ${TIMEOUT}s)..."

elapsed=0
while [ $elapsed -lt $TIMEOUT ]; do
  ready_count=0
  for node in "${NODES[@]}"; do
    # Try to connect to the node; if it accepts TCP, count it as up
    host="${node%:*}"
    port="${node#*:}"
    if nc -z -w 2 "$host" "$port" 2>/dev/null; then
      ready_count=$((ready_count + 1))
    fi
  done

  if [ $ready_count -eq ${#NODES[@]} ]; then
    echo "All ${#NODES[@]} nodes are reachable. Waiting 5s for leader election..."
    sleep 5
    echo "Cluster should be ready!"
    exit 0
  fi

  echo "  ($ready_count/${#NODES[@]} nodes ready)..."
  sleep $INTERVAL
  elapsed=$((elapsed + INTERVAL))
done

echo "ERROR: Cluster did not become ready within ${TIMEOUT}s"
exit 1
