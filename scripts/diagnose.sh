#!/bin/bash
# Diagnose RollingRaft cluster issues

echo "=========================================="
echo "RollingRaft Cluster Diagnostics"
echo "=========================================="

# Clean up old data
echo "[1/5] Cleaning old data..."
rm -rf data/node*/*

# Check ports
echo ""
echo "[2/5] Checking ports..."
for port in 8001 8002 8003; do
  if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "  Port $port: IN USE ($(lsof -Pi :$port -sTCP:LISTEN | tail -1 | awk '{print $1}'))"
  else
    echo "  Port $port: FREE"
  fi
done

# Start first node in background with logging
echo ""
echo "[3/5] Starting Node 1 (with debug logging)..."
mkdir -p data/node1
./build/example/example_counter_server 1 8001 8002 8003 > /tmp/node1.log 2>&1 &
NODE1_PID=$!
echo "  Node 1 PID: $NODE1_PID"
sleep 2

# Check if process is running
if ps -p $NODE1_PID > /dev/null; then
  echo "  Node 1 is running"
  echo "  Log output:"
  head -20 /tmp/node1.log | sed 's/^/    /'
else
  echo "  ERROR: Node 1 failed to start!"
  cat /tmp/node1.log | sed 's/^/    /'
  exit 1
fi

# Start second node
echo ""
echo "[4/5] Starting Node 2..."
mkdir -p data/node2
./build/example/example_counter_server 2 8002 8001 8003 > /tmp/node2.log 2>&1 &
NODE2_PID=$!
echo "  Node 2 PID: $NODE2_PID"
sleep 2

# Start third node
echo ""
echo "[5/5] Starting Node 3..."
mkdir -p data/node3
./build/example/example_counter_server 3 8003 8001 8002 > /tmp/node3.log 2>&1 &
NODE3_PID=$!
echo "  Node 3 PID: $NODE3_PID"
sleep 2

echo ""
echo "=========================================="
echo "Waiting 10 seconds for election..."
echo "=========================================="
sleep 10

echo ""
echo "Node 1 log (last 30 lines):"
tail -30 /tmp/node1.log | sed 's/^/  /'

echo ""
echo "Node 2 log (last 30 lines):"
tail -30 /tmp/node2.log | sed 's/^/  /'

echo ""
echo "Node 3 log (last 30 lines):"
tail -30 /tmp/node3.log | sed 's/^/  /'

echo ""
echo "=========================================="
echo "Testing client connection..."
echo "=========================================="
echo "inc" | timeout 5 ./build/example/example_counter_client 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003 2>&1 | head -20

echo ""
echo "=========================================="
echo "Cleaning up..."
echo "=========================================="
kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
sleep 1
echo "Done. Full logs saved to /tmp/node*.log"
