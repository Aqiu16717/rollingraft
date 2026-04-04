# RollingRaft Benchmarks

This directory contains performance benchmarking tools for RollingRaft.

## Benchmark Executables

### 1. benchmark_client - Throughput Benchmark

Measures write/read throughput under load.

```bash
# Basic usage
./benchmark_client 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003

# Read benchmark with 4 concurrent clients for 30 seconds
./benchmark_client -t read -d 30 -c 4 127.0.0.1:8001

# Large payload (1KB) benchmark with JSON output
./benchmark_client -s 1024 -o results.json 127.0.0.1:8001

# Mixed workload (80% write, 20% read)
./benchmark_client -t mixed 127.0.0.1:8001 127.0.0.1:8002
```

**Options:**
- `-t, --type <write|read|mixed>` - Benchmark type (default: write)
- `-d, --duration <seconds>` - Test duration (default: 10)
- `-c, --clients <n>` - Number of concurrent clients (default: 1)
- `-s, --size <bytes>` - Payload size (default: 100)
- `-o, --output <file>` - Save results to JSON file

**Output:**
```
========== Results ==========
Duration: 10000 ms
Total Operations: 52341
Throughput: 5234 ops/sec
Success Rate: 100.0%
Latency (us): min=120 avg=180 p50=150 p99=350 max=1200
```

---

### 2. benchmark_latency_curve - Latency vs Throughput

Measures latency at different throughput levels to find the performance "knee".

```bash
# Default test at 100, 200, 500, 1000, 2000, 5000 ops/sec
./benchmark_latency_curve 127.0.0.1:8001 127.0.0.1:8002

# Longer duration per level (10 seconds)
./benchmark_latency_curve -d 10 127.0.0.1:8001

# Large payload
./benchmark_latency_curve -s 4096 127.0.0.1:8001
```

**Options:**
- `-d, --duration <seconds>` - Duration per throughput level (default: 5)
- `-s, --size <bytes>` - Payload size (default: 100)

**Output:**
```
========== Latency Curve Results ==========
Target (ops/s)  Actual (ops/s)  Success%    P50 (us)    P99 (us)   P999 (us)
-----------------------------------------------------------------------------
100             98              100.0       120         180        250
500             495             100.0       125         200        280
1000            980             100.0       140         250        400
2000            1950            100.0       180         400        800
```

---

### 3. benchmark_failover - Failover Recovery Time

Measures cluster recovery time after leader failure.

```bash
# Kill leader process and measure recovery
./benchmark_failover \
  -k 'pkill -f "counter_server 1"' \
  -c 'curl -s http://localhost:8002/status' \
  127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003

# With custom operation rate and warmup
./benchmark_failover \
  -k 'docker stop raft-node-1' \
  -r 500 \
  -w 10 \
  127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003
```

**Options:**
- `-k, --kill <command>` - Shell command to kill leader (**required**)
- `-c, --check <command>` - Shell command to verify new leader elected
- `-r, --rate <ops/sec>` - Operation rate during test (default: 100)
- `-w, --warmup <seconds>` - Warmup duration (default: 5)

**Output:**
```
========== Failover Results ==========
Metric                          Value
--------------------------------------------------
Failure Detection Time:         245 ms
Leader Election Time:           312 ms
Total Recovery Time:            557 ms
Operations During Failover:     45
Failed Operations:              8
Availability During Failover:   82.2%
```

---

## Prerequisites

1. **Running Raft Cluster**: Start at least one Raft node before running benchmarks
   ```bash
   # Example: Start 3-node counter cluster
   ./example/counter/counter_server 1 8001 8002 8003 &
   ./example/counter/counter_server 2 8002 8001 8003 &
   ./example/counter/counter_server 3 8003 8001 8002 &
   ```

2. **Wait for Leader Election**: Ensure cluster has elected a leader
   ```bash
   # Leader address should not be empty
   ./example/counter/counter_client 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003
   > get
   ```

---

## Typical Workflow

### 1. Baseline Throughput Test
```bash
./benchmark_client -t write -d 60 -c 1 127.0.0.1:8001
```

### 2. Find Maximum Throughput
```bash
./benchmark_client -t write -d 30 -c 4 127.0.0.1:8001
./benchmark_client -t write -d 30 -c 8 127.0.0.1:8001
./benchmark_client -t write -d 30 -c 16 127.0.0.1:8001
```

### 3. Measure Latency Curve
```bash
./benchmark_latency_curve -d 10 127.0.0.1:8001
```

### 4. Test Failover
```bash
# Terminal 1: Run benchmark
./benchmark_failover -k 'pkill -f "counter_server 1"' 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003

# Benchmark will automatically kill the leader and measure recovery
```

---

## Interpreting Results

### Throughput
- **Single-client throughput**: Shows latency-optimal performance
- **Multi-client throughput**: Shows scalability under contention
- **Expected range**: 1,000 - 10,000+ ops/sec depending on hardware and network

### Latency
- **P50 (median)**: Typical operation latency (target: < 1ms local, < 5ms WAN)
- **P99**: Tail latency, 99% of operations complete within this time
- **P999**: Extreme tail, important for SLAs

### Failover
- **Detection time**: Network timeout + retry delays (~200-500ms typical)
- **Election time**: Raft election timeout (~300-600ms typical)
- **Total recovery**: Should be < 1-2 seconds for healthy clusters
- **Availability**: Percentage of operations succeeding during failover (> 80% target)

---

## Build

Benchmarks are built by default. To disable:
```bash
cmake .. -DBUILD_BENCHMARK=OFF
```

To build only benchmarks:
```bash
cmake --build build --target benchmark_client benchmark_latency_curve benchmark_failover
```
