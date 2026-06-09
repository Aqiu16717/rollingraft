#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build_test"
BENCHMARK_BIN="${BUILD_DIR}/benchmark/benchmark_persister"
OUTPUT_DIR="${PROJECT_ROOT}/benchmark/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "${OUTPUT_DIR}"

COMBINED_CSV="${OUTPUT_DIR}/large_payload_benchmark_${TIMESTAMP}.csv"

echo "RollingRaft Large Payload + Concurrency Benchmark"
echo "=================================================="
echo "Output: ${COMBINED_CSV}"
echo ""

# Header
{
    echo "scenario,backend,entries,payload_bytes,batch_size,compression,threads,"
    echo "ops_per_sec,latency_p50_us,latency_p99_us,latency_avg_us,"
    echo "rss_kb,dir_size_mb,duration_ms,recovery_entries,reopen_ms,create_ms"
} | tr -d '\n' > "${COMBINED_CSV}"
echo "" >> "${COMBINED_CSV}"

run_scenario() {
    local backend="$1"
    local payload="$2"
    local threads="$3"
    local batch="$4"
    local compression="$5"
    local csv_tmp
    csv_tmp=$(mktemp)

    echo "[$(date '+%H:%M:%S')] backend=${backend} payload=${payload} threads=${threads} batch=${batch} compression=${compression}"
    
    "${BENCHMARK_BIN}" \
        --backend="${backend}" \
        --entries=10000 \
        --payload="${payload}" \
        --batch="${batch}" \
        --compression="${compression}" \
        --threads="${threads}" \
        --output="${csv_tmp}" \
        --data-dir="/tmp/rollingraft_largepayload_${backend}_${payload}_${threads}t"

    # Append all rows
    tail -n +2 "${csv_tmp}" >> "${COMBINED_CSV}"
    rm -f "${csv_tmp}"
}

# Focused matrix: payload sizes x backends x threads
PAYLOADS=(100 1024 10240 102400)
THREADS=(1 4)
BACKENDS=(leveldb hybrid)

for payload in "${PAYLOADS[@]}"; do
    for threads in "${THREADS[@]}"; do
        for backend in "${BACKENDS[@]}"; do
            # Primary: batch=10, compression=1 (Snappy)
            run_scenario "${backend}" "${payload}" "${threads}" "10" "1"
            
            # For 10KB, also test batch=1 and batch=100
            if [ "${payload}" -eq 10240 ]; then
                run_scenario "${backend}" "${payload}" "${threads}" "1" "1"
                run_scenario "${backend}" "${payload}" "${threads}" "100" "1"
            fi
            
            # For 100B baseline, also test no compression
            if [ "${payload}" -eq 100 ]; then
                run_scenario "${backend}" "${payload}" "${threads}" "10" "0"
            fi
        done
    done
done

echo ""
echo "Benchmark complete. CSV: ${COMBINED_CSV}"
