#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Run persister benchmark matrix for LevelDBPersister and HybridPersister.
# Produces combined CSV + a summary markdown report.
#
# Usage:
#   ./scripts/run_persister_benchmark.sh [BUILD_DIR] [OUTPUT_DIR]
#
# Defaults:
#   BUILD_DIR=build_test
#   OUTPUT_DIR=benchmark/results
# -----------------------------------------------------------------------------

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BUILD_DIR="${1:-${PROJECT_ROOT}/build_test}"
OUTPUT_DIR="${2:-${PROJECT_ROOT}/benchmark/results}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "${OUTPUT_DIR}"

BENCHMARK_BIN="${BUILD_DIR}/benchmark/benchmark_persister"
if [[ ! -x "${BENCHMARK_BIN}" ]]; then
    echo " benchmark_persister not found at ${BENCHMARK_BIN}"
    echo " Building..."
    cd "${PROJECT_ROOT}"
    cmake -S . -B "${BUILD_DIR}" >/dev/null
    cmake --build "${BUILD_DIR}" --target benchmark_persister -j$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)
fi

COMBINED_CSV="${OUTPUT_DIR}/persister_benchmark_${TIMESTAMP}.csv"
SUMMARY_MD="${OUTPUT_DIR}/persister_benchmark_${TIMESTAMP}.md"

# Full benchmark matrix
ENTRIES=50000
PAYLOAD=100
BATCH_SIZES="1,10,100"
COMPRESSION_MODES="0,1"

echo "RollingRaft Persister Benchmark Runner"
echo "======================================"
echo "Build:  ${BUILD_DIR}"
echo "Output: ${OUTPUT_DIR}"
echo "CSV:    ${COMBINED_CSV}"
echo ""

# Write combined CSV header
{
    echo "scenario,backend,entries,payload_bytes,batch_size,compression,ops_per_sec,latency_p50_us,latency_p99_us,latency_avg_us,rss_kb,dir_size_mb,duration_ms,recovery_entries,reopen_ms,create_ms"
} > "${COMBINED_CSV}"

run_backend() {
    local backend="$1"
    local csv_tmp
    csv_tmp=$(mktemp)

    echo "[$(date '+%H:%M:%S')] Running ${backend} backend..."
    "${BENCHMARK_BIN}" \
        --backend="${backend}" \
        --entries="${ENTRIES}" \
        --payload="${PAYLOAD}" \
        --batch="${BATCH_SIZES}" \
        --compression="${COMPRESSION_MODES}" \
        --output="${csv_tmp}" \
        --data-dir="/tmp/rollingraft_persister_bench_${backend}"

    # Append data rows (skip header) to combined CSV
    tail -n +2 "${csv_tmp}" >> "${COMBINED_CSV}"
    rm -f "${csv_tmp}"
    echo "[$(date '+%H:%M:%S')] ${backend} complete."
}

# Run both backends
run_backend "leveldb"
run_backend "hybrid"

echo ""
echo "Benchmark complete."
echo "Combined CSV: ${COMBINED_CSV}"

# Generate summary markdown
cat > "${SUMMARY_MD}" <<EOF
# Persister Benchmark Summary

**Timestamp**: ${TIMESTAMP}  
**Commit**: $(cd "${PROJECT_ROOT}" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")  
**Backends tested**: leveldb  
**Entries per scenario**: ${ENTRIES}  
**Payload size**: ${PAYLOAD} bytes  
**Batch sizes**: ${BATCH_SIZES}  
**Compression modes**: ${COMPRESSION_MODES}

## Raw Data

- [CSV]($(basename "${COMBINED_CSV}"))

## Quick Stats

### Append Throughput (ops/sec)

\`\`\`
$(awk -F',' '$1=="append" && $2=="leveldb" {printf "batch=%d compression=%d  ops/sec=%.0f  p50=%.1fus  p99=%.1fus  dir=%.2fMB\n", $5, $6, $7, $8, $9, $12}' "${COMBINED_CSV}")
\`\`\`

### Recovery Time (ms)

\`\`\`
$(awk -F',' '$1=="recovery" && $2=="leveldb" {printf "entries=%d compression=%d  reopen=%.0fms  create=%.0fms  dir=%.2fMB\n", $3, $6, $15, $16, $12}' "${COMBINED_CSV}")
\`\`\`

### Memory Footprint (KB)

\`\`\`
$(awk -F',' '$1=="memory" && $2=="leveldb" {printf "entries=%d compression=%d  rss_delta=%dKB  dir=%.2fMB\n", $3, $6, $11, $12}' "${COMBINED_CSV}")
\`\`\`

## T3 Phase 3 Notes

When \`HybridPersister\` is ready, uncomment the hybrid run in this script:

\`\`\`bash
run_backend "hybrid"
\`\`\`

Then re-run to produce side-by-side comparison CSV.
EOF

echo "Summary:    ${SUMMARY_MD}"
