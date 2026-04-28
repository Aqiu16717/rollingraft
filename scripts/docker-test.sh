#!/bin/bash
#
# Docker-based integration testing for RollingRaft
#
# Usage:
#   ./scripts/docker-test.sh          # Run all tests
#   ./scripts/docker-test.sh build    # Build images only
#   ./scripts/docker-test.sh up       # Start cluster
#   ./scripts/docker-test.sh test     # Run integration tests
#   ./scripts/docker-test.sh down     # Stop and cleanup
#   ./scripts/docker-test.sh client   # Run interactive client
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"

# Detect docker compose command (v1 or v2)
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    DOCKER_COMPOSE="docker compose"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Build Docker images
cmd_build() {
    log_info "Building Docker images..."
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" build --no-cache
    log_info "Build complete!"
}

# Start the cluster
cmd_up() {
    log_info "Starting 3-node Raft cluster..."
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" up -d raft-node-1 raft-node-2 raft-node-3
    
    log_info "Waiting for cluster to be healthy..."
    sleep 5
    
    # Check health
    if $DOCKER_COMPOSE -f "$COMPOSE_FILE" ps | grep -q "healthy"; then
        log_info "Cluster is healthy!"
        show_cluster_status
    else
        log_warn "Cluster may still be starting, check status with: $DOCKER_COMPOSE -f $COMPOSE_FILE ps"
    fi
}

# Stop the cluster
cmd_down() {
    log_info "Stopping cluster and cleaning up..."
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" down -v
    log_info "Cleanup complete!"
}

# Show cluster status
cmd_status() {
    log_info "Cluster status:"
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" ps
}

# Show logs
cmd_logs() {
    log_info "Showing logs (Ctrl+C to exit)..."
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" logs -f raft-node-1 raft-node-2 raft-node-3
}

# Run integration tests
cmd_test() {
    log_info "Running integration tests..."
    
    # Ensure cluster is running
    if ! $DOCKER_COMPOSE -f "$COMPOSE_FILE" ps | grep -q "raft-node-1"; then
        log_warn "Cluster not running, starting..."
        cmd_up
        sleep 10
    fi
    
    # Run tests
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" --profile test run --rm integration-tests
    
    TEST_RESULT=$?
    
    if [ $TEST_RESULT -eq 0 ]; then
        log_info "All tests passed!"
    else
        log_error "Tests failed with exit code $TEST_RESULT"
    fi
    
    return $TEST_RESULT
}

# Run interactive client
cmd_client() {
    log_info "Starting interactive client..."
    
    # Ensure cluster is running
    if ! $DOCKER_COMPOSE -f "$COMPOSE_FILE" ps | grep -q "raft-node-1"; then
        log_warn "Cluster not running, starting..."
        cmd_up
        sleep 10
    fi
    
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" --profile client-test run --rm client-tests
}

# Run full test suite
cmd_full() {
    log_info "Running full Docker test suite..."
    
    cmd_down 2>/dev/null || true
    cmd_build
    cmd_up
    
    log_info "Waiting for cluster stabilization..."
    sleep 15
    
    cmd_test
    
    log_info "Test complete, cleaning up..."
    cmd_down
}

# Show help
cmd_help() {
    cat << EOF
RollingRaft Docker Test Script

Usage: $0 [command]

Commands:
    build   - Build Docker images
    up      - Start 3-node cluster
    down    - Stop cluster and cleanup
    status  - Show cluster status
    logs    - Show cluster logs
    test    - Run integration tests
    client  - Run interactive client
    full    - Run full test suite (build + test + cleanup)
    help    - Show this help

Examples:
    $0 up          # Start cluster for manual testing
    $0 test        # Run integration tests
    $0 full        # CI/CD: full test suite
    $0 client      # Interactive client session

EOF
}

# Show cluster status with details
show_cluster_status() {
    echo ""
    echo "=========================================="
    echo "Cluster Information"
    echo "=========================================="
    echo "Node 1: localhost:8001 (raft-node-1)"
    echo "Node 2: localhost:8002 (raft-node-2)"
    echo "Node 3: localhost:8003 (raft-node-3)"
    echo ""
    echo "To connect with client:"
    echo "  ./build/example/example_counter_client 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003"
    echo "=========================================="
}

# Main command dispatcher
case "${1:-help}" in
    build)
        cmd_build
        ;;
    up)
        cmd_up
        ;;
    down)
        cmd_down
        ;;
    status)
        cmd_status
        ;;
    logs)
        cmd_logs
        ;;
    test)
        cmd_test
        ;;
    client)
        cmd_client
        ;;
    full)
        cmd_full
        ;;
    help|--help|-h)
        cmd_help
        ;;
    *)
        log_error "Unknown command: $1"
        cmd_help
        exit 1
        ;;
esac
