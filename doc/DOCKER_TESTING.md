# Docker Integration Testing

This guide explains how to use Docker for integration testing of RollingRaft.

## Quick Start

```bash
# Run full Docker test suite
./scripts/docker-test.sh full

# Or step by step:
./scripts/docker-test.sh build    # Build images
./scripts/docker-test.sh up       # Start cluster
./scripts/docker-test.sh test     # Run tests
./scripts/docker-test.sh down     # Cleanup
```

## Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- 4GB+ available RAM
- Ports 8001-8003 available

## Architecture

The Docker setup creates a 3-node Raft cluster with:
- Each node in its own container
- Shared Docker network for communication
- Health checks for readiness
- Integration test runner container

## Commands

| Command | Description |
|---------|-------------|
| `build` | Build Docker images |
| `up` | Start 3-node cluster |
| `down` | Stop and cleanup |
| `status` | Show cluster status |
| `logs` | Stream logs from all nodes |
| `test` | Run integration tests |
| `client` | Interactive client session |
| `full` | Complete CI/CD pipeline |

## CI/CD Integration

```bash
# Single command for CI
./scripts/docker-test.sh full
```

This command:
1. Builds fresh images
2. Starts 3-node cluster
3. Waits for cluster to be healthy
4. Runs integration tests
5. Cleans up on exit

## Troubleshooting

**Port conflicts:**
```bash
lsof -i :8001 :8002 :8003
```

**Check logs:**
```bash
./scripts/docker-test.sh logs
```

**Full reset:**
```bash
./scripts/docker-test.sh down
./scripts/docker-test.sh full
```
