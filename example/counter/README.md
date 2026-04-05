# Counter Example

A distributed counter service built with RollingRaft.

This example demonstrates:
- Implementing a `StateMachine` for a simple counter
- Leader election and log replication
- Client interaction with leader redirection
- Snapshot support for log compaction

## Building

```bash
cd rollingraft
cmake --build build --target example_counter_server example_counter_client
```

## Running

### 1. Start a 3-node cluster

Open three terminals:

**Terminal 1 (Node 1):**
```bash
mkdir -p data/node1
./build/example/counter/counter_server 1 8001 8002 8003
```

**Terminal 2 (Node 2):**
```bash
mkdir -p data/node2
./build/example/counter/counter_server 2 8002 8001 8003
```

**Terminal 3 (Node 3):**
```bash
mkdir -p data/node3
./build/example/counter/counter_server 3 8003 8001 8002
```

Wait for leader election (you'll see "Current count value" messages from the leader).

### 2. Connect with client

**Terminal 4 (Client):**
```bash
./build/example/counter/counter_client 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003
```

### 3. Try commands

```
> inc
[Client] Success. Response: 1
> inc
[Client] Success. Response: 2
> add 10
[Client] Success. Response: 12
> dec
[Client] Success. Response: 11
> exit
```

## Commands

- `inc` - Increment counter by 1
- `dec` - Decrement counter by 1
- `add N` - Add N to counter
- `sub N` - Subtract N from counter
- `exit` - Quit client

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Node 1    │────▶│   Node 2    │◀────│   Node 3    │
│  (Leader)   │◀────│  (Follower) │────▶│  (Follower) │
└──────┬──────┘     └─────────────┘     └─────────────┘
       │
       │ Propose commands
       ▼
┌─────────────┐
│   Client    │
└─────────────┘
```

## Files

- `counter_server.cpp` - Raft node with counter state machine
- `counter_client.cpp` - Interactive client with retry logic

## Notes

- Data is persisted in `data/node*/` directories
- Only the leader accepts write commands
- Client automatically redirects to the leader
- Counter value is replicated to all nodes
