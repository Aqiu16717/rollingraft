# Multi-Raft 3-Node Example + README Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a self-contained single-binary multi-raft demo (`example/multi_raft/multi_raft_3node.cpp`) and a README "Multi-Raft" section documenting `RaftStore`/`RaftGroup`/`SharedNodeInfra` with build/run instructions.

**Architecture:** The demo runs 3 `RaftStore` instances in one process (node IDs 1–3 on `127.0.0.1:9101–9103`), creates 2 groups per store each backed by a minimal in-memory `CounterMachine`, waits for a leader per group, proposes 5 increments per group, prints all six counters (must read 5), then stops everything gracefully and removes its temp data dirs. The README section explains the shared-infra model with a diagram and quick-start commands.

**Tech Stack:** C++20, ASIO (via RollingRaft internals), CMake + the project Makefile, Google-style clang-format.

**Spec:** `docs/superpowers/specs/2026-08-18-multi-raft-3node-example-design.md`

## Global Constraints

- Conventional Commits, imperative subject ≤ 50 chars, no AI attribution trailers.
- Braces on all `if` bodies; build warning-free under `-Wall -Wextra -Wpedantic`.
- Never edit `third_party/`, `cmake/patches/`, CI workflows, or Docker scripts.
- Every `Status` return value in the demo is checked; failures print and exit non-zero.
- Repo path convention is `example/` (singular) — the issue text's `examples/` is deliberately not used.
- Do not modify the existing `multi_raft_server.cpp` / `multi_raft_client.cpp`.
- Do not push without human approval; do not close issue #18 without human approval.

---

### Task 1: Demo binary + CMake target

**Files:**
- Create: `example/multi_raft/multi_raft_3node.cpp`
- Modify: `example/CMakeLists.txt`
- Reference (read, do not edit): `example/multi_raft/multi_raft_server.cpp` (CounterMachine pattern), `tests/integration/test_multi_raft_2groups.cpp` (3-store setup pattern), `src/raft_store.h` (API)

**Interfaces:**
- Consumes: `rollingraft::RaftStore(store_config)` → `Initialize()` / `Start()` / `Stop()` / `CreateGroup(gid, options, sm)` / `GetGroup(gid)` (returns `RaftNode::RaftNodeImpl*` with `IsLeader()` and `Propose(data, callback)` returning `Status`); `RaftStoreConfig{node_id, listen_addr, peers, peer_node_ids, data_dir}`; `RaftGroupOptions{group_id, election_timeout_ms, heartbeat_interval_ms}`; `rollingraft::StateMachine` virtuals (`Apply`, `Query`, `GetLastAppliedIndex`, `CreateSnapshot`, `Restore`, `WaitIndex`) and `rollingraft::Snapshot`.
- Produces: executable `example_multi_raft_3node` in `build/<config>/example/`; demo exits 0 with all six counters at 5 and temp dirs removed.

- [ ] **Step 1: Write the demo source**

Create `example/multi_raft/multi_raft_3node.cpp` with this exact content:

```cpp
/**
 * @file multi_raft_3node.cpp
 * @brief Single-binary multi-raft demo: 3 nodes, 2 Raft groups per node
 *
 * Runs three RaftStore instances in one process (one per physical node) and
 * creates two independent Raft groups on each. The demo waits for a leader
 * per group, proposes five counter increments through each leader, prints
 * the per-node counter state for both groups (all values must converge to
 * 5), then shuts the stores down and removes its data directories.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rollingraft/persister.h"
#include "rollingraft/state_machine.h"

#include "raft_store.h"

namespace {

// In-memory snapshot of the counter value (mirrors the server example).
class CounterSnapshot : public rollingraft::Snapshot {
 public:
  CounterSnapshot(int64_t value, uint64_t index, uint64_t term) : value_(value) {
    meta_.last_included_index_ = index;
    meta_.last_included_term_ = term;
    data_.resize(sizeof(int64_t));
    std::memcpy(data_.data(), &value_, sizeof(int64_t));
  }

  const rollingraft::SnapshotMeta& GetMeta() const override { return meta_; }

  size_t Read(uint64_t offset, uint8_t* dest, size_t length) override {
    if (offset > data_.size()) {
      return 0;
    }
    size_t can_read = std::min(data_.size() - static_cast<size_t>(offset), length);
    std::memcpy(dest, data_.data() + offset, can_read);
    return can_read;
  }

  std::string GetPath() const override { return ""; }

 private:
  rollingraft::SnapshotMeta meta_;
  int64_t value_;
  std::vector<uint8_t> data_;
};

// Minimal in-memory counter state machine. Mirrors the CounterMachine in
// example/multi_raft/multi_raft_server.cpp but only supports "inc"/"dec".
class CounterMachine : public rollingraft::StateMachine {
 public:
  rollingraft::ApplyResult Apply(std::span<const uint8_t> data, uint64_t index) override {
    std::lock_guard<std::mutex> lock(mtx_);
    std::string cmd(data.begin(), data.end());
    if (cmd == "inc") {
      ++value_;
    } else if (cmd == "dec") {
      --value_;
    }
    last_applied_index_ = index;
    NotifyWaitersLocked(index);
    rollingraft::ApplyResult result;
    result.success = true;
    result.response = std::to_string(value_);
    result.applied_index = index;
    return result;
  }

  uint64_t GetLastAppliedIndex() const override { return last_applied_index_.load(); }

  std::shared_ptr<rollingraft::Snapshot> CreateSnapshot() override {
    std::lock_guard<std::mutex> lock(mtx_);
    return std::make_shared<CounterSnapshot>(value_, last_applied_index_, 0);
  }

  bool Restore(const std::vector<uint8_t>& snapshot) override {
    std::lock_guard<std::mutex> lock(mtx_);
    if (snapshot.size() < sizeof(int64_t)) {
      return false;
    }
    std::memcpy(&value_, snapshot.data(), sizeof(int64_t));
    return true;
  }

  void WaitIndex(uint64_t index, std::function<void()> cb) override {
    std::lock_guard<std::mutex> lock(mtx_);
    if (last_applied_index_.load() >= index) {
      cb();
    } else {
      waiters_.emplace(index, std::move(cb));
    }
  }

  rollingraft::ApplyResult Query(std::span<const uint8_t> /*data*/) override {
    std::lock_guard<std::mutex> lock(mtx_);
    rollingraft::ApplyResult result;
    result.success = true;
    result.response = std::to_string(value_);
    result.applied_index = last_applied_index_.load();
    return result;
  }

  int64_t GetValue() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return value_;
  }

 private:
  // Caller must hold mtx_. Fires and removes all waiters whose index has been
  // applied; without this, WaitIndex callbacks would never run.
  void NotifyWaitersLocked(uint64_t index) {
    auto it = waiters_.begin();
    while (it != waiters_.end() && it->first <= index) {
      it->second();
      it = waiters_.erase(it);
    }
  }

  mutable std::mutex mtx_;
  int64_t value_ = 0;
  std::atomic<uint64_t> last_applied_index_{0};
  std::multimap<uint64_t, std::function<void()>> waiters_;
};

constexpr int kNumNodes = 3;
constexpr uint16_t kBasePort = 9101;  // Avoids the docker-compose cluster (8001-8003)
const std::vector<uint64_t> kGroupIds = {1, 2};
constexpr int kProposalsPerGroup = 5;

// Poll all stores until one reports a leader for `group_id`, or timeout.
rollingraft::RaftNode::RaftNodeImpl* GetGroupLeader(
    const std::vector<std::unique_ptr<rollingraft::RaftStore>>& stores, uint64_t group_id,
    int timeout_sec = 10) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
  while (std::chrono::steady_clock::now() < deadline) {
    for (const auto& store : stores) {
      auto* group = store->GetGroup(group_id);
      if (group && group->IsLeader()) {
        return group;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return nullptr;
}

// Propose one command through the group leader and wait for its apply
// callback (with a deadline). Returns false on proposal failure or timeout.
bool ProposeAndWait(rollingraft::RaftNode::RaftNodeImpl* leader, const std::string& cmd,
                    int timeout_sec = 5) {
  std::atomic<bool> done{false};
  auto status = leader->Propose(cmd, [&done](const rollingraft::ApplyResult& /*result*/) {
    done.store(true, std::memory_order_relaxed);
  });
  if (!status.ok()) {
    std::cerr << "Propose failed: " << status.ToString() << std::endl;
    return false;
  }
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
  while (!done.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  if (!done.load(std::memory_order_relaxed)) {
    std::cerr << "Propose timed out: " << cmd << std::endl;
    return false;
  }
  return true;
}

}  // namespace

int main() {
  const std::filesystem::path data_root = "/tmp/rollingraft_3node_demo";
  std::filesystem::remove_all(data_root);
  std::filesystem::create_directories(data_root);

  // group_id -> per-node state machine (index = node_id - 1).
  std::unordered_map<uint64_t, std::vector<std::shared_ptr<CounterMachine>>> machines;

  std::vector<std::unique_ptr<rollingraft::RaftStore>> stores;
  stores.reserve(kNumNodes);

  auto fail = [&](const std::string& msg) {
    std::cerr << "FAILED: " << msg << std::endl;
    for (auto& store : stores) {
      auto stop_status = store->Stop();
      if (!stop_status.ok()) {
        std::cerr << "warning: stop failed: " << stop_status.ToString() << std::endl;
      }
    }
    std::filesystem::remove_all(data_root);
    return 1;
  };

  // 1. Create and initialize one store per physical node.
  for (int i = 0; i < kNumNodes; ++i) {
    rollingraft::RaftStoreConfig config;
    config.node_id = i + 1;
    config.listen_addr = "127.0.0.1:" + std::to_string(kBasePort + i);
    config.data_dir = (data_root / ("node" + std::to_string(i + 1))).string();
    for (int j = 0; j < kNumNodes; ++j) {
      if (j != i) {
        config.peers.push_back("127.0.0.1:" + std::to_string(kBasePort + j));
        config.peer_node_ids.push_back(j + 1);
      }
    }
    auto store = std::make_unique<rollingraft::RaftStore>(config);
    auto status = store->Initialize();
    if (!status.ok()) {
      return fail("store " + std::to_string(i + 1) + " init: " + status.ToString());
    }
    stores.push_back(std::move(store));
  }

  // 2. Create both groups on every node before starting any store.
  for (uint64_t group_id : kGroupIds) {
    auto& group_machines = machines[group_id];
    for (int i = 0; i < kNumNodes; ++i) {
      rollingraft::RaftGroupOptions options;
      options.group_id = group_id;
      options.election_timeout_ms = 300;
      options.heartbeat_interval_ms = 50;
      auto sm = std::make_shared<CounterMachine>();
      group_machines.push_back(sm);
      auto status = stores[i]->CreateGroup(group_id, options, std::move(sm));
      if (!status.ok()) {
        return fail("CreateGroup " + std::to_string(group_id) + " on node " +
                    std::to_string(i + 1) + ": " + status.ToString());
      }
    }
  }

  // 3. Start all stores.
  for (int i = 0; i < kNumNodes; ++i) {
    auto status = stores[i]->Start();
    if (!status.ok()) {
      return fail("store " + std::to_string(i + 1) + " start: " + status.ToString());
    }
  }

  std::cout << "-------------------------------------------\n";
  std::cout << " Multi-Raft 3-node demo started\n";
  std::cout << " nodes: 127.0.0.1:" << kBasePort << "-" << (kBasePort + kNumNodes - 1)
            << "\n";
  std::cout << " groups per node: " << kGroupIds.size() << "\n";
  std::cout << " data: " << data_root << "\n";
  std::cout << "-------------------------------------------\n";

  // 4. Wait for a leader per group and propose.
  for (uint64_t group_id : kGroupIds) {
    auto* leader = GetGroupLeader(stores, group_id);
    if (leader == nullptr) {
      return fail("no leader elected for group " + std::to_string(group_id));
    }
    std::cout << "group " << group_id << ": leader elected, proposing "
              << kProposalsPerGroup << " increments\n";
    for (int i = 0; i < kProposalsPerGroup; ++i) {
      if (!ProposeAndWait(leader, "inc")) {
        return fail("proposal " + std::to_string(i) + " on group " + std::to_string(group_id));
      }
    }
  }

  // 5. Wait for every node to observe all proposals. Followers apply one RPC
  //    round-trip behind the leader, so the leader's apply callbacks alone do
  //    not guarantee the followers have caught up yet.
  bool converged = false;
  auto converge_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < converge_deadline) {
    converged = true;
    for (uint64_t group_id : kGroupIds) {
      for (const auto& machine : machines[group_id]) {
        if (machine->GetValue() != kProposalsPerGroup) {
          converged = false;
        }
      }
    }
    if (converged) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // 6. Print the final state: every node x group must read the same value
  //    (per-group state is replicated independently).
  std::cout << "\nFinal counter state (all cells must read " << kProposalsPerGroup
            << "):\n";
  std::cout << "              group 1   group 2\n";
  for (int i = 0; i < kNumNodes; ++i) {
    std::cout << "  node " << (i + 1) << "        " << machines[1][i]->GetValue() << "         "
              << machines[2][i]->GetValue() << "\n";
  }
  if (!converged) {
    return fail("counters did not converge: expected every cell to read " +
                std::to_string(kProposalsPerGroup) + " within 5s");
  }

  // 7. Graceful shutdown and cleanup.
  for (auto& store : stores) {
    auto stop_status = store->Stop();
    if (!stop_status.ok()) {
      std::cerr << "warning: stop failed: " << stop_status.ToString() << std::endl;
    }
  }
  stores.clear();
  std::filesystem::remove_all(data_root);

  std::cout << "\nDemo complete: all groups converged, stores stopped, data removed.\n";
  return 0;
}
```

- [ ] **Step 2: Register the target in CMake**

In `example/CMakeLists.txt`, after the existing multi-raft server/client targets (line 13-17 area), add:

```cmake
# Multi-raft single-binary 3-node demo
add_executable(example_multi_raft_3node multi_raft/multi_raft_3node.cpp)
target_link_libraries(example_multi_raft_3node PRIVATE rollingraft)
target_include_directories(example_multi_raft_3node PRIVATE ${PROJECT_SOURCE_DIR}/src)
```

And add `example_multi_raft_3node` to the `install(TARGETS ...)` list at the bottom of the file.

- [ ] **Step 3: Build**

Run: `make release`
Expected: build completes with no warnings; `build/release/example/example_multi_raft_3node` exists.

- [ ] **Step 4: Run the demo and verify output**

Run: `./build/release/example/example_multi_raft_3node`
Expected: exit code 0. Output contains:
- "Multi-Raft 3-node demo started"
- "group 1: leader elected" and "group 2: leader elected"
- The final table: six cells, all reading `5`. (The demo polls up to 5 s for
  follower convergence — followers apply one RPC round-trip behind the
  leader — and exits non-zero with a message if any cell fails to converge.)
- "Demo complete: all groups converged, stores stopped, data removed."
- `/tmp/rollingraft_3node_demo` no longer exists afterwards.

Run it a second time to confirm re-runs are clean (dir recreated and removed).

- [ ] **Step 5: Format check**

Run: `make format-check`
Expected: `Code formatting OK!`

- [ ] **Step 6: Commit**

```bash
git add example/multi_raft/multi_raft_3node.cpp example/CMakeLists.txt
git commit -m "feat(example): add multi-raft 3-node demo binary"
```

---

### Task 2: README multi-raft section

**Files:**
- Modify: `README.md` (insert new section after the existing single-group usage docs, before any closing sections)

**Interfaces:**
- Consumes: the demo binary from Task 1 (`example_multi_raft_3node`), the existing `example_multi_raft_server` / `example_multi_raft_client`, `tests/integration/test_multi_raft_2groups.cpp`.
- Produces: a README section users can follow end-to-end with the commands shown.

- [ ] **Step 1: Add the section**

Insert the following into `README.md` (adjust the anchor placement to sit naturally after the single-group examples):

````markdown
## Multi-Raft (Multiple Groups per Node)

RollingRaft can host many independent Raft groups in one process, sharing a
single network transport, timer service, and protocol stack:

- **`SharedNodeInfra`** — per-node shared infrastructure: network transport,
  timer service, protocol codec, metrics registry.
- **`RaftGroup`** — all per-group state: term, log, leader state, membership.
  Groups on the same node elect leaders and replicate independently.
- **`RaftStore`** — owns the infra plus the group table. Inbound RPCs carry a
  `group_id` and are routed to the matching group (`group_id == 0` keeps the
  legacy single-group path).

```
                 ┌─────────────────────────────────────┐
  node 1         │  RaftStore                          │
  (127.0.0.1)    │  ┌────────────┐  ┌────────────┐     │
                 │  │ group 1    │  │ group 2    │     │
                 │  │ term/log/  │  │ term/log/  │     │
                 │  │ leader     │  │ leader     │     │
                 │  └────────────┘  └────────────┘     │
                 │  SharedNodeInfra: transport/timers/ │
                 │  protocol/metrics (one per node)    │
                 └─────────────────────────────────────┘
        replicated across node 1, node 2, node 3 (each runs a RaftStore)
```

### Quick start (single binary, 3 nodes in one process)

```bash
make release
./build/release/example/example_multi_raft_3node
```

The demo starts 3 nodes on `127.0.0.1:9101-9103`, creates 2 groups per node,
waits for a leader per group, proposes 5 increments per group, and prints the
converged counters — every node × group cell must read `5`:

```
-------------------------------------------
 Multi-Raft 3-node demo started
 nodes: 127.0.0.1:9101-9103
 groups per node: 2
 data: /tmp/rollingraft_3node_demo
-------------------------------------------
group 1: leader elected, proposing 5 increments
group 2: leader elected, proposing 5 increments

Final counter state (all cells must read 5):
              group 1   group 2
  node 1        5         5
  node 2        5         5
  node 3        5         5

Demo complete: all groups converged, stores stopped, data removed.
```

### Running nodes in separate processes (multi-host)

For a multi-host deployment, run one `RaftStore` per process:

```bash
./build/release/example/example_multi_raft_server 1 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003
./build/release/example/example_multi_raft_server 2 127.0.0.1:8002 127.0.0.1:8001 127.0.0.1:8003
./build/release/example/example_multi_raft_server 3 127.0.0.1:8003 127.0.0.1:8001 127.0.0.1:8002
```

Each process hosts groups 1 and 2; see `example/multi_raft/multi_raft_client.cpp`
for the matching client. Integration coverage lives in
`tests/integration/test_multi_raft_2groups.cpp`.
````

- [ ] **Step 2: Verify markdown structure**

Run: `grep -n "^## " README.md`
Expected: the new "## Multi-Raft (Multiple Groups per Node)" heading appears exactly once, in a sensible order relative to other top-level sections.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add multi-raft usage guide to README"
```

---

### Task 3: Acceptance sweep

**Files:** none (verification only)

**Interfaces:** consumes the deliverables of Tasks 1-2.

- [ ] **Step 1: Full test suite**

Run: `make test`
Expected: `100% tests passed` (371 tests; the example is compile-only and does not add test cases).

- [ ] **Step 2: Warning-free werror build**

Run: `make werror`
Expected: all rollingraft/src/example objects compile clean under `-Werror`.
Known pre-existing local failures (not part of this work, do not fix here):
`third_party/googletest` (`gtest-printers.h:524`, `char8_t` conversion) and
`benchmark/protobuf_mvp_benchmark.cpp:60` (sign-compare). CI's werror job is
the authoritative gate.

- [ ] **Step 3: Demo end-to-end re-run**

Run: `./build/release/example/example_multi_raft_3node`
Expected: exit 0, all six counters read 5 (same checks as Task 1 Step 4).

- [ ] **Step 4: Map acceptance criteria and report**

Check against the issue checklist:
- `multi_raft_3node.cpp` compiles and runs locally ✓ (Task 1)
- README has a multi-raft section ✓ (Task 2)
- Format check passes ✓ (Task 1 Step 5)
- CI build passes — the example compiles in the default build matrix; confirm on the next CI run after push (do not push without human approval)

Report results to the human; do NOT close issue #18 without explicit approval.

---

## Self-Review Notes

- Spec coverage: demo binary (Task 1), README section + diagram + both run modes (Task 2), verification + acceptance mapping (Task 3). CMake registration folded into Task 1 per the spec's build-integration section. ✓
- Placeholder scan: no TBD/TODO; all code blocks complete. The README sample output shows representative (not guaranteed-identical) leader election output — acceptable for docs. ✓
- Type consistency: `CounterMachine` / `GetGroupLeader` / `ProposeAndWait` signatures are identical across all steps; `machines[1][i]` indexing matches `node_id - 1` convention stated in the comment. ✓
