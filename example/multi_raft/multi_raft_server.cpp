#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rollingraft/persister.h"
#include "rollingraft/raft_node.h"
#include "rollingraft/state_machine.h"

#include "raft_store.h"

namespace {

class CounterSnapshot : public rollingraft::Snapshot {
 public:
  CounterSnapshot(int64_t value, uint64_t index, uint64_t term) : value_(value) {
    meta_.last_included_index_ = index;
    meta_.last_included_term_ = term;
    data_.resize(sizeof(int64_t));
    std::memcpy(data_.data(), &value_, sizeof(int64_t));
  }

  ~CounterSnapshot() = default;

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

class CounterMachine : public rollingraft::StateMachine {
 public:
  CounterMachine() = default;

  rollingraft::ApplyResult Apply(std::span<const uint8_t> data, uint64_t index) override {
    std::lock_guard<std::mutex> lock(mtx_);
    std::string cmd(data.begin(), data.end());
    // Malformed numeric arguments must not escape as exceptions: the apply
    // loop has no try/catch, so a throw here would terminate the process.
    try {
      if (cmd == "inc") {
        ++value_;
      } else if (cmd == "dec") {
        --value_;
      } else if (cmd.rfind("add ", 0) == 0) {
        value_ += std::stoll(cmd.substr(4));
      } else if (cmd.rfind("sub ", 0) == 0) {
        value_ -= std::stoll(cmd.substr(4));
      }
    } catch (const std::exception& e) {
      rollingraft::ApplyResult error;
      error.success = false;
      error.error_message = std::string("invalid command: ") + e.what();
      error.applied_index = index;
      return error;
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

std::atomic<bool> g_running{true};

void SignalHandler(int signum) {
  std::cout << "\nInterrupt signal (" << signum << ") received. Shutting down...\n";
  g_running = false;
}

void PrintUsage(const char* prog) {
  std::cout << "Usage: " << prog << " <node_id> <listen_addr> <peer1_addr> [peer2_addr ...]\n";
  std::cout << "Examples:\n";
  std::cout << "  " << prog << " 1 8001 8002 8003\n";
  std::cout << "  " << prog << " 1 0.0.0.0:8001 0.0.0.0:8002 0.0.0.0:8003\n";
  std::cout << "Note: node_id must be unique and consistent across the cluster.\n";
}

std::string NormalizeAddr(std::string addr) {
  if (addr.find(':') == std::string::npos) {
    addr = "127.0.0.1:" + addr;
  }
  return addr;
}

// Canonicalize an address for node-ID mapping: "0.0.0.0" (bind-any) and
// "127.0.0.1" denote the same endpoint, and mixing spellings across nodes
// would otherwise produce divergent ID mappings with no error.
std::string CanonicalAddr(const std::string& addr) {
  if (addr.rfind("0.0.0.0:", 0) == 0) {
    return "127.0.0.1:" + addr.substr(8);
  }
  return addr;
}

// Build a stable node_id -> address mapping from the listen address and all
// peer addresses.  IDs are assigned by sorting addresses lexicographically so
// every node in the cluster derives the same mapping without central coord.
bool BuildNodeMapping(const std::string& listen_addr, const std::vector<std::string>& peer_addrs,
                      rollingraft::NodeId requested_node_id, rollingraft::NodeId& out_node_id,
                      std::vector<rollingraft::NodeId>& out_peer_node_ids,
                      std::vector<std::string>& out_peers) {
  std::set<std::string> sorted;
  sorted.insert(CanonicalAddr(listen_addr));
  for (const auto& peer : peer_addrs) {
    sorted.insert(CanonicalAddr(peer));
  }

  std::unordered_map<std::string, rollingraft::NodeId> addr_to_id;
  rollingraft::NodeId next_id = 1;
  for (const auto& addr : sorted) {
    addr_to_id[addr] = next_id++;
  }

  auto it = addr_to_id.find(CanonicalAddr(listen_addr));
  if (it == addr_to_id.end()) {
    return false;
  }
  out_node_id = it->second;
  if (out_node_id != requested_node_id) {
    std::cerr << "[Warning] requested node_id " << requested_node_id
              << " does not match derived node_id " << out_node_id << ", using derived id"
              << std::endl;
  }

  out_peers = peer_addrs;
  out_peer_node_ids.clear();
  for (const auto& peer : peer_addrs) {
    auto id_it = addr_to_id.find(CanonicalAddr(peer));
    if (id_it == addr_to_id.end()) {
      return false;
    }
    out_peer_node_ids.push_back(id_it->second);
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 4) {
    PrintUsage(argv[0]);
    return 1;
  }

  rollingraft::NodeId requested_node_id = 0;
  try {
    requested_node_id = std::stoll(argv[1]);
  } catch (const std::exception&) {
    std::cerr << "Invalid node_id: '" << argv[1] << "'" << std::endl;
    PrintUsage(argv[0]);
    return 1;
  }
  std::string listen_addr = NormalizeAddr(argv[2]);

  std::vector<std::string> peer_addrs;
  for (int i = 3; i < argc; ++i) {
    peer_addrs.push_back(NormalizeAddr(argv[i]));
  }

  rollingraft::NodeId node_id = requested_node_id;
  std::vector<rollingraft::NodeId> peer_node_ids;
  std::vector<std::string> peers;
  if (!BuildNodeMapping(listen_addr, peer_addrs, requested_node_id, node_id, peer_node_ids,
                        peers)) {
    std::cerr << "Failed to build stable node ID mapping from addresses" << std::endl;
    return 1;
  }

  rollingraft::RaftStoreConfig store_config;
  store_config.node_id = node_id;
  store_config.listen_addr = listen_addr;
  store_config.peers = std::move(peers);
  store_config.peer_node_ids = std::move(peer_node_ids);
  store_config.data_dir = "./data_mraft/node" + std::to_string(node_id);
  // Multi-raft metrics are still single-server-per-store in this release, so
  // keep them disabled in the example to avoid groups overwriting each other.
  store_config.metrics_enabled = false;

  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  try {
    rollingraft::RaftStore store(store_config);
    auto status = store.Initialize();
    if (!status.ok()) {
      std::cerr << "Failed to initialize store: " << status.ToString() << std::endl;
      return 1;
    }

    // Create two independent Raft groups on the same store.
    // Each group reuses the same peer topology but maintains isolated state.
    std::unordered_map<uint64_t, std::shared_ptr<CounterMachine>> machines;
    {
      rollingraft::RaftGroupOptions options;
      options.group_id = 1;
      auto sm = std::make_shared<CounterMachine>();
      machines[1] = sm;
      status = store.CreateGroup(1, options, std::move(sm));
      if (!status.ok()) {
        std::cerr << "Failed to create group 1: " << status.ToString() << std::endl;
        return 1;
      }
    }
    {
      rollingraft::RaftGroupOptions options;
      options.group_id = 2;
      auto sm = std::make_shared<CounterMachine>();
      machines[2] = sm;
      status = store.CreateGroup(2, options, std::move(sm));
      if (!status.ok()) {
        std::cerr << "Failed to create group 2: " << status.ToString() << std::endl;
        return 1;
      }
    }

    status = store.Start();
    if (!status.ok()) {
      std::cerr << "Failed to start store: " << status.ToString() << std::endl;
      return 1;
    }

    std::cout << "-------------------------------------------\n";
    std::cout << " Multi-Raft Node " << node_id << " Started\n";
    std::cout << " Listening on: " << listen_addr << "\n";
    std::cout << " Groups: 1, 2\n";
    std::cout << "-------------------------------------------\n";

    while (g_running) {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      for (uint64_t gid : store.ListGroups()) {
        auto* group = store.GetGroup(gid);
        if (!group) {
          continue;
        }
        std::cout << "[group " << gid
                  << "] role=" << rollingraft::RaftNodeRoleToString(group->GetRole())
                  << " term=" << group->CurrentTerm() << " leader_addr=" << group->GetLeaderAddr()
                  << " count=" << machines[gid]->GetValue() << "\n";
      }
    }

    std::cout << "Stopping multi-raft store..." << std::endl;
    store.Stop();
  } catch (const std::exception& e) {
    std::cerr << "Uncaught exception: " << e.what() << std::endl;
    return 1;
  }

  std::cout << "Node exited safely" << std::endl;
  return 0;
}
