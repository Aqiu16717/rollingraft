#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "rollingraft/rpc.h"

// Client configuration for retry strategy
struct ClientConfig {
  std::chrono::milliseconds rpc_timeout{5000};
  std::chrono::milliseconds operation_timeout{30000};
  std::chrono::milliseconds initial_retry_delay{100};
  std::chrono::milliseconds max_retry_delay{2000};
  int max_retries{10};
};

std::chrono::milliseconds CalculateBackoff(int attempt, std::chrono::milliseconds initial,
                                           std::chrono::milliseconds max_delay) {
  auto delay = initial * (1 << attempt);
  return std::min(delay, max_delay);
}

// Parse an input line into a target group_id and the raw command.
// Supported forms:
//   "inc"                -> group 1, command "inc"
//   "add 5"              -> group 1, command "add 5"
//   "g2 inc"             -> group 2, command "inc"
//   "2 add 5"            -> group 2, command "add 5"
bool ParseCommand(const std::string& input, uint64_t default_group, uint64_t& group_id,
                  std::string& command) {
  std::string trimmed = input;
  while (!trimmed.empty() && std::isspace(static_cast<unsigned char>(trimmed.front()))) {
    trimmed.erase(trimmed.begin());
  }
  while (!trimmed.empty() && std::isspace(static_cast<unsigned char>(trimmed.back()))) {
    trimmed.pop_back();
  }
  if (trimmed.empty()) {
    return false;
  }

  size_t pos = 0;
  while (pos < trimmed.size() && std::isspace(static_cast<unsigned char>(trimmed[pos]))) {
    ++pos;
  }
  size_t start = pos;
  while (pos < trimmed.size() && !std::isspace(static_cast<unsigned char>(trimmed[pos]))) {
    ++pos;
  }
  std::string first = trimmed.substr(start, pos - start);

  uint64_t parsed_group = default_group;
  size_t value_start = 0;
  if (!first.empty() && first.front() == 'g') {
    try {
      parsed_group = static_cast<uint64_t>(std::stoull(first.substr(1)));
      value_start = pos;
    } catch (...) {
      // first token is not a group selector, treat whole line as command for default group
      parsed_group = default_group;
      value_start = 0;
    }
  } else {
    try {
      parsed_group = static_cast<uint64_t>(std::stoull(first));
      value_start = pos;
    } catch (...) {
      parsed_group = default_group;
      value_start = 0;
    }
  }

  while (value_start < trimmed.size() &&
         std::isspace(static_cast<unsigned char>(trimmed[value_start]))) {
    ++value_start;
  }
  if (value_start >= trimmed.size()) {
    std::cerr << "[Client] Missing command after group selector" << std::endl;
    return false;
  }

  group_id = parsed_group;
  command = trimmed.substr(value_start);
  return true;
}

class MultiRaftClient {
 public:
  explicit MultiRaftClient(const std::vector<std::string>& servers,
                           const ClientConfig& config = ClientConfig())
      : servers_(servers),
        config_(config),
        connect_idx_(0),
        client_id_(GenerateClientId()),
        seq_(1) {}

  void SendCommand(uint64_t group_id, const std::string& cmd) {
    rollingraft::ClientRequest req;
    req.command = cmd;
    req.client_id = client_id_;
    req.seq = seq_++;
    req.read_only = false;
    req.group_id = group_id;

    auto start_time = std::chrono::steady_clock::now();
    std::chrono::milliseconds delay = config_.initial_retry_delay;
    std::string current_leader;

    for (int attempt = 0; attempt < config_.max_retries; ++attempt) {
      auto elapsed = std::chrono::steady_clock::now() - start_time;
      if (elapsed > config_.operation_timeout) {
        std::cerr << "[Client] Operation timeout after " << config_.operation_timeout.count()
                  << "ms" << std::endl;
        return;
      }

      rollingraft::ClientResponse resp;
      std::string target_addr = current_leader.empty() ? servers_[connect_idx_] : current_leader;

      std::cout << "[Client][group " << group_id << "] Sending '" << cmd << "' to " << target_addr
                << " (attempt " << (attempt + 1) << "/" << config_.max_retries << ")..."
                << std::endl;

      rollingraft::Status status = rollingraft::RpcCall(target_addr, req, resp);

      if (status.ok()) {
        if (resp.success) {
          std::cout << "[Client][group " << group_id << "] Success. Response: " << resp.response
                    << std::endl;
          current_leader = target_addr;
          return;
        } else {
          if (!resp.leader_addr.empty()) {
            std::cout << "[Client][group " << group_id
                      << "] Redirected to Leader: " << resp.leader_id << " (" << resp.leader_addr
                      << ")" << std::endl;
            current_leader = resp.leader_addr;
            continue;
          } else {
            std::cerr << "[Client][group " << group_id << "] Error: " << resp.error << std::endl;
            return;
          }
        }
      } else {
        std::cerr << "[Client][group " << group_id << "] RPC failed: " << status.GetMessage()
                  << ", retrying in " << delay.count() << "ms..." << std::endl;

        std::this_thread::sleep_for(delay);
        delay = CalculateBackoff(attempt + 1, config_.initial_retry_delay, config_.max_retry_delay);

        // If the failure was against the cached leader, drop the cache so the
        // next attempt falls back to round-robin; otherwise every remaining
        // retry would hammer the same dead server.
        if (!current_leader.empty() && target_addr == current_leader) {
          current_leader.clear();
        }
        if (current_leader.empty()) {
          connect_idx_ = (connect_idx_ + 1) % servers_.size();
        }
      }
    }

    std::cerr << "[Client][group " << group_id << "] Max retries exceeded, command failed"
              << std::endl;
  }

 private:
  static uint64_t GenerateClientId() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist;
    return dist(gen);
  }

  std::vector<std::string> servers_;
  ClientConfig config_;
  size_t connect_idx_;
  uint64_t client_id_;
  uint64_t seq_;
};

void PrintUsage(const char* prog) {
  std::cout << "Usage: " << prog << " <server1_addr> [server2_addr ...]\n";
  std::cout << "Example: " << prog << " 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003\n";
  std::cout << "Commands:\n";
  std::cout << "  inc                - Increment group 1 counter by 1\n";
  std::cout << "  dec                - Decrement group 1 counter by 1\n";
  std::cout << "  add N              - Add N to group 1 counter\n";
  std::cout << "  sub N              - Subtract N from group 1 counter\n";
  std::cout << "  g2 inc             - Increment group 2 counter by 1\n";
  std::cout << "  2 add N            - Add N to group 2 counter\n";
  std::cout << "  exit               - Quit client\n";
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    PrintUsage(argv[0]);
    return 1;
  }

  std::vector<std::string> server_addrs;
  for (int i = 1; i < argc; ++i) {
    server_addrs.push_back(argv[i]);
  }

  std::cout << "[Client] Initialized with " << server_addrs.size() << " nodes.\n";
  std::cout << "[Client] Client ID will be auto-generated.\n";

  MultiRaftClient client(server_addrs);

  std::string input;
  while (std::cout << "> " && std::getline(std::cin, input)) {
    if (input == "exit") {
      break;
    }
    if (input.empty()) {
      continue;
    }

    uint64_t group_id = 1;
    std::string cmd;
    if (!ParseCommand(input, /*default_group=*/1, group_id, cmd)) {
      continue;
    }
    client.SendCommand(group_id, cmd);
  }

  std::cout << "[Client] Exiting..." << std::endl;
  return 0;
}
