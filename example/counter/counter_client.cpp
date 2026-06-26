#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "rollingraft/rpc.h"

// Client configuration for retry strategy
struct ClientConfig {
  // Timeout for single RPC call
  std::chrono::milliseconds rpc_timeout{5000};

  // Total timeout for operation (including retries)
  std::chrono::milliseconds operation_timeout{30000};

  // Initial retry delay
  std::chrono::milliseconds initial_retry_delay{100};

  // Max retry delay (exponential backoff cap)
  std::chrono::milliseconds max_retry_delay{2000};

  // Max retry attempts
  int max_retries{10};
};

// Calculate exponential backoff delay
std::chrono::milliseconds CalculateBackoff(int attempt, std::chrono::milliseconds initial,
                                           std::chrono::milliseconds max_delay) {
  // Exponential: 100ms, 200ms, 400ms, 800ms, 1000ms (capped)
  auto delay = initial * (1 << attempt);
  return std::min(delay, max_delay);
}

class CounterClient {
 public:
  explicit CounterClient(const std::vector<std::string>& servers,
                         const ClientConfig& config = ClientConfig())
      : servers_(servers),
        config_(config),
        connect_idx_(0),
        client_id_(GenerateClientId()),
        seq_(1) {}

  void SendCommand(const std::string& cmd) {
    rollingraft::ClientRequest req;
    req.command = cmd;
    req.client_id = client_id_;
    req.seq = seq_++;  // Increment seq for each command
    req.read_only = false;

    auto start_time = std::chrono::steady_clock::now();
    std::chrono::milliseconds delay = config_.initial_retry_delay;
    std::string current_leader;

    for (int attempt = 0; attempt < config_.max_retries; ++attempt) {
      // Check total timeout
      auto elapsed = std::chrono::steady_clock::now() - start_time;
      if (elapsed > config_.operation_timeout) {
        std::cerr << "[Client] Operation timeout after " << config_.operation_timeout.count()
                  << "ms" << std::endl;
        return;
      }

      rollingraft::ClientResponse resp;
      std::string target_addr = current_leader.empty() ? servers_[connect_idx_] : current_leader;

      std::cout << "[Client] Sending '" << cmd << "' to " << target_addr << " (attempt "
                << (attempt + 1) << "/" << config_.max_retries << ")..." << std::endl;

      rollingraft::Status status = rollingraft::RpcCall(target_addr, req, resp);

      if (status.ok()) {
        if (resp.success) {
          // Success!
          std::cout << "[Client] Success. Response: " << resp.response << std::endl;
          current_leader = target_addr;  // Remember working leader
          return;
        } else {
          // Not leader - redirect
          if (!resp.leader_addr.empty()) {
            std::cout << "[Client] Redirected to Leader: " << resp.leader_id << " ("
                      << resp.leader_addr << ")" << std::endl;
            current_leader = resp.leader_addr;
            // Retry immediately with new leader
            continue;
          } else {
            std::cerr << "[Client] Error: " << resp.error << std::endl;
            return;
          }
        }
      } else {
        // Network error - retry with backoff
        std::cerr << "[Client] RPC failed: " << status.GetMessage() << ", retrying in "
                  << delay.count() << "ms..." << std::endl;

        std::this_thread::sleep_for(delay);
        delay = CalculateBackoff(attempt + 1, config_.initial_retry_delay, config_.max_retry_delay);

        // Try next server if no known leader
        if (current_leader.empty()) {
          connect_idx_ = (connect_idx_ + 1) % servers_.size();
        }
      }
    }

    std::cerr << "[Client] Max retries exceeded, command failed" << std::endl;
  }

 private:
  // Generate random client ID
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

void PrintClientUsage(const char* prog) {
  std::cout << "Usage: " << prog << " <server1_addr> [server2_addr ...]\n";
  std::cout << "Example: " << prog << " 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003\n";
  std::cout << "Commands:\n";
  std::cout << "  inc       - Increment counter by 1\n";
  std::cout << "  dec       - Decrement counter by 1\n";
  std::cout << "  add N     - Add N to counter\n";
  std::cout << "  sub N     - Subtract N from counter\n";
  std::cout << "  exit      - Quit client\n";
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    PrintClientUsage(argv[0]);
    return 1;
  }

  std::vector<std::string> server_addrs;
  for (int i = 1; i < argc; ++i) {
    server_addrs.push_back(argv[i]);
  }

  std::cout << "[Client] Initialized with " << server_addrs.size() << " nodes.\n";
  std::cout << "[Client] Client ID will be auto-generated.\n";

  CounterClient client(server_addrs);

  std::string input;
  while (std::cout << "> " && std::getline(std::cin, input)) {
    if (input == "exit") {
      break;
    }
    if (input.empty()) {
      continue;
    }
    client.SendCommand(input);
  }

  std::cout << "[Client] Exiting..." << std::endl;
  return 0;
}
