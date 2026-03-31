#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "rollingraft/rpc.h"

class CounterClient {
 public:
  explicit CounterClient(const std::vector<std::string>& servers)
      : servers_(servers), connect_idx_(0), client_id_(12345), seq_(1) {}

  void SendCommand(const std::string& cmd) {
    rollingraft::ClientRequest req;
    req.command = cmd;
    req.client_id = client_id_;
    req.seq = seq_++;

    bool success = false;
    while (!success) {
      rollingraft::ClientResponse resp;
      std::string current_addr = servers_[connect_idx_];

      std::cout << "[Client] Sending '" << cmd << "' to Node " << connect_idx_
                << " (" << current_addr << ")..." << std::endl;
      rollingraft::Status status =
          rollingraft::RpcCall(current_addr, req, resp);
      if (status.ok()) {
        if (resp.success) {
          std::cout << "[Client] Success. Response: " << resp.response
                    << std::endl;
          success = true;
        } else {
          std::cout << "[Client] Redirected to Node: " << resp.leader_id << "("
                    << resp.leader_addr << ")" << std::endl;
          current_addr = resp.leader_addr;
          continue;
        }
      } else {
        std::cerr << "[Client] Connection failed, trying next..." << std::endl;
        connect_idx_ = (connect_idx_ + 1) % servers_.size();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    }
    rollingraft::ClientResponse resp;
  }

 private:
  std::vector<std::string> servers_;
  size_t connect_idx_;
  uint64_t client_id_;
  uint64_t seq_;
};

void PrintClientUsage(const char* prog) {
  std::cout << "Usage: " << prog << " <server1_addr> [server2_addr ...]\n";
  std::cout << "Example: " << prog
            << " 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003\n";
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

  std::cout << "[Client] Initialized with " << server_addrs.size()
            << " nodes.\n";

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

  return 0;
}
