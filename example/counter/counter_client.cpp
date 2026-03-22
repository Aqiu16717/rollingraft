#include <iostream>
#include <string>
#include <vector>

clas CounterClient {
 public:
  CounterClient() {}

 private:
  std::vector<std::string> servers_;
};

void PrintClientUsage(const char* prog) {
  std::cout << "Usage: " << prog << " <server1_addr> [server2_addr ...]\n";
  std::cout << "Example: " << prog
            << " 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003\n";
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    PrintClientUsage();
  }

  std::vector<std::string> server_addrs;
  for (int i = 1; i < argc; ++i) {
    server_addrs.push_back(argv[i]);
  }

  std::cout << "[Client] Initialized with " << server_addrs.size()
            << " nodes.\n";

  CounterClient client(server_addrs);

  return 0;
}
