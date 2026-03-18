#include <iostream>
#include <memory>
#include <rollingraft/raft_node.h>

class CounterMachine : Public StateMachine {};

int main() {
  rollingraft::RaftNodeConfig config;
  config.node_id = 1;
  config.listen_addr_ = "127.0.0.1:9527";
  config.listen_addr_ = {"127.0.0.1:9528", "127.0.0.1:9529"};
  config.data_dir_ = "./data/node1";

  auto sm = std::make_unique<CounterMachine>();

  return 0;
}
