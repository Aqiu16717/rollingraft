/**
 * @file client_example.cpp
 * @brief Example usage of RollingRaft Client Library
 */

#include <iostream>
#include <thread>

#include "rollingraft/client.h"

using namespace rollingraft;

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <server1> [server2] [server3]..." << std::endl;
    std::cerr << "Example: " << argv[0] << " 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003"
              << std::endl;
    return 1;
  }

  // Collect server addresses
  std::vector<std::string> servers;
  for (int i = 1; i < argc; ++i) {
    servers.push_back(argv[i]);
  }

  std::cout << "Creating client with servers:" << std::endl;
  for (const auto& s : servers) {
    std::cout << "  - " << s << std::endl;
  }

  // Create client
  Client client(servers);
  std::cout << "Client created, ID: " << client.GetClientId() << std::endl;

  // Interactive loop
  std::cout << "\nCommands:" << std::endl;
  std::cout << "  set <key> <value>  - Set a key-value pair" << std::endl;
  std::cout << "  get <key>          - Get value for key" << std::endl;
  std::cout << "  inc                - Increment counter" << std::endl;
  std::cout << "  dec                - Decrement counter" << std::endl;
  std::cout << "  leader             - Show current leader" << std::endl;
  std::cout << "  health             - Check client health" << std::endl;
  std::cout << "  quit               - Exit" << std::endl;
  std::cout << std::endl;

  std::string line;
  while (true) {
    std::cout << "> " << std::flush;
    if (!std::getline(std::cin, line)) {
      break;
    }

    if (line.empty()) continue;

    // Parse command
    std::vector<std::string> parts;
    size_t pos = 0;
    while (pos < line.size()) {
      size_t next = line.find(' ', pos);
      if (next == std::string::npos) {
        parts.push_back(line.substr(pos));
        break;
      }
      parts.push_back(line.substr(pos, next - pos));
      pos = next + 1;
      while (pos < line.size() && line[pos] == ' ') ++pos;
    }

    if (parts.empty()) continue;

    const std::string& cmd = parts[0];

    if (cmd == "quit" || cmd == "exit") {
      break;
    } else if (cmd == "leader") {
      std::string leader = client.GetLeaderAddr();
      if (leader.empty()) {
        std::cout << "Leader: unknown (will discover on next request)" << std::endl;
      } else {
        std::cout << "Leader: " << leader << std::endl;
      }
    } else if (cmd == "health") {
      bool healthy = client.IsHealthy();
      std::cout << "Health: " << (healthy ? "healthy" : "unhealthy") << std::endl;
    } else if (cmd == "set") {
      if (parts.size() < 3) {
        std::cout << "Usage: set <key> <value>" << std::endl;
        continue;
      }
      std::string key = parts[1];
      std::string value = parts[2];
      std::string command = "set " + key + " " + value;

      std::cout << "Executing: " << command << std::endl;
      auto result = client.Execute(command);

      if (result.ok()) {
        std::cout << "Success: " << result.value() << std::endl;
      } else {
        std::cout << "Error: " << result.error_message() << std::endl;
      }
    } else if (cmd == "get") {
      if (parts.size() < 2) {
        std::cout << "Usage: get <key>" << std::endl;
        continue;
      }
      std::string key = parts[1];
      std::string command = "get " + key;

      std::cout << "Querying: " << key << std::endl;
      auto result = client.Query(command);

      if (result.ok()) {
        std::cout << "Value: " << result.value() << std::endl;
      } else {
        std::cout << "Error: " << result.error_message() << std::endl;
      }
    } else if (cmd == "inc") {
      auto result = client.Execute("inc");
      if (result.ok()) {
        std::cout << "Counter: " << result.value() << std::endl;
      } else {
        std::cout << "Error: " << result.error_message() << std::endl;
      }
    } else if (cmd == "dec") {
      auto result = client.Execute("dec");
      if (result.ok()) {
        std::cout << "Counter: " << result.value() << std::endl;
      } else {
        std::cout << "Error: " << result.error_message() << std::endl;
      }
    } else if (cmd == "async") {
      // Demonstrate async execution
      std::cout << "Sending async request..." << std::endl;
      client.ExecuteAsync("inc", [](ClientResult result) {
        if (result.ok()) {
          std::cout << "\nAsync result: " << result.value() << std::endl;
        } else {
          std::cout << "\nAsync error: " << result.error_message() << std::endl;
        }
        std::cout << "> " << std::flush;
      });
      std::cout << "Request sent (non-blocking)" << std::endl;
    } else {
      // Try to execute as raw command
      auto result = client.Execute(line);
      if (result.ok()) {
        std::cout << "Result: " << result.value() << std::endl;
      } else {
        std::cout << "Error: " << result.error_message() << std::endl;
      }
    }
  }

  std::cout << "Goodbye!" << std::endl;
  return 0;
}
