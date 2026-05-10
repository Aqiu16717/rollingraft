/**
 * @file failover_benchmark.cpp
 * @brief Measure cluster recovery time after leader failure
 */

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "rollingraft/client.h"

using namespace rollingraft;

struct FailoverResult {
  std::chrono::milliseconds detection_time{0};
  std::chrono::milliseconds election_time{0};
  std::chrono::milliseconds recovery_time{0};
  uint64_t operations_during_failover = 0;
  uint64_t operations_failed = 0;
  double availability_during_failover = 0.0;
};

void PrintUsage(const char* program) {
  std::cout
      << "Usage: " << program << " [options] <server1> <server2> ...\n"
      << "\n"
      << "Options:\n"
      << "  -k, --kill <command>     Shell command to kill leader (required)\n"
      << "  -c, --check <command>    Shell command to check if new leader "
         "elected\n"
      << "  -r, --rate <ops/sec>     Operation rate during test (default: "
         "100)\n"
      << "  -w, --warmup <seconds>   Warmup duration (default: 5)\n"
      << "  -h, --help               Show this help\n"
      << "\n"
      << "Measures cluster recovery time after leader failure.\n"
      << "\n"
      << "Example:\n"
      << "  " << program << " -k 'pkill -f counter_server.*:8001' \\\n"
      << "           -c 'curl -s http://localhost:8002/status' \\\n"
      << "           127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003\n";
}

// Execute shell command and return exit code
int ExecuteCommand(const std::string& cmd) { return std::system(cmd.c_str()); }

FailoverResult RunFailoverBenchmark(Client* client,
                                    const std::string& kill_command,
                                    const std::string& check_command,
                                    int ops_per_second, int warmup_seconds) {
  FailoverResult result;

  std::cout << "\n=== Warmup Phase (" << warmup_seconds
            << " seconds) ===" << std::endl;
  auto warmup_start = std::chrono::steady_clock::now();
  uint64_t warmup_ops = 0;

  while (std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::steady_clock::now() - warmup_start)
             .count() < warmup_seconds) {
    auto r = client->Execute("inc");
    if (r.ok()) {
      ++warmup_ops;
    }
    std::this_thread::sleep_for(
        std::chrono::milliseconds(1000 / ops_per_second));
  }

  std::cout << "Warmup complete: " << warmup_ops << " operations" << std::endl;
  std::cout << "Current leader: " << client->GetLeaderAddr() << std::endl;

  // Start steady load
  std::cout << "\n=== Steady Load Phase (5 seconds) ===" << std::endl;
  std::atomic<bool> stop_load{false};
  std::atomic<uint64_t> steady_success{0};
  std::atomic<uint64_t> steady_failed{0};

  std::thread load_thread([&]() {
    while (!stop_load) {
      auto r = client->Execute("inc");
      if (r.ok()) {
        ++steady_success;
      } else {
        ++steady_failed;
      }
      std::this_thread::sleep_for(
          std::chrono::milliseconds(1000 / ops_per_second));
    }
  });

  // Let steady load run for a bit
  std::this_thread::sleep_for(std::chrono::seconds(5));

  // Kill the leader
  std::cout << "\n=== FAILOVER: Killing leader ===" << std::endl;
  auto kill_time = std::chrono::steady_clock::now();

  int kill_result = ExecuteCommand(kill_command);
  if (kill_result != 0) {
    std::cerr << "Warning: Kill command failed with exit code " << kill_result
              << std::endl;
  }

  // Measure detection and recovery
  auto detection_time = kill_time;
  bool detected = false;
  bool recovered = false;

  uint64_t failover_ops = 0;
  uint64_t failover_failed = 0;

  auto max_wait = std::chrono::seconds(30);
  auto check_interval = std::chrono::milliseconds(100);

  std::cout << "Waiting for failover..." << std::flush;

  while (std::chrono::steady_clock::now() - kill_time < max_wait) {
    // Check if recovered
    if (!check_command.empty() && ExecuteCommand(check_command) == 0) {
      if (!recovered) {
        auto recovery_time = std::chrono::steady_clock::now();

        if (!detected) {
          // Direct recovery without detection phase
          detection_time = kill_time;
          detected = true;
        }

        result.detection_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                detection_time - kill_time);
        result.election_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                recovery_time - detection_time);
        result.recovery_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                recovery_time - kill_time);

        recovered = true;
        break;
      }
    }

    // Try an operation to detect failure
    auto r = client->Execute("inc");
    ++failover_ops;

    if (!r.ok()) {
      ++failover_failed;
      if (!detected) {
        detection_time = std::chrono::steady_clock::now();
        detected = true;
        std::cout << "\nFailure detected after "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(
                         detection_time - kill_time)
                         .count()
                  << " ms" << std::endl;
      }
    } else if (detected && !recovered) {
      // Success after failure = recovery
      auto recovery_time = std::chrono::steady_clock::now();

      result.detection_time =
          std::chrono::duration_cast<std::chrono::milliseconds>(detection_time -
                                                                kill_time);
      result.election_time =
          std::chrono::duration_cast<std::chrono::milliseconds>(recovery_time -
                                                                detection_time);
      result.recovery_time =
          std::chrono::duration_cast<std::chrono::milliseconds>(recovery_time -
                                                                kill_time);

      recovered = true;
      break;
    }

    std::this_thread::sleep_for(check_interval);
    std::cout << "." << std::flush;
  }

  stop_load = true;
  load_thread.join();

  if (!recovered) {
    std::cout << "\nFAILOVER TIMEOUT!" << std::endl;
    result.recovery_time = max_wait;
  }

  result.operations_during_failover = failover_ops;
  result.operations_failed = failover_failed;
  result.availability_during_failover =
      (failover_ops > 0)
          ? static_cast<double>(failover_ops - failover_failed) / failover_ops
          : 0.0;

  return result;
}

int main(int argc, char* argv[]) {
  std::string kill_command;
  std::string check_command;
  int ops_per_second = 100;
  int warmup_seconds = 5;
  std::vector<std::string> servers;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    if (arg == "-h" || arg == "--help") {
      PrintUsage(argv[0]);
      return 0;
    } else if ((arg == "-k" || arg == "--kill") && i + 1 < argc) {
      kill_command = argv[++i];
    } else if ((arg == "-c" || arg == "--check") && i + 1 < argc) {
      check_command = argv[++i];
    } else if ((arg == "-r" || arg == "--rate") && i + 1 < argc) {
      ops_per_second = std::stoi(argv[++i]);
    } else if ((arg == "-w" || arg == "--warmup") && i + 1 < argc) {
      warmup_seconds = std::stoi(argv[++i]);
    } else if (arg[0] != '-') {
      servers.push_back(arg);
    } else {
      std::cerr << "Unknown option: " << arg << std::endl;
      return 1;
    }
  }

  if (servers.empty()) {
    std::cerr << "Error: No server addresses provided." << std::endl;
    PrintUsage(argv[0]);
    return 1;
  }

  if (kill_command.empty()) {
    std::cerr << "Error: Kill command required (-k option)." << std::endl;
    PrintUsage(argv[0]);
    return 1;
  }

  std::cout << "========== Failover Benchmark ==========" << std::endl;
  std::cout << "Servers: ";
  for (const auto& s : servers) {
    std::cout << s << " ";
  }
  std::cout << std::endl;
  std::cout << "Operation rate: " << ops_per_second << " ops/sec" << std::endl;
  std::cout << "Kill command: " << kill_command << std::endl;
  if (!check_command.empty()) {
    std::cout << "Check command: " << check_command << std::endl;
  }
  std::cout << "========================================" << std::endl;

  // Create client
  ClientOptions options;
  options.max_retries = 3;
  options.request_timeout = std::chrono::milliseconds(2000);
  options.initial_retry_delay = std::chrono::milliseconds(50);

  std::cout << "\nConnecting to cluster..." << std::endl;
  Client client(servers, options);

  if (!client.IsHealthy()) {
    std::cerr << "Error: Could not connect to cluster." << std::endl;
    return 1;
  }

  std::cout << "Connected. Leader: " << client.GetLeaderAddr() << std::endl;

  // Run failover benchmark
  auto result = RunFailoverBenchmark(&client, kill_command, check_command,
                                     ops_per_second, warmup_seconds);

  // Print results
  std::cout << "\n========== Failover Results ==========" << std::endl;
  std::cout << std::left << std::setw(30) << "Metric" << "Value" << std::endl;
  std::cout << std::string(50, '-') << std::endl;
  std::cout << std::left << std::setw(30)
            << "Failure Detection Time:" << result.detection_time.count()
            << " ms" << std::endl;
  std::cout << std::left << std::setw(30)
            << "Leader Election Time:" << result.election_time.count() << " ms"
            << std::endl;
  std::cout << std::left << std::setw(30)
            << "Total Recovery Time:" << result.recovery_time.count() << " ms"
            << std::endl;
  std::cout << std::left << std::setw(30) << "Operations During Failover:"
            << result.operations_during_failover << std::endl;
  std::cout << std::left << std::setw(30)
            << "Failed Operations:" << result.operations_failed << std::endl;
  std::cout << std::left << std::setw(30)
            << "Availability During Failover:" << std::fixed
            << std::setprecision(1)
            << (result.availability_during_failover * 100.0) << "%"
            << std::endl;

  return 0;
}
