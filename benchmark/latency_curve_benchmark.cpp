/**
 * @file latency_curve_benchmark.cpp
 * @brief Measure latency at different throughput levels
 */

#include <iomanip>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "rollingraft/client.h"

#include "benchmark.h"

using namespace rollingraft;

std::unique_ptr<Client> g_client;

std::string GeneratePayload(size_t size) {
  std::string payload;
  payload.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    payload.push_back('a' + (i % 26));
  }
  return payload;
}

OperationResult DoOperation() {
  static thread_local std::string payload = GeneratePayload(100);

  OperationResult result;
  auto client_result = g_client->Execute(payload);

  result.success = client_result.ok();
  if (!result.success) {
    result.error_message = client_result.error_message();
  }

  return result;
}

void PrintUsage(const char* program) {
  std::cout << "Usage: " << program << " [options] <server1> <server2> ...\n"
            << "\n"
            << "Options:\n"
            << "  -d, --duration <seconds>  Duration per throughput level (default: "
               "5)\n"
            << "  -s, --size <bytes>        Payload size (default: 100)\n"
            << "  -h, --help                Show this help\n"
            << "\n"
            << "Measures latency (P50, P99, P999) at increasing throughput levels.\n"
            << "\n"
            << "Example:\n"
            << "  " << program << " 127.0.0.1:8001 127.0.0.1:8002\n";
}

int main(int argc, char* argv[]) {
  int duration_per_level = 5;
  size_t payload_size = 100;
  std::vector<std::string> servers;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    if (arg == "-h" || arg == "--help") {
      PrintUsage(argv[0]);
      return 0;
    } else if ((arg == "-d" || arg == "--duration") && i + 1 < argc) {
      duration_per_level = std::stoi(argv[++i]);
    } else if ((arg == "-s" || arg == "--size") && i + 1 < argc) {
      payload_size = std::stoi(argv[++i]);
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

  std::cout << "========== Latency Curve Benchmark ==========" << std::endl;
  std::cout << "Servers: ";
  for (const auto& s : servers) {
    std::cout << s << " ";
  }
  std::cout << std::endl;
  std::cout << "Duration per level: " << duration_per_level << " seconds" << std::endl;
  std::cout << "Payload size: " << payload_size << " bytes" << std::endl;
  std::cout << "==============================================" << std::endl;

  // Create client
  ClientOptions options;
  options.max_retries = 3;
  options.request_timeout = std::chrono::milliseconds(5000);

  std::cout << "\nConnecting to cluster..." << std::endl;
  g_client = std::make_unique<Client>(servers, options);

  if (!g_client->IsHealthy()) {
    std::cout << "Warning: Could not connect to cluster." << std::endl;
  } else {
    std::cout << "Connected. Leader: " << g_client->GetLeaderAddr() << std::endl;
  }

  // Throughput levels to test
  std::vector<int> throughput_levels = {100, 200, 500, 1000, 2000, 5000};

  std::cout << "\n========== Latency Curve Results ==========" << std::endl;
  std::cout << std::left << std::setw(15) << "Target (ops/s)" << std::setw(15) << "Actual (ops/s)"
            << std::setw(12) << "Success%" << std::setw(12) << "P50 (us)" << std::setw(12)
            << "P99 (us)" << std::setw(12) << "P999 (us)" << std::endl;
  std::cout << std::string(78, '-') << std::endl;

  // Test at each throughput level
  for (int target : throughput_levels) {
    BenchmarkConfig config;
    config.duration = std::chrono::seconds(duration_per_level);
    config.payload_size = payload_size;

    // Create rate-limited operation
    auto delay_between_ops = std::chrono::microseconds(1000000 / target);
    auto last_op_time = std::chrono::steady_clock::now();

    auto rate_limited_op = [delay_between_ops, &last_op_time]() -> OperationResult {
      auto now = std::chrono::steady_clock::now();
      auto elapsed = now - last_op_time;
      if (elapsed < delay_between_ops) {
        std::this_thread::sleep_for(delay_between_ops - elapsed);
      }
      last_op_time = std::chrono::steady_clock::now();
      return DoOperation();
    };

    ThroughputBenchmark benchmark(config, rate_limited_op);
    auto stats = benchmark.Run();

    std::cout << std::left << std::setw(15) << target << std::setw(15)
              << static_cast<int>(stats.operations_per_second) << std::setw(12) << std::fixed
              << std::setprecision(1) << (stats.success_rate * 100) << std::setw(12)
              << static_cast<int>(stats.latency_p50_us) << std::setw(12)
              << static_cast<int>(stats.latency_p99_us) << std::setw(12)
              << static_cast<int>(stats.latency_p999_us) << std::endl;
  }

  std::cout << "\nLatency curve benchmark complete." << std::endl;

  return 0;
}
