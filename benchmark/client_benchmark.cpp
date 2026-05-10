/**
 * @file client_benchmark.cpp
 * @brief Benchmark tool for Client Library performance testing
 */

#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "rollingraft/client.h"

#include "benchmark.h"

using namespace rollingraft;

// Global client for benchmark operations
std::unique_ptr<Client> g_client;

// Generate payload of specified size
std::string GeneratePayload(size_t size) {
  std::string payload;
  payload.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    payload.push_back('a' + (i % 26));
  }
  return payload;
}

// Operation wrapper for write benchmark
OperationResult DoWriteOperation() {
  static thread_local std::string payload = GeneratePayload(100);

  OperationResult result;
  auto client_result = g_client->Execute(payload);

  result.success = client_result.ok();
  if (!result.success) {
    result.error_message = client_result.error_message();
  }

  return result;
}

// Operation wrapper for read benchmark
OperationResult DoReadOperation() {
  OperationResult result;
  auto client_result = g_client->Query("get");

  result.success = client_result.ok();
  if (!result.success) {
    result.error_message = client_result.error_message();
  }

  return result;
}

void PrintUsage(const char* program) {
  std::cout
      << "Usage: " << program << " [options] <server1> <server2> ...\n"
      << "\n"
      << "Options:\n"
      << "  -t, --type <write|read|mixed>  Benchmark type (default: write)\n"
      << "  -d, --duration <seconds>       Test duration (default: 10)\n"
      << "  -c, --clients <n>              Number of concurrent clients "
         "(default: 1)\n"
      << "  -s, --size <bytes>             Payload size (default: 100)\n"
      << "  -o, --output <file>            Save results to JSON file\n"
      << "  -h, --help                     Show this help\n"
      << "\n"
      << "Examples:\n"
      << "  " << program << " 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003\n"
      << "  " << program << " -t read -d 30 -c 4 127.0.0.1:8001\n"
      << "  " << program
      << " -t mixed -s 1024 -o results.json 127.0.0.1:8001\n";
}

int main(int argc, char* argv[]) {
  // Parse arguments
  BenchmarkConfig config;
  config.duration = std::chrono::seconds(10);
  config.num_clients = 1;
  config.payload_size = 100;
  config.operation_type = BenchmarkConfig::OperationType::kWrite;
  std::string output_file;
  std::vector<std::string> servers;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    if (arg == "-h" || arg == "--help") {
      PrintUsage(argv[0]);
      return 0;
    } else if ((arg == "-t" || arg == "--type") && i + 1 < argc) {
      std::string type = argv[++i];
      if (type == "write") {
        config.operation_type = BenchmarkConfig::OperationType::kWrite;
      } else if (type == "read") {
        config.operation_type = BenchmarkConfig::OperationType::kRead;
      } else if (type == "mixed") {
        config.operation_type = BenchmarkConfig::OperationType::kMixed;
      } else {
        std::cerr << "Unknown benchmark type: " << type << std::endl;
        return 1;
      }
    } else if ((arg == "-d" || arg == "--duration") && i + 1 < argc) {
      config.duration = std::chrono::seconds(std::stoi(argv[++i]));
    } else if ((arg == "-c" || arg == "--clients") && i + 1 < argc) {
      config.num_clients = std::stoi(argv[++i]);
    } else if ((arg == "-s" || arg == "--size") && i + 1 < argc) {
      config.payload_size = std::stoi(argv[++i]);
    } else if ((arg == "-o" || arg == "--output") && i + 1 < argc) {
      output_file = argv[++i];
    } else if (arg[0] != '-') {
      servers.push_back(arg);
    } else {
      std::cerr << "Unknown option: " << arg << std::endl;
      PrintUsage(argv[0]);
      return 1;
    }
  }

  if (servers.empty()) {
    std::cerr << "Error: No server addresses provided.\n" << std::endl;
    PrintUsage(argv[0]);
    return 1;
  }

  // Print benchmark configuration
  std::cout << "========== Client Benchmark ==========" << std::endl;
  std::cout << "Servers: ";
  for (const auto& s : servers) {
    std::cout << s << " ";
  }
  std::cout << std::endl;
  std::cout << "Type: "
            << (config.operation_type == BenchmarkConfig::OperationType::kWrite
                    ? "write"
                : config.operation_type == BenchmarkConfig::OperationType::kRead
                    ? "read"
                    : "mixed")
            << std::endl;
  std::cout << "Duration: " << config.duration.count() << " seconds"
            << std::endl;
  std::cout << "Clients: " << config.num_clients << std::endl;
  std::cout << "Payload size: " << config.payload_size << " bytes" << std::endl;
  std::cout << "======================================" << std::endl;

  // Create client with optimized options for benchmarking
  ClientOptions options;
  options.max_retries = 3;
  options.request_timeout = std::chrono::milliseconds(5000);
  options.initial_retry_delay = std::chrono::milliseconds(100);

  std::cout << "\nConnecting to cluster..." << std::endl;
  g_client = std::make_unique<Client>(servers, options);

  // Check if cluster is healthy
  if (!g_client->IsHealthy()) {
    std::cout << "Warning: Could not connect to cluster. Benchmark may fail."
              << std::endl;
  } else {
    std::cout << "Connected. Leader: " << g_client->GetLeaderAddr()
              << std::endl;
  }

  // Run benchmark
  std::cout << "\nRunning benchmark..." << std::endl;

  BenchmarkStats stats;

  if (config.num_clients == 1) {
    // Single client benchmark
    std::function<OperationResult()> op;
    if (config.operation_type == BenchmarkConfig::OperationType::kRead) {
      op = DoReadOperation;
    } else {
      op = DoWriteOperation;
    }

    ThroughputBenchmark benchmark(config, op);
    stats = benchmark.Run();
  } else {
    // Multi-client benchmark - run concurrent benchmarks and aggregate
    std::cout << "Running with " << config.num_clients
              << " concurrent clients..." << std::endl;

    std::vector<std::future<BenchmarkStats>> futures;

    for (int i = 0; i < config.num_clients; ++i) {
      futures.push_back(std::async(std::launch::async, [&config]() {
        std::function<OperationResult()> op;
        if (config.operation_type == BenchmarkConfig::OperationType::kRead) {
          op = DoReadOperation;
        } else {
          op = DoWriteOperation;
        }

        ThroughputBenchmark benchmark(config, op);
        return benchmark.Run();
      }));
    }

    // Aggregate results
    std::vector<BenchmarkStats> all_stats;
    for (auto& f : futures) {
      all_stats.push_back(f.get());
    }

    // Sum up statistics
    stats.duration_ms = all_stats[0].duration_ms;
    for (const auto& s : all_stats) {
      stats.total_operations += s.total_operations;
      stats.success_count += s.success_count;
      stats.failure_count += s.failure_count;
    }
    stats.operations_per_second =
        (static_cast<double>(stats.total_operations) * 1000.0) /
        stats.duration_ms.count();
    stats.success_rate =
        (stats.total_operations > 0)
            ? static_cast<double>(stats.success_count) / stats.total_operations
            : 0.0;

    // Use max latency from all clients as conservative estimate
    for (const auto& s : all_stats) {
      stats.latency_max_us = std::max(stats.latency_max_us, s.latency_max_us);
    }
  }

  // Print results
  std::cout << "\n========== Results ==========" << std::endl;
  std::cout << stats.ToString() << std::endl;

  // Save to file if requested
  if (!output_file.empty()) {
    benchmark::SaveToJson(output_file, stats);
    std::cout << "Results saved to: " << output_file << std::endl;
  }

  return 0;
}
