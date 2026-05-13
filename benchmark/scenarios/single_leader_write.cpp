/**
 * @file single_leader_write.cpp
 * @brief Scenario A: Single-leader write throughput benchmark
 */

#include <chrono>
#include <string>

#include "../benchmark.h"
#include "../cluster_benchmark.h"
#include "../scenario_registry.h"

namespace rollingraft {

class SingleLeaderWriteScenario : public ClusterBenchmark {
 public:
  SingleLeaderWriteScenario()
      : ClusterBenchmark(
            []() {
              BenchmarkConfig config;
              config.duration = std::chrono::seconds(30);
              config.num_clients = 1;
              config.payload_size = 100;
              return config;
            }(),
            []() {
              ClusterConfig config;
              config.num_nodes = 3;
              config.election_timeout = std::chrono::milliseconds(300);
              config.heartbeat_interval = std::chrono::milliseconds(50);
              return config;
            }()) {}

  std::string Name() const override { return "single_leader_write"; }

 protected:
  OperationResult DoOperation() override {
    static thread_local std::string payload(100, 'x');

    OperationResult result;
    auto status = ExecuteCommand(payload);
    result.success = status.ok();
    if (!result.success) {
      result.error_message = status.ToString();
    }
    return result;
  }
};

REGISTER_SCENARIO(single_leader_write, []() {
  return std::make_unique<SingleLeaderWriteScenario>();
});

}  // namespace rollingraft
