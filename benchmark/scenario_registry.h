/**
 * @file scenario_registry.h
 * @brief Scenario registry for unified benchmark runner
 */

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "benchmark.h"

namespace rollingraft {

using ScenarioFactory = std::function<std::unique_ptr<Benchmark>()>;

/**
 * Registry for benchmark scenarios.
 *
 * Scenarios register themselves at startup using REGISTER_SCENARIO macro.
 */
class ScenarioRegistry {
 public:
  static ScenarioRegistry& Instance();

  void Register(const std::string& name, ScenarioFactory factory);
  std::unique_ptr<Benchmark> Create(const std::string& name) const;
  std::vector<std::string> ListScenarios() const;
  bool HasScenario(const std::string& name) const;

 private:
  ScenarioRegistry() = default;
  std::map<std::string, ScenarioFactory> factories_;
};

// Macro for scenario self-registration
#define REGISTER_SCENARIO(name, factory)                     \
  struct ScenarioRegistrar_##name {                          \
    ScenarioRegistrar_##name() {                             \
      ScenarioRegistry::Instance().Register(#name, factory); \
    }                                                        \
  } scenario_registrar_##name;

}  // namespace rollingraft
