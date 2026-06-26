/**
 * @file scenario_registry.cpp
 * @brief Scenario registry implementation
 */

#include "scenario_registry.h"

namespace rollingraft {

ScenarioRegistry& ScenarioRegistry::Instance() {
  static ScenarioRegistry instance;
  return instance;
}

void ScenarioRegistry::Register(const std::string& name, ScenarioFactory factory) {
  factories_[name] = std::move(factory);
}

std::unique_ptr<Benchmark> ScenarioRegistry::Create(const std::string& name) const {
  auto it = factories_.find(name);
  if (it != factories_.end()) {
    return it->second();
  }
  return nullptr;
}

std::vector<std::string> ScenarioRegistry::ListScenarios() const {
  std::vector<std::string> names;
  for (const auto& [name, _] : factories_) {
    names.push_back(name);
  }
  return names;
}

bool ScenarioRegistry::HasScenario(const std::string& name) const {
  return factories_.find(name) != factories_.end();
}

}  // namespace rollingraft
