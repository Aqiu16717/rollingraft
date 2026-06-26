/**
 * @file main.cpp
 * @brief Unified benchmark runner
 *
 * Usage:
 *   ./benchmark_runner --all --output-dir=benchmark/results
 *   ./benchmark_runner --scenario=single_leader_write --output=results.json
 *   ./benchmark_runner --compare
 * --baseline=baselines/main/single_leader_write.json --current=results.json
 *   ./benchmark_runner --list
 */

#include <filesystem>
#include <iostream>
#include <map>
#include <string>

#include "scenario_registry.h"

using namespace rollingraft;

void PrintUsage(const char* program) {
  std::cout << "Usage: " << program << " [options]\n"
            << "\n"
            << "Options:\n"
            << "  --all                    Run all scenarios\n"
            << "  --scenario <name>        Run specific scenario\n"
            << "  --output-dir <dir>       Output directory for results (default: "
               "benchmark/results)\n"
            << "  --output <file>          Output file for single scenario\n"
            << "  --compare                Compare current results against baseline\n"
            << "  --baseline <file>        Baseline JSON file for comparison\n"
            << "  --current <file>         Current JSON file for comparison\n"
            << "  --threshold <ratio>      Regression threshold (default: 0.05 = "
               "5%)\n"
            << "  --repetitions <n>        Number of repetitions (default: 3)\n"
            << "  --list                   List available scenarios\n"
            << "  --help                   Show this help\n"
            << "\n"
            << "Examples:\n"
            << "  " << program << " --all --output-dir=benchmark/results\n"
            << "  " << program << " --scenario=single_leader_write --output=results.json\n"
            << "  " << program
            << " --compare --baseline=baselines/main/single_leader_write.json "
               "--current=results.json\n";
}

void ListScenarios() {
  auto scenarios = ScenarioRegistry::Instance().ListScenarios();
  std::cout << "Available scenarios (" << scenarios.size() << "):\n";
  for (const auto& name : scenarios) {
    std::cout << "  - " << name << "\n";
  }
}

struct RunConfig {
  bool run_all = false;
  std::string scenario_name;
  std::string output_dir = "benchmark/results";
  std::string output_file;
  bool compare = false;
  std::string baseline_file;
  std::string current_file;
  double threshold = 0.05;
  int repetitions = 3;
};

RunConfig ParseArgs(int argc, char* argv[]) {
  RunConfig config;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      std::exit(0);
    } else if (arg == "--all") {
      config.run_all = true;
    } else if (arg == "--list") {
      ListScenarios();
      std::exit(0);
    } else if (arg == "--scenario" && i + 1 < argc) {
      config.scenario_name = argv[++i];
    } else if (arg == "--output-dir" && i + 1 < argc) {
      config.output_dir = argv[++i];
    } else if (arg == "--output" && i + 1 < argc) {
      config.output_file = argv[++i];
    } else if (arg == "--compare") {
      config.compare = true;
    } else if (arg == "--baseline" && i + 1 < argc) {
      config.baseline_file = argv[++i];
    } else if (arg == "--current" && i + 1 < argc) {
      config.current_file = argv[++i];
    } else if (arg == "--threshold" && i + 1 < argc) {
      config.threshold = std::stod(argv[++i]);
    } else if (arg == "--repetitions" && i + 1 < argc) {
      config.repetitions = std::stoi(argv[++i]);
    }
  }

  return config;
}

int RunScenario(const std::string& name, const RunConfig& config) {
  auto& registry = ScenarioRegistry::Instance();

  if (!registry.HasScenario(name)) {
    std::cerr << "Unknown scenario: " << name << "\n";
    return 1;
  }

  std::cout << "\n========== Running: " << name << " ==========\n" << std::endl;

  auto benchmark = registry.Create(name);
  if (!benchmark) {
    std::cerr << "Failed to create scenario: " << name << "\n";
    return 1;
  }

  auto stats = RunRepeated(benchmark.get(), config.repetitions);
  std::cout << stats.ToString() << std::endl;

  // Save results
  std::string output_path;
  if (!config.output_file.empty()) {
    output_path = config.output_file;
  } else {
    std::filesystem::create_directories(config.output_dir);
    output_path = config.output_dir + "/" + name + ".json";
  }

  std::map<std::string, std::string> parameters;
  parameters["repetitions"] = std::to_string(config.repetitions);
  parameters["scenario"] = "\"" + name + "\"";

  stats.SaveToJson(output_path, name, parameters);
  std::cout << "Results saved to: " << output_path << std::endl;

  return 0;
}

int RunAll(const RunConfig& config) {
  auto scenarios = ScenarioRegistry::Instance().ListScenarios();
  if (scenarios.empty()) {
    std::cerr << "No scenarios registered!\n";
    return 1;
  }

  int total_exit = 0;
  for (const auto& name : scenarios) {
    total_exit |= RunScenario(name, config);
  }

  return total_exit;
}

int CompareResults(const RunConfig& config) {
  (void)config;
  std::cerr << "Comparison not yet implemented. Use manual diff for now.\n";
  return 0;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    PrintUsage(argv[0]);
    return 1;
  }

  auto config = ParseArgs(argc, argv);

  if (config.compare) {
    return CompareResults(config);
  }

  if (config.run_all) {
    return RunAll(config);
  }

  if (!config.scenario_name.empty()) {
    return RunScenario(config.scenario_name, config);
  }

  std::cerr << "Error: No action specified. Use --all, --scenario, or --compare.\n";
  PrintUsage(argv[0]);
  return 1;
}
