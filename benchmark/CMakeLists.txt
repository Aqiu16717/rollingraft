# Benchmark executables

# Benchmark library
add_library(rollingraft_benchmark STATIC
  benchmark.cpp
)

target_include_directories(rollingraft_benchmark
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(rollingraft_benchmark
  PUBLIC
    rollingraft
)

# Client benchmark executable
add_executable(benchmark_client
  client_benchmark.cpp
)

target_link_libraries(benchmark_client
  PRIVATE
    rollingraft_benchmark
    rollingraft
)

# Latency curve benchmark executable
add_executable(benchmark_latency_curve
  latency_curve_benchmark.cpp
)

target_link_libraries(benchmark_latency_curve
  PRIVATE
    rollingraft_benchmark
    rollingraft
)

# Failover benchmark executable
add_executable(benchmark_failover
  failover_benchmark.cpp
)

target_link_libraries(benchmark_failover
  PRIVATE
    rollingraft
)

# Install benchmark executables
install(TARGETS benchmark_client benchmark_latency_curve benchmark_failover
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}/benchmark
)
