# Benchmark executables

# Benchmark library
add_library(rollingraft_benchmark STATIC
  benchmark.cpp
  cluster_benchmark.cpp
  scenario_registry.cpp
)

target_include_directories(rollingraft_benchmark
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests
)

target_link_libraries(rollingraft_benchmark
  PUBLIC
    rollingraft
    test_mocks
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

# Persister microbenchmark executable
add_executable(benchmark_persister
  persister_benchmark.cpp
)

target_link_libraries(benchmark_persister
  PRIVATE
    rollingraft
)

# Unified benchmark runner
add_executable(benchmark_runner
  main.cpp
  scenarios/single_leader_write.cpp
  scenarios/latency_curve.cpp
  scenarios/failover.cpp
  scenarios/snapshot_recovery.cpp
  scenarios/leader_transfer.cpp
  scenarios/concurrent_writes.cpp
)

target_link_libraries(benchmark_runner
  PRIVATE
    rollingraft_benchmark
    rollingraft
)

# Protobuf MVP benchmark (optional — requires protobuf)
if(TARGET rollingraft_proto)
  add_executable(benchmark_protobuf_mvp
    protobuf_mvp_benchmark.cpp
  )

  target_include_directories(benchmark_protobuf_mvp
    PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/..
      ${CMAKE_CURRENT_SOURCE_DIR}/../include
      ${CMAKE_CURRENT_BINARY_DIR}/..
  )

  target_link_libraries(benchmark_protobuf_mvp
    PRIVATE
      rollingraft_proto
      rollingraft::json
  )
endif()

# Install benchmark executables
install(TARGETS benchmark_client benchmark_latency_curve benchmark_failover benchmark_runner
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}/benchmark
)
