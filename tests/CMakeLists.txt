enable_testing()

# Find Google Test
find_package(GTest QUIET)
if(NOT GTest_FOUND)
  # Use bundled googletest
  add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/googletest ${CMAKE_BINARY_DIR}/googletest EXCLUDE_FROM_ALL)
  set(GTEST_LIBRARIES gtest)
  set(GTEST_MAIN_LIBRARIES gtest_main)
  set(GTEST_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include)
endif()

# Mock library
add_library(test_mocks STATIC
  mock/mock_timer.cpp
  mock/mock_network.cpp
  mock/mock_persister.cpp
  mock/mock_state_machine.cpp
)

target_include_directories(test_mocks
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(test_mocks
  PUBLIC
    rollingraft
    ${GTEST_LIBRARIES}
)

# Unit tests
add_executable(unit_tests
  unit/test_asio_ssl_context_factory.cpp
  unit/test_log_persister.cpp
  unit/test_leveldb_persister.cpp
  unit/test_raft_election.cpp
  unit/test_raft_log_replication.cpp
  unit/test_read_index.cpp
  unit/test_membership_change.cpp
  unit/test_snapshot_transfer.cpp
  unit/test_network_partition.cpp
  unit/test_leader_stepdown.cpp
  unit/test_membership_edge_cases.cpp
  unit/test_client_result.cpp
  unit/test_leader_tracker.cpp
  unit/test_retry_policy.cpp
  unit/test_connection_pool.cpp
  unit/test_client.cpp
  unit/test_metrics.cpp
  unit/test_log_compaction.cpp
  unit/test_event_bus.cpp
  unit/test_metrics_http_server.cpp
  unit/test_raft_node_config.cpp
)

target_include_directories(unit_tests
  PRIVATE
    ${GTEST_INCLUDE_DIRS}
    ${PROJECT_SOURCE_DIR}/src
)
if(OpenSSL_FOUND)
  target_include_directories(unit_tests PRIVATE ${OPENSSL_INCLUDE_DIR})
endif()

target_link_libraries(unit_tests
  PRIVATE
    test_mocks
    ${GTEST_MAIN_LIBRARIES}
)

# Deterministic test infrastructure
add_library(deterministic STATIC
  deterministic/simulated_clock.cpp
  deterministic/simulated_network.cpp
  deterministic/simulated_network_transport.cpp
  deterministic/simulated_timer_service.cpp
  deterministic/test_cluster.cpp
)

target_include_directories(deterministic
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(deterministic
  PUBLIC
    rollingraft
    test_mocks
)

# Integration tests
add_executable(integration_tests
  integration/test_cluster_3nodes.cpp
  integration/test_metrics_endpoint.cpp
)

target_include_directories(integration_tests
  PRIVATE
    ${GTEST_INCLUDE_DIRS}
)

target_link_libraries(integration_tests
  PRIVATE
    test_mocks
    deterministic
    ${GTEST_MAIN_LIBRARIES}
)

# Deterministic tests
add_executable(deterministic_tests
  deterministic/test_chaos.cpp
  deterministic/test_chaos_scenarios.cpp
)

target_include_directories(deterministic_tests
  PRIVATE
    ${GTEST_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(deterministic_tests
  PRIVATE
    deterministic
    ${GTEST_MAIN_LIBRARIES}
)

# Install test binaries
install(TARGETS unit_tests integration_tests deterministic_tests
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Register tests
include(GoogleTest)
gtest_discover_tests(unit_tests
  TEST_PREFIX "unit."
  XML_OUTPUT_DIR ${CMAKE_BINARY_DIR}/test_results
)

gtest_discover_tests(integration_tests
  TEST_PREFIX "integration."
  XML_OUTPUT_DIR ${CMAKE_BINARY_DIR}/test_results
  PROPERTIES TIMEOUT 120 RUN_SERIAL TRUE
)

gtest_discover_tests(deterministic_tests
  TEST_PREFIX "deterministic."
  XML_OUTPUT_DIR ${CMAKE_BINARY_DIR}/test_results
  PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE
)

# Add test targets
add_custom_target(run_unit_tests
  COMMAND ${CMAKE_CTEST_COMMAND} -R "unit\\." --output-on-failure
  DEPENDS unit_tests
)

add_custom_target(run_integration_tests
  COMMAND ${CMAKE_CTEST_COMMAND} -R "integration\\." --output-on-failure
  DEPENDS integration_tests
)

add_custom_target(run_deterministic_tests
  COMMAND ${CMAKE_CTEST_COMMAND} -R "deterministic\\." --output-on-failure
  DEPENDS deterministic_tests
)
