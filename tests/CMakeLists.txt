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
  unit/test_log_persister.cpp
  unit/test_raft_election.cpp
  unit/test_raft_log_replication.cpp
  unit/test_read_index.cpp
  unit/test_membership_change.cpp
)

target_include_directories(unit_tests
  PRIVATE
    ${GTEST_INCLUDE_DIRS}
)

target_link_libraries(unit_tests
  PRIVATE
    test_mocks
    ${GTEST_MAIN_LIBRARIES}
)

# Integration tests
add_executable(integration_tests
  integration/test_cluster_3nodes.cpp
)

target_include_directories(integration_tests
  PRIVATE
    ${GTEST_INCLUDE_DIRS}
)

target_link_libraries(integration_tests
  PRIVATE
    test_mocks
    ${GTEST_MAIN_LIBRARIES}
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
  PROPERTIES TIMEOUT 60
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
