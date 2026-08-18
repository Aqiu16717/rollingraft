# Counter example
add_executable(example_counter_server counter/counter_server.cpp)
target_link_libraries(example_counter_server PRIVATE rollingraft)

add_executable(example_counter_client counter/counter_client.cpp)
target_link_libraries(example_counter_client PRIVATE rollingraft)

# Client library example
add_executable(example_client client_example.cpp)
target_link_libraries(example_client PRIVATE rollingraft)

# Multi-raft example
add_executable(example_multi_raft_server multi_raft/multi_raft_server.cpp)
target_link_libraries(example_multi_raft_server PRIVATE rollingraft)
target_include_directories(example_multi_raft_server PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_executable(example_multi_raft_client multi_raft/multi_raft_client.cpp)
target_link_libraries(example_multi_raft_client PRIVATE rollingraft)

# Multi-raft single-binary 3-node demo
add_executable(example_multi_raft_3node multi_raft/multi_raft_3node.cpp)
target_link_libraries(example_multi_raft_3node PRIVATE rollingraft)
target_include_directories(example_multi_raft_3node PRIVATE ${PROJECT_SOURCE_DIR}/src)

# Install examples
install(TARGETS example_counter_server example_counter_client example_client
                example_multi_raft_server example_multi_raft_client
                example_multi_raft_3node
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
