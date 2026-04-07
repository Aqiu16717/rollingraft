# Counter example
add_executable(example_counter_server counter/counter_server.cpp)
target_link_libraries(example_counter_server PRIVATE rollingraft)

add_executable(example_counter_client counter/counter_client.cpp)
target_link_libraries(example_counter_client PRIVATE rollingraft)

# Client library example
add_executable(example_client client_example.cpp)
target_link_libraries(example_client PRIVATE rollingraft)
