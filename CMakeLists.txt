cmake_minimum_required(VERSION 3.11)
project(rollingraft LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(rollingraft "")
target_include_directories(rollingraft
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)
target_sources(rollingraft
    PRIVATE
    "src/server.cpp"
    "src/status.cpp"
    "src/raft_node.cpp"
)

# add third_party non-boost-asio
add_library(nonboost_asio INTERFACE)
target_include_directories(asio
  INTERFACE 
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/nonboost_asio/asio/include
)
target_compile_definitions(nonboost_asio
  INTERFACE 
    ASIO_STANDALONE
)
target_link_libraries(rollingraft PRIVATE nonboost_asio)


# add third_party nlohmann_json
add_library(nlohmann_json INTERFACE)
target_include_directories(nlohmann_json
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/nlohmann_json/single_include
)
target_link_libraries(rollingraft
  PRIVATE
    nlohmann_json
)


# This project is tested using GoogleTest
# add_subdirectory("third_party/googletest")
enable_testing()
add_subdirectory(tests)

add_subdirectory(example)