cmake_minimum_required(VERSION 3.10)
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

add_library(asio INTERFACE)
target_include_directories(asio INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/third_party/asio/asio/include)
target_compile_definitions(asio INTERFACE ASIO_STANDALONE)

#add_subdirectory("third_party/asio")
target_link_libraries(rollingraft PRIVATE asio)
# use non-boost-asio
# target_compile_definitions(rollingraft PRIVATE ASIO_STANDALONE)

# This project is tested using GoogleTest
# add_subdirectory("third_party/googletest")
enable_testing()
add_subdirectory(tests)

add_subdirectory(example)