cmake_minimum_required(VERSION 3.14)
project(rollingraft
  VERSION 0.1.0
  LANGUAGES C CXX
  DESCRIPTION "A C++20 Raft consensus library based on Asio"
)

# C++ Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build options
option(BUILD_SHARED_LIBS "Build shared library" OFF)
option(BUILD_TESTING "Build tests" ON)
option(BUILD_EXAMPLES "Build examples" ON)

# Compiler warnings
if(MSVC)
  add_compile_options(/W4)
else()
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Main library
add_library(rollingraft)

# Collect source files automatically
file(GLOB_RECURSE ROLLINGRAFT_SOURCES
  CONFIGURE_DEPENDS
  "src/*.cpp"
)
target_sources(rollingraft PRIVATE ${ROLLINGRAFT_SOURCES})

# Include directories
target_include_directories(rollingraft
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

# Third-party dependencies

# Asio (header-only)
add_library(rollingraft_asio INTERFACE)
target_include_directories(rollingraft_asio INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/nonboost_asio/asio/include
)
target_compile_definitions(rollingraft_asio INTERFACE ASIO_STANDALONE)
add_library(rollingraft::asio ALIAS rollingraft_asio)

# nlohmann_json (header-only)
add_library(rollingraft_json INTERFACE)
target_include_directories(rollingraft_json INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/nlohmann_json/single_include
)
add_library(rollingraft::json ALIAS rollingraft_json)

# spdlog (header-only)
add_library(rollingraft_spdlog INTERFACE)
target_include_directories(rollingraft_spdlog INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/spdlog/include
)
add_library(rollingraft::spdlog ALIAS rollingraft_spdlog)

# LevelDB (system library)
find_package(leveldb REQUIRED)

# Link dependencies
target_link_libraries(rollingraft
  PUBLIC
    rollingraft::asio
    rollingraft::json
  PRIVATE
    rollingraft::spdlog
    leveldb
)

# Testing
if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(tests)
endif()

# Examples
if(BUILD_EXAMPLES)
  add_subdirectory(example)
endif()

# Installation (simplified - just library and headers)
include(GNUInstallDirs)

install(TARGETS rollingraft
  EXPORT rollingraft-targets
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY include/rollingraft
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
