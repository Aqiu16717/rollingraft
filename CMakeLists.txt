cmake_minimum_required(VERSION 3.14)
project(rollingraft
  VERSION 0.3.0
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
option(BUILD_BENCHMARK "Build benchmarks" ON)

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

# LevelDB (bundled via submodule)
# Apply macOS/C++20 compatibility patch if not already applied
set(LEVELDB_PATCH_FILE ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/leveldb-macos-cxx20.patch)
set(LEVELDB_PATCH_MARKER ${CMAKE_CURRENT_SOURCE_DIR}/third_party/leveldb/.patched)
if(EXISTS ${LEVELDB_PATCH_FILE} AND NOT EXISTS ${LEVELDB_PATCH_MARKER})
  message(STATUS "Applying LevelDB macOS/C++20 compatibility patch...")
  execute_process(
    COMMAND git -C ${CMAKE_CURRENT_SOURCE_DIR}/third_party/leveldb apply ${LEVELDB_PATCH_FILE}
    RESULT_VARIABLE PATCH_RESULT
  )
  if(PATCH_RESULT EQUAL 0)
    file(WRITE ${LEVELDB_PATCH_MARKER} "patched")
    message(STATUS "LevelDB patch applied successfully")
  else()
    message(WARNING "Failed to apply LevelDB patch, build may fail on macOS")
  endif()
endif()

set(LEVELDB_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(LEVELDB_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(LEVELDB_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/leveldb EXCLUDE_FROM_ALL)

# Create alias for consistency
if(NOT TARGET leveldb::leveldb)
  if(TARGET leveldb)
    add_library(leveldb::leveldb ALIAS leveldb)
  endif()
endif()

# Disable warnings for LevelDB (third-party)
if(TARGET leveldb)
  target_compile_options(leveldb PRIVATE
    -Wno-unused-parameter
    -Wno-deprecated-declarations
    -Wno-unused-but-set-variable
    -Wno-implicit-fallthrough
  )
endif()

# OpenSSL
find_package(OpenSSL REQUIRED)

# Protobuf (optional — for MVP benchmark)
# Abseil may be needed by protobuf 35+
find_package(absl CONFIG QUIET)
find_package(Protobuf QUIET)
if(Protobuf_FOUND)
  message(STATUS "Protobuf found: ${Protobuf_VERSION}")
  
  # Generate C++ code from .proto files
  file(GLOB PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/proto/*.proto)
  if(PROTO_FILES)
    foreach(proto_file ${PROTO_FILES})
      get_filename_component(proto_name ${proto_file} NAME_WE)
      get_filename_component(proto_dir ${proto_file} DIRECTORY)
      set(proto_src ${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.cc)
      set(proto_hdr ${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.h)
      
      add_custom_command(
        OUTPUT ${proto_src} ${proto_hdr}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
          --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
          -I${proto_dir}
          ${proto_file}
        DEPENDS ${proto_file}
        COMMENT "Generating C++ code from ${proto_name}.proto"
      )
      
      list(APPEND PROTO_SRCS ${proto_src})
      list(APPEND PROTO_HDRS ${proto_hdr})
    endforeach()
    
    add_library(rollingraft_proto STATIC ${PROTO_SRCS})
    target_include_directories(rollingraft_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
    target_link_libraries(rollingraft_proto PUBLIC protobuf::libprotobuf)
    set_target_properties(rollingraft_proto PROPERTIES POSITION_INDEPENDENT_CODE ON)
    # Protobuf 35+ depends on abseil — ensure transitive deps propagate
    if(TARGET absl::base)
      target_link_libraries(rollingraft_proto PUBLIC absl::log_internal_message absl::log_internal_check_op)
    endif()
  endif()
else()
  message(STATUS "Protobuf not found — protobuf MVP benchmark disabled")
endif()

# Link rollingraft with protobuf generated code (must be after rollingraft_proto target)
if(TARGET rollingraft_proto)
  # PUBLIC so that consumers of rollingraft (tests, examples) can include
  # raft_log_entry.pb.h and get transitive protobuf linkage.
  target_link_libraries(rollingraft PUBLIC rollingraft_proto)
endif()

# Link dependencies
target_link_libraries(rollingraft
  PUBLIC
    rollingraft::asio
    rollingraft::json
    rollingraft::spdlog
  PRIVATE
    leveldb::leveldb
    OpenSSL::SSL
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

# Benchmarks
if(BUILD_BENCHMARK)
  add_subdirectory(benchmark)
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
