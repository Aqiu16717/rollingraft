#pragma once

#include <atomic>
#include <cstdint>
#include <unistd.h>
#include <vector>

/**
 * Return a unique ephemeral port for unit testing.
 *
 * Each process gets a distinct base derived from its PID, and an
 * atomic counter ensures uniqueness within the process. This avoids
 * port collisions when ctest runs tests in parallel with -jN.
 */
inline uint16_t GetUniqueTestPort() {
  static uint16_t base =
      static_cast<uint16_t>(20000 + (getpid() % 30000));
  static std::atomic<uint16_t> counter{0};
  return static_cast<uint16_t>(base + counter.fetch_add(1));
}

/**
 * Allocate a block of unique test ports.
 */
inline std::vector<uint16_t> GetTestPorts(size_t count) {
  std::vector<uint16_t> ports;
  ports.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    ports.push_back(GetUniqueTestPort());
  }
  return ports;
}
