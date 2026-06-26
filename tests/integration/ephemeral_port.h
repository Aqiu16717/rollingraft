/**
 * @file ephemeral_port.h
 * @brief Ephemeral port allocator for integration tests
 *
 * Binds an ASIO acceptor to :0 to obtain a kernel-assigned ephemeral port,
 * then immediately releases it. Tests can re-bind to the same port safely
 * because SO_REUSEADDR is set on both sides.
 */

#pragma once

#include <string>
#include <vector>

#include <asio.hpp>

namespace rollingraft {

/**
 * Allocate a set of ephemeral TCP ports on 127.0.0.1.
 *
 * Each port is discovered by briefly binding an ASIO acceptor to :0,
 * reading the kernel-assigned port number, and closing the acceptor.
 * SO_REUSEADDR is set so that the test code can re-bind immediately.
 *
 * @param count Number of ports to allocate
 * @return Vector of port numbers (e.g., {54321, 54322, 54323})
 */
inline std::vector<uint16_t> AllocateEphemeralPorts(size_t count) {
  std::vector<uint16_t> ports;
  ports.reserve(count);

  asio::io_context io;
  for (size_t i = 0; i < count; ++i) {
    asio::ip::tcp::acceptor acceptor(io);
    acceptor.open(asio::ip::tcp::v4());
    acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
    acceptor.bind(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    ports.push_back(acceptor.local_endpoint().port());
    acceptor.close();
  }

  return ports;
}

/**
 * Format a vector of ports into "127.0.0.1:<port>" strings.
 */
inline std::vector<std::string> FormatAddrs(const std::vector<uint16_t>& ports) {
  std::vector<std::string> addrs;
  addrs.reserve(ports.size());
  for (uint16_t port : ports) {
    addrs.push_back("127.0.0.1:" + std::to_string(port));
  }
  return addrs;
}

}  // namespace rollingraft
