#pragma once

#include <cstdint>
#include <memory>

namespace rollingraft {

class Server {
 public:
  Server() = default;
  Server(uint32_t id, int port);
  ~Server();
  void Start();
 private:
  class ServerImpl;
  std::unique_ptr<ServerImpl> server_impl_;
};

}  // namespace rollingraft
