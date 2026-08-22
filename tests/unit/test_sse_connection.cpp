#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>

#include <asio.hpp>

#include "sse_connection.h"
#include <gtest/gtest.h>

namespace rollingraft {
namespace {

TEST(SseConnectionTest, PeerDisconnectClosesAndNotifies) {
  asio::io_context io_context;
  asio::ip::tcp::acceptor acceptor(io_context,
                                   asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));

  asio::ip::tcp::socket client_socket(io_context);
  client_socket.connect(acceptor.local_endpoint());
  asio::ip::tcp::socket server_socket = acceptor.accept();

  std::promise<void> closed_promise;
  auto closed_future = closed_promise.get_future();
  std::atomic<int> close_count{0};
  auto connection = std::make_shared<SseConnection>(
      std::move(server_socket), asio::io_context::strand(io_context),
      [&closed_promise, &close_count](SseConnection*) {
        if (close_count.fetch_add(1) == 0) {
          closed_promise.set_value();
        }
      });

  connection->Start();
  std::thread io_thread([&io_context] { io_context.run(); });

  asio::streambuf headers;
  asio::read_until(client_socket, headers, "\r\n\r\n");
  std::error_code ec;
  client_socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
  client_socket.close(ec);

  EXPECT_EQ(closed_future.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  EXPECT_FALSE(connection->IsOpen());
  EXPECT_EQ(close_count.load(), 1);

  io_context.stop();
  io_thread.join();
}

}  // namespace
}  // namespace rollingraft
