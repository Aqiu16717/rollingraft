#include "rollingraft/server.h"

#include <asio.hpp>
#include <cstdint>
#include <iostream>
#include <memory>
#include <ostream>
#include <system_error>
#include <vector>

#include "rollingraft/command_handler.h"

#include "asio/io_context.hpp"
#include "asio/ip/tcp.hpp"
#include "asio/write.hpp"

using namespace rollingraft;

class Session : public std::enable_shared_from_this<Session> {
 public:
  explicit Session(asio::ip::tcp::socket socket,
                   std::shared_ptr<CommandHandler> command_handler)
      : socket_(std::move(socket)), command_handler_(command_handler) {}

  void Start() { DoRead(); }

 private:
  void DoRead() {
    auto self = shared_from_this();
    socket_.async_read_some(
        asio::buffer(data_),
        [this, self](std::error_code ec, std::size_t length) {
          if (!ec) {
            std::string request(data_.data(), length);
            std::string response;
            command_handler_->HandleCommand(request, response);
            DoWrite(response);
          } else {
            std::cerr << "Read error: " << ec.message() << std::endl;
          }
        });
  }

  void HandleCommand(const std::string& request, std::string& response) {}

  void DoWrite(const std::string& msg) {
    auto self = shared_from_this();
    asio::async_write(socket_, asio::buffer(msg),
                      [this, self](std::error_code ec, std::size_t) {
                        if (!ec) {
                          DoRead();
                        } else {
                          std::cerr << "Write error: " << ec.message()
                                    << std::endl;
                        }
                      });
  }

 private:
  asio::ip::tcp::socket socket_;
  std::vector<char> data_;
  std::shared_ptr<CommandHandler> command_handler_;
};

class Server::ServerImpl {
 public:
  ServerImpl(uint32_t id, uint16_t port, asio::io_context& io_ctx,
             std::shared_ptr<CommandHandler> command_handler)
      : server_id_(id),
        io_context_(io_ctx),
        acceptor_(io_ctx, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)),
        command_handler_(std::move(command_handler)) {}

  void Start() { DoAccept(); }

 private:
  void DoAccept() {
    acceptor_.async_accept([this](std::error_code ec,
                                  asio::ip::tcp::socket socket) {
      if (!ec) {
        std::make_shared<Session>(std::move(socket), command_handler_)->Start();
      } else {
        std::cerr << "Accept error: " << ec.message() << std::endl;
      }
      DoAccept();
    });
  }

 private:
  uint32_t server_id_;
  asio::io_context& io_context_;
  asio::ip::tcp::acceptor acceptor_;
  std::shared_ptr<CommandHandler> command_handler_;
};
