#include "rollingraft/server.h"

#include <asio.hpp>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <ostream>
#include <system_error>
#include <vector>

#include "asio/io_context.hpp"
#include "asio/ip/tcp.hpp"
#include "asio/write.hpp"
#include "rollingraft/rpc.h"

using namespace rollingraft;

class Session : public std::enable_shared_from_this<Session> {
 public:
  explicit Session(asio::ip::tcp::socket socket)
   : socket_(std::move(socket)) {}

  void Start() {
    DoRead();
  }

 private:
  void DoRead() {
    auto slef = shared_from_this();
    socket_.async_read_some(asio::buffer(data_),[this, slef](std::error_code ec, std::size_t length){
      if (!ec) {
        std::string request(data_.data(), length);
        std::string response;
        HandleCommand(request, response);
        DoWrite(response);
      } else {
        std::cerr << "Read error: " << ec.message() << std::endl;
      }
    });
  }

  void HandleCommand(const std::string& request, std::string& response) {
  }

  void DoWrite(const std::string& msg) {
    auto self = shared_from_this();
    asio::async_write(socket_, asio::buffer(msg),[this, self](std::error_code ec, std::size_t) {
      if (!ec) {
        DoRead();
      } else {
        std::cerr << "Write error: " << ec.message() << std::endl;
      }
    });
  }

 private:
  asio::ip::tcp::socket socket_;
  std::vector<char> data_;
};

class Server::ServerImpl {
 public:
  ServerImpl(uint32_t id, const std::vector<uint32_t>& peers, uint16_t port,
             asio::io_context& io_ctx)
      : server_id_(id),
        peers_(peers),
        io_context_(io_ctx),
        acceptor_(io_ctx, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)) {}
  void Start();

  Status RequestVote(const RequestVoteRequest&, RequestVoteResponse&);
  Status AppendEntries(const AppendEntriesRequest&, AppendEntriesResponse&);
  Status InstallSnapshot(const InstallSnapshotRequest&,
                         InstallSnapshotResponse&);

  Status BecomeFollower();
  Status BecomeCandidate();
  Status BecomeLeader();

  Status Election();

  inline void SetState(const ServerState& state);
 private:
  void DoAccept() {
    acceptor_.async_accept([this](std::error_code ec, asio::ip::tcp::socket socket){
      if (!ec) {
        std::make_shared<Session>(std::move(socket))->Start();
      }
      DoAccept();
    });
  }
 private:
  void RandomizeElectionTimeout();

 private:
  // Persistent state on all servers
  /**
   * Raft divides time into terms of arbitrary length, as
   * shown in Figure 5. Terms are numbered with consecutive
   * integers. Each term begins with an election, in which one
   * or more candidates attempt to become leader as described
   * in Section 5.2. If a candidate wins the election, then it
   * serves as leader for the rest of the term. In some situations
   * an election will result in a split vote. In this case the term
   * will end with no leader; a new term (with a new election) will
   * begin shortly. Raft ensures that there is at most one leader
   * in a given term.
   * Different servers may observe the transitions between
   * terms at different times, and in some situations a server
   * may not observe an election or even entire terms. Terms
   * act as a logical clock [14] in Raft, and they allow servers
   * to detect obsolete information such as stale leaders. Each
   * server stores a current term number, which increases
   * monotonically over time. Current terms are exchanged
   * whenever servers communicate; if one server’s current
   * term is smaller than the other’s, then it updates its current
   * term to the larger value. If a candidate or leader discovers
   * that its term is out of date, it immediately reverts to fol-
   * lower state. If a server receives a request with a stale term
   * number, it rejects the request.
   */
  // latest term server has seen (initialized to 0
  // on first boot, increases monotonically)
  uint32_t current_term_ = 0;
  ServerState state_;

  // Volatile state on all servers

  // index of highest log entry known to be committed
  // initialized to 0, increases monotonically)
  uint32_t commit_index_;
  // index of highest log entry applied to state machine
  // initialized to 0, increases monotonically
  uint32_t last_applied_;

  std::vector<uint32_t> peers_;
  Log log_;
  uint32_t server_id_;
  uint32_t vote_count_;

  // amount of time left till timeout
  int timeout_elapsed_ = 0;

 private:
  // Volatile state on leaders

  // for each server, index of the next log entry
  // to send to that server (initialized to leader
  // last log index + 1)
  std::vector<uint32_t> next_index_;
  // for each server, index of highest log entry
  // known to be replicated on server
  // (initialized to 0, increases monotonically)
  std::vector<uint32_t> match_index_;

 private:
  asio::io_context& io_context_;
  asio::ip::tcp::acceptor acceptor_;
};

void Server::ServerImpl::Start() {
}

void Server::ServerImpl::SetState(const ServerState& state) { state_ = state; }

Status Server::ServerImpl::BecomeFollower() {
  SetState(ServerState::FOLLOWER);
  RandomizeElectionTimeout();
  return Status();
}

// 1. become candidate
// 2. start election
Status Server::ServerImpl::BecomeCandidate() {
  ++current_term_;
  ++vote_count_;
  SetState(ServerState::CANDIDATE);
  return Status();
}

Status Server::ServerImpl::BecomeLeader() {
  SetState(ServerState::LEADER);
  // AppendEntriesRequest req{
  //     .} AppendEntries(req);
  return Status();
}

Status Server::ServerImpl::Election() {
  RequestVoteRequest req{.term_ = current_term_,
                         .candidate_id_ = server_id_,
                         .last_log_index_ = log_.LastLogIndex(),
                         .last_log_term_ = log_.LastLogTerm()};

  // parallel
  std::vector<std::future<RequestVoteResponse>> fus;
  for (int i = 0; i < peers_.size(); ++i) {
    fus.push_back(std::async(std::launch::async, [&]() {
      RequestVoteResponse res;
      RequestVote(req, res);
      return res;
    }));
  }

  for (auto& fu : fus) {
    RequestVoteResponse res = fu.get();
    if (res.vote_granted_) {
      ++vote_count_;
    }
  }

  if (vote_count_ > peers_.size() / 2) {
    return Status();
  }

  return Status();
}

Status Server::ServerImpl::RequestVote(
    const RequestVoteRequest& request_vote_request,
    RequestVoteResponse& request_vote_reponse) {
  return Status();
}

Status Server::ServerImpl::AppendEntries(
    const AppendEntriesRequest& append_entries_request,
    AppendEntriesResponse& append_enctries_response) {
  for (;;) {
    // send
  }
  return Status();
}

Status Server::ServerImpl::InstallSnapshot(
    const InstallSnapshotRequest& install_snapshot_request,
    InstallSnapshotResponse& install_snapshot_response) {
  return Status();
}
