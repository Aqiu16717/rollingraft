#include "rollingraft/raft_node.h"

#include <atomic>
#include <cstdint>
#include <future>
#include <random>

#include "rollingraft/logger.h"
#include "rollingraft/rpc.h"
#include "rollingraft/server.h"

using namespace rollingraft;

class RaftNode::RaftNodeImpl {
  using RaftNodeId = int32_t;

 public:
  RaftNodeImpl(const RaftNodeConfig& config,
               std::shared_ptr<StateMachine> state_machine,
               std::unique_ptr<NetworkTransport> network,
               std::unique_ptr<TimerService> timer,
               std::unique_ptr<Persister> persister,
               std::unique_ptr<Protocol> protocol) {}
  ~RaftNodeImpl() = default;

  Status Start();
  Status Stop();

  bool IsLeader() const;
  RaftNodeRole GetRole() const;
  uint64_t CurrentTerm() const;
  std::string GetLeaderAddr() const;

  void SetRoleChangeCallback(std::function<void(RaftNodeRole, uint64_t)> cb);
  void SetLeaderChangeCallback(std::function<void(RaftNodeId, std::string)> cb);

  Status Propose(const std::string& command,
                 std::function<void(const ApplyResult&)> callback);
  Status ReadIndex(std::function<void()> callback);

  Status RequestVote(const RequestVoteRequest&, RequestVoteResponse&);
  Status AppendEntries(const AppendEntriesRequest&, AppendEntriesResponse&);
  Status InstallSnapshot(const InstallSnapshotRequest&,
                         InstallSnapshotResponse&);

 private:
  Status BecomeFollower();
  Status BecomeCandidate();
  Status BecomeLeader();

  Status Election();

  inline void SetRole(const RaftNodeRole& state);

  inline uint32_t GetServerId() const { return server_id_; }

  void RandomizeElectionTimeout();

 private:
  RaftNodeId server_id_;
  std::vector<RaftNodeId> peers_;

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
  // candidate id that received vote in current term
  // initialized to -1 (null)
  RaftNodeId voted_for_;
  RaftNodeRole role_;

  // Volatile state on all servers

  // index of highest log entry known to be committed
  // initialized to 0, increases monotonically)
  uint32_t commit_index_;
  // index of highest log entry applied to state machine
  // initialized to 0, increases monotonically
  uint32_t last_applied_;

  RaftLog log_;
  uint32_t vote_count_;

  // amount of time left till timeout
  int timeout_elapsed_ = 0;
  int election_timeout_ = 0;

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
  std::mutex mtx_;

 private:
  using WorkGuard = asio::executor_work_guard<asio::io_context::executor_type>;
  std::unique_ptr<WorkGuard> work_guard_;
  std::unique_ptr<Server> server_;
  asio::io_context io_context_;
  std::thread io_thread_;
  std::atomic<bool> is_running_;

 private:
  RaftNodeConfig config_;
  std::shared_ptr<StateMachine> state_machine_;
  std::unique_ptr<NetworkTransport> network_;
  std::unique_ptr<TimerService> timer_;
  std::unique_ptr<Persister> persister_;
  std::unique_ptr<Protocol> protocol_;
};

void RaftNode::RaftNodeImpl::SetRole(const RaftNodeRole& role) { role_ = role; }

Status RaftNode::RaftNodeImpl::BecomeFollower() {
  LOG_INFO("Node {} become follower.", server_id_);
  SetRole(RaftNodeRole::FOLLOWER);
  RandomizeElectionTimeout();
  return Status();
}

// 1. become candidate
// 2. start election
Status RaftNode::RaftNodeImpl::BecomeCandidate() {
  ++current_term_;
  ++vote_count_;
  SetRole(RaftNodeRole::CANDIDATE);
  return Status();
}

Status RaftNode::RaftNodeImpl::BecomeLeader() {
  SetRole(RaftNodeRole::LEADER);
  // AppendEntriesRequest req{
  //     .} AppendEntries(req);
  return Status();
}

Status RaftNode::RaftNodeImpl::Election() {
  RequestVoteRequest req(current_term_, server_id_, log_.LastLogIndex(),
                         log_.LastLogTerm());

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

Status RaftNode::RaftNodeImpl::RequestVote(const RequestVoteRequest& request,
                                           RequestVoteResponse& response) {
  // lock for thread safety
  std::lock_guard<std::mutex> lock(mtx_);

  // always set response.term_ to current_term_
  response.term_ = current_term_;

  LOG_INFO("Node {} received RequestVote from {} at term {}", server_id_,
           request.candidate_id_, request.term_);

  // refuse vote if candidate's term is older than my term
  if (request.term_ < current_term_) {
    response.vote_granted_ = false;
    LOG_INFO("Rejecting vote for {}: their term {} is older than mine {}",
             request.candidate_id_, request.term_, current_term_);
    return Status::OK();
  }

  // if cnadidate's term is newer, become follower whatever my state is
  if (request.term_ > current_term_) {
    LOG_INFO("Updating term from {} to {}", current_term_, request.term_);
    // reset voted_for_
    voted_for_ = -1;
    current_term_ = request.term_;
    // todo
    // persister_->
    BecomeFollower();
  }

  // Each server will vote for at most one candidate in a
  // given term, on a first-come-first-served basis (note: Sec-
  // tion 5.4 adds an additional restriction on votes).

  return Status();
}

Status RaftNode::RaftNodeImpl::AppendEntries(
    const AppendEntriesRequest& append_entries_request,
    AppendEntriesResponse& append_enctries_response) {
  for (;;) {
    // send
  }
  return Status();
}

Status RaftNode::RaftNodeImpl::InstallSnapshot(
    const InstallSnapshotRequest& install_snapshot_request,
    InstallSnapshotResponse& install_snapshot_response) {
  return Status();
}

void RaftNode::RaftNodeImpl::RandomizeElectionTimeout() {
  constexpr uint64_t kMinElectionTimeout = 150;
  constexpr uint64_t kMaxElectionTimeout = 300;

  // Thread-safe random number generator
  // use static variables to ensure it is initialized only once
  static std::mt19937_64 rng;
  // Ensure the random number seed is initialized only once
  static std::once_flag init_flag;
  std::call_once(init_flag, []() {
    // Use system time as the seed to ensure that the random sequence is
    // different each time it is started
    rng.seed(std::chrono::system_clock::now().time_since_epoch().count());
  });

  // Generate a random number between [kMinElectionTimeout,
  // kMaxElectionTimeout)
  std::uniform_int_distribution<uint64_t> dist(kMinElectionTimeout,
                                               kMaxElectionTimeout);
  election_timeout_ = dist(rng);

  // Reset the elapsed time
  timeout_elapsed_ = 0;

  LOG_INFO("Node {} randomized election timeout to {}ms (role: {})", server_id_,
           election_timeout_, RaftNodeRoleToString(role_));
}

Status RaftNode::RaftNodeImpl::Start() {
  if (is_running_.exchange(true)) {
    return Status::OK();
  }

  LOG_INFO("Starting RaftNode {} on {}", config_.node_id, config_.listen_addr);

  Status status = server_->Start();
  if (!status.ok()) {
    LOG_ERROR("Failed to start server: {}", status.ToString());
    return status;
  }

  work_guard_ = std::make_unique<WorkGuard>(asio::make_work_guard(io_context_));

  if (!work_guard_) {
    return Status::RaftNodeStartError("Failed to allocate work_guard");
  }

  io_thread_ = std::thread([this]() { io_context_.run(); });

  status = BecomeFollower();
  if (!status.ok()) {
    LOG_ERROR("Failed to become follower: {}", status.ToString());
    // todo: cleanup or stop
    return status;
  }

  LOG_INFO("Start success: {} on {}", config_.node_id, config_.listen_addr);
  return Status::OK();
}

Status RaftNode::RaftNodeImpl::Stop() {
  if (!is_running_.exchange(false)) {
    return Status::OK();
  }

  LOG_INFO("Stopping RaftNode: {} on {}", config_.node_id, config_.listen_addr);
  if (server_) {
    server_->Stop();
  }

  if (work_guard_) {
    work_guard_.reset();
  }

  io_context_.stop();

  if (io_thread_.joinable()) {
    if (std::this_thread::get_id() == io_thread_.get_id()) {
      LOG_ERROR(
          "Stop() called from within the IO thread! Deatching instead of "
          "joining.");
      io_thread_.detach();
    } else {
      io_thread_.join();
    }
  }

  LOG_INFO("Stop success: {} on {}", config_.node_id, config_.listen_addr);
  return Status::OK();
}
