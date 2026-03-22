#include "rollingraft/raft_node.h"

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
  RaftNodeImpl(uint32_t id, const std::vector<RaftNodeId>& peers)
      : server_id_(id),
        peers_(peers),
        current_term_(0),
        voted_for_(-1),
        state_(FOLLOWER),
        commit_index_(0),
        last_applied_(0),
        vote_count_(0),
        timeout_elapsed_(0),
        next_index_(0),
        match_index_(0) {}

  Status RequestVote(const RequestVoteRequest&, RequestVoteResponse&);
  Status AppendEntries(const AppendEntriesRequest&, AppendEntriesResponse&);
  Status InstallSnapshot(const InstallSnapshotRequest&,
                         InstallSnapshotResponse&);

  Status BecomeFollower();
  Status BecomeCandidate();
  Status BecomeLeader();

  Status Election();

  inline void SetState(const RaftNodeState& state);

  inline uint32_t GetServerId() const { return server_id_; }

 private:
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
  RaftNodeState state_;

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
  std::unique_ptr<Server> server_;
};

void RaftNode::RaftNodeImpl::SetState(const RaftNodeState& state) {
  state_ = state;
}

Status RaftNode::RaftNodeImpl::BecomeFollower() {
  LOG_INFO("Node {} become follower.", server_id_);
  SetState(RaftNodeState::FOLLOWER);
  RandomizeElectionTimeout();
  return Status();
}

// 1. become candidate
// 2. start election
Status RaftNode::RaftNodeImpl::BecomeCandidate() {
  ++current_term_;
  ++vote_count_;
  SetState(RaftNodeState::CANDIDATE);
  return Status();
}

Status RaftNode::RaftNodeImpl::BecomeLeader() {
  SetState(RaftNodeState::LEADER);
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

  // Generate a random number between [kMinElectionTimeout, kMaxElectionTimeout)
  std::uniform_int_distribution<uint64_t> dist(kMinElectionTimeout,
                                               kMaxElectionTimeout);
  election_timeout_ = dist(rng);

  // Reset the elapsed time
  timeout_elapsed_ = 0;

  LOG_INFO("Node {} randomized election timeout to {}ms (state: {})",
           server_id_, election_timeout_, RaftNodeStateToString(state_));
}
