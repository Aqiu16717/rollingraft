#include "server.h"
#include <thread>
#include <vector>
#include <future>

using namespace rollingraft;

Status Server::RequestVote(const RequestVoteRequest& request_vote_request,
                           RequestVoteResponse& request_vote_reponse) {

    return Status();
}

Status Server::AppendEntries(const AppendEntriesRequest& append_entries_request,
                             AppendEntriesResponse& append_enctries_response) {
    for (;;) {
        // send
    }
    return Status();
}

Status Server::InstallSnapshot(const InstallSnapshotRequest& install_snapshot_request,
                               InstallSnapshotResponse& install_snapshot_response) {
    return Status();
}

Status Server::BecomeFollower() {
    SetState(ServerState::FOLLOWER);
    RandomizeElectionTimeout();
    return Status();
}

// 1. become candidate
// 2. start election
Status Server::BecomeCandidate() {
    ++current_term_;
    ++vote_count_;
    SetState(ServerState::CANDIDATE);
    return Status();
}

Status Server::BecomeLeader() {
    SetState(ServerState::LEADER);
    return Status();
}

Status Server::Election() {
    RequestVoteRequest req {
        .term_ = current_term_,
        .candidate_id_ = server_id_,
        .last_log_index_ = log_.LastLogIndex(),
        .last_log_term_ = log_.LastLogTerm()
    };

    // parallel
    std::vector<std::future<RequestVoteResponse>> fus;
    for (int i = 0; i < peers_.size(); ++i) {
        fus.push_back(std::async(std::launch::async,
            [&]() {
                RequestVoteResponse res;
                RequestVote(req, res);
                return res;
            }
        ));
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
}
