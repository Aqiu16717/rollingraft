#include "server.h"
#include <thread>
#include <vector>

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

// 1. become candidate
// 2. start election
Status Server::BecomeCandidate() {
    ++current_term_;
    ++vote_count_;
    state_ = CANDIDATE;
}

Status Server::Election() {
    RequestVoteRequest req{
        .term_ = current_term_,
        .candidate_id_ = server_id_,
        .last_log_index_ = log_.LastLogIndex(),
        .last_log_term_ = log_.LastLogTerm()
    };
    RequestVoteResponse res;

    // parallel
    std::vector<std::thread> thds;
    for (int i = 0; i < peers_.size(); ++i) {
        thds.push_back(std::thread([&](){
            RequestVote(req, res);
        }));
    }

    return Status();
}
