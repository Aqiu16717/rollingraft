#include "server.h"

using namespace rollingraft;

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

Status Server::RequestVote(const RequestVoteRequest& request_vote_request,
                           RequestVoteResponse& request_vote_reponse) {
    return Status();
}
