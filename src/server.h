#pragma once

#include <cstdint>

#include "rpc.h"
#include "status.h"

namespace rollingraft {

/**
 * At any given time each server is in one of three states:
 * leader, follower, or candidate.
 * In normal operation there is exactly one leader and all
 * of the other servers are followers. 
 * Followers are passive: they issue no requests on
 * their own but simply respond to requests from leaders
 * and candidates.
 * The leader handles all client requests (if a client contacts
 * a follower, the follower redirects it to the leader).
 * The third state, candidate, is used to elect a new leader
 * as described in Section 5.2. Figure 4 shows the states and
 * their transitions; the transitions are discussed below.
 */
enum ServerState {
    LEADER = 0,
    FOLLOWER = 1,
    CANDIDATE = 3
};


class Server {
public:
    Status RequestVote();
    Status AppendEntries();
    Status InstallSnapshot();
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
    uint32_t current_term_;
    ServerState state_;

    // Volatile state on all servers

    // index of highest log entry known to be committed
    // initialized to 0, increases monotonically)
    uint32_t commit_index_;
    // index of highest log entry applied to state machine
    // initialized to 0, increases monotonically
    uint32_t last_applied_;
};

class LeaderServer: public Server {
public:
    void heartbeat();
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
};

class FollowerServer: public Server {
public:
private:
};

class CandidateServer: public Server {
public:
private:
};

} // namespace RollingRaft
