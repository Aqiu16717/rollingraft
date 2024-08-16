#pragma once

#include <cstdint>

namespace rollingraft {

/*
  At any given time each server is in one of three states:
  leader, follower, or candidate.
  In normal operation there is exactly one leader and all
  of the other servers are followers. 
  Followers are passive: they issue no requests on
  their own but simply respond to requests from leaders
  and candidates.
  The leader handles all client requests (if a client contacts
  a follower, the follower redirects it to the leader).
  The third state, candidate, is used to elect a new leader
  as described in Section 5.2. Figure 4 shows the states and
  their transitions; the transitions are discussed below.
*/
enum ServerState {
    LEADER = 0,
    FOLLOWER = 1,
    CANDIDATE = 3
};

class Server {
public:
private:
    uint32_t current_term_;
    ServerState state_;
};

} // namespace RollingRaft
