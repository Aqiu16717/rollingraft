# pragma once

namespace rollingraft {

class Status {
public:
private:
    enum Code {
        KOk = 0
    };
    // steal from leveldb
    // OK status has a null state_
    // Otherwise, state_ is a new[] array of the following from
    // state_[0..3]  == length of message
    // state_[4] == code
    // state_[5..] == message
    const char* state_;
};

} // namespace rollingraft
