# pragma once
#include <string>

namespace rollingraft {

// most code steal from leveldb
class Status {
public:
    // Create a success status.
    Status() noexcept : state_(nullptr) {}
    ~Status() {
        delete[] state_;
    }

    Status(const Status& rhs);
    Status& operator=(const Status& rhs);

    Status(Status&& rhs) noexcept : state_(rhs.state_) {
        rhs.state_ = nullptr;
    }
    Status& operator=(Status&& rhs) noexcept;

    // Return a success status.
    static Status OK() { return Status(); }

    // Return error status of an appropriate type


    // Returns true iff the status indicates success.
    bool ok() const { return (state_ == nullptr); }

    // Return a string representation of this status suitable of printing.
    // Return the string "OK" for success.
    std::string ToString() const;

private:
    enum Code : int8_t {
        KOk = 0,
        KRequestVoteError,
        KAppendEntriesError,
        KInstallSnapshotErro
    };

    Code code() const {
        return (state_ == nullptr) ? KOk : static_cast<Code>(state_[4]);
    }
    static const char* CopyState(const char* s);

    // OK status has a null state_
    // Otherwise, state_ is a new[] array of the following from
    // state_[0..3]  == length of message
    // state_[4] == code
    // state_[5..] == message
    const char* state_;
};

inline Status::Status(const Status& rhs) {
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

inline Status& Status::operator=(const Status& rhs) {
    // The following condition catches both aliasing (when this == &rhs),
    // and the common case where both rhs and *this are ok.
    if (state_ != rhs.state_) {
        delete[] state_;
        state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
    }
    return *this;
}
inline Status& Status::operator=(Status&& rhs) {
    std::swap(state_, rhs.state_);
    return *this;
}

} // namespace rollingraft
