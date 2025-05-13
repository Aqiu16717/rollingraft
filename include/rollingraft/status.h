# pragma once

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

    // Returns true iff the status indicates success.
    bool ok() const { return (state_ == nullptr); }

private:
    enum Code {
        KOk = 0
    };
    // OK status has a null state_
    // Otherwise, state_ is a new[] array of the following from
    // state_[0..3]  == length of message
    // state_[4] == code
    // state_[5..] == message
    const char* state_;
};

} // namespace rollingraft
