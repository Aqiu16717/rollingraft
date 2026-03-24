#pragma once
#include <string>

namespace rollingraft {

// most code steal from leveldb
class Status {
 public:
  // Create a success status.
  Status() noexcept : state_(nullptr) {}
  ~Status() { delete[] state_; }

  Status(const Status& rhs);
  Status& operator=(const Status& rhs);

  Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }
  Status& operator=(Status&& rhs) noexcept;

  // Return a success status.
  static Status OK() { return Status(); }

  static Status Corruption(const std::string& msg,
                           const std::string& msg2 = "") {
    return Status(kCorruption, msg, msg2);
  }

  // Return error status of an appropriate type
  static Status RequestVoteError(const std::string& msg,
                                 const std::string& msg2 = "") {
    return Status(kRequestVoteError, msg, msg2);
  }
  static Status AppendEntriesError(const std::string& msg,
                                   const std::string& msg2 = "") {
    return Status(kAppendEntriesError, msg, msg2);
  }
  static Status InstallSnapshotError(const std::string& msg,
                                     const std::string& msg2 = "") {
    return Status(kInstallSnapshotError, msg, msg2);
  }
  static Status SerializeError(const std::string& msg,
                               const std::string& msg2 = "") {
    return Status(kSerializeError, msg, msg2);
  }
  static Status DeSerializeError(const std::string& msg,
                                 const std::string& msg2 = "") {
    return Status(kDeSerializeError, msg, msg2);
  }
  static Status ProtocolError(const std::string& msg,
                              const std::string& msg2 = "") {
    return Status(kProtocolError, msg, msg2);
  }
  static Status RaftNodeStartError(const std::string& msg,
                                   const std::string& msg2 = "") {
    return Status(kRaftNodeStartError, msg, msg2);
  }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == nullptr); }

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const { return code() == kCorruption; }

  // Returns true iff the status indicates a RequestVoteError error.
  bool IsRequestVoteError() const { return code() == kRequestVoteError; };

  // Returns true iff the status indicates a AppendEntries error.
  bool IsAppendEntriesError() const { return code() == kAppendEntriesError; }

  // Returns true iff the status indicates a InstallSnapshot error.
  bool IsInstallSnapshotError() const {
    return code() == kInstallSnapshotError;
  }

  // Returns true iff the status indicates a DeSerialize error.
  bool IsDeSerializeError() const { return code() == kDeSerializeError; }

  // Returns true iff the status indicates a Serialize error.
  bool IsSerializeError() const { return code() == kSerializeError; }

  // Returns true iff the status indicates a Protocol error.
  bool IsProtocolError() const { return code() == kProtocolError; }

  // Returns true iff the status indicates a Protocol error.
  bool IsRaftNodeStartError() const { return code() == kRaftNodeStartError; }

  // Return a string representation of this status suitable of printing.
  // Return the string "OK" for success.
  std::string ToString() const;
  std::string GetMessage() const;
  int GetCode() const;

 private:
  enum Code : int8_t {
    kOk = 0,
    kCorruption,
    kRequestVoteError,
    kAppendEntriesError,
    kInstallSnapshotError,
    kSerializeError,
    kDeSerializeError,
    kProtocolError,
    kRaftNodeStartError
  };

  Code code() const {
    return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
  }

  Status(Code code, const std::string& msg, const std::string& msg2);
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
inline Status& Status::operator=(Status&& rhs) noexcept {
  std::swap(state_, rhs.state_);
  return *this;
}

}  // namespace rollingraft
