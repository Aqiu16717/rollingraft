/**
 * @file status.h
 * @brief Status codes for error handling
 *
 * Inspired by LevelDB's Status class. Provides lightweight error
 * handling with rich error codes and messages.
 */

#pragma once

#include <cstdint>
#include <string>

#include "rollingraft/types.h"

namespace rollingraft {

/**
 * Status code for operation results.
 *
 * Lightweight value type for returning errors. Uses SSO-like
 * optimization where OK status has zero allocation.
 *
 * Example:
 *   Status s = DoSomething();
 *   if (!s.ok()) {
 *       LOG_ERROR("Failed: {}", s.ToString());
 *   }
 */
class Status {
 public:
  /** Create a success status. */
  Status() noexcept : state_(nullptr) {}

  ~Status() { delete[] state_; }

  Status(const Status& rhs);
  Status& operator=(const Status& rhs);

  Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }
  Status& operator=(Status&& rhs) noexcept;

  /** Return a success status. */
  static Status OK() { return Status(); }

  /** Return a corruption error status. */
  static Status Corruption(const std::string& msg,
                           const std::string& msg2 = "") {
    return Status(kCorruption, msg, msg2);
  }

  /** Return a RequestVote RPC error status. */
  static Status RequestVoteError(const std::string& msg,
                                 const std::string& msg2 = "") {
    return Status(kRequestVoteError, msg, msg2);
  }

  /** Return an AppendEntries RPC error status. */
  static Status AppendEntriesError(const std::string& msg,
                                   const std::string& msg2 = "") {
    return Status(kAppendEntriesError, msg, msg2);
  }

  /** Return an InstallSnapshot RPC error status. */
  static Status InstallSnapshotError(const std::string& msg,
                                     const std::string& msg2 = "") {
    return Status(kInstallSnapshotError, msg, msg2);
  }

  /** Return a serialization error status. */
  static Status SerializeError(const std::string& msg,
                               const std::string& msg2 = "") {
    return Status(kSerializeError, msg, msg2);
  }

  /** Return a deserialization error status. */
  static Status DeSerializeError(const std::string& msg,
                                 const std::string& msg2 = "") {
    return Status(kDeSerializeError, msg, msg2);
  }

  /** Return a protocol error status. */
  static Status ProtocolError(const std::string& msg,
                              const std::string& msg2 = "") {
    return Status(kProtocolError, msg, msg2);
  }

  /** Return a Raft node start error status. */
  static Status RaftNodeStartError(const std::string& msg,
                                   const std::string& msg2 = "") {
    return Status(kRaftNodeStartError, msg, msg2);
  }

  /** Return a generic error status. */
  static Status Error(const std::string& msg, const std::string& msg2 = "") {
    return Status(kGenericError, msg, msg2);
  }

  /**
   * Return a NotLeader error status.
   * @param leader_id Current leader node ID (or -1 if unknown)
   * @param leader_addr Current leader address
   */
  static Status NotLeader(NodeId leader_id, const std::string& leader_addr) {
    std::string msg = "Not leader";
    if (leader_id >= 0) {
      msg += ", leader is " + leader_addr;
    }
    return Status(kNotLeader, msg, "");
  }

  /** Returns true if the status indicates success. */
  bool ok() const { return (state_ == nullptr); }

  /** Returns true if the status indicates a Corruption error. */
  bool IsCorruption() const { return code() == kCorruption; }

  /** Returns true if the status indicates a RequestVote error. */
  bool IsRequestVoteError() const { return code() == kRequestVoteError; }

  /** Returns true if the status indicates an AppendEntries error. */
  bool IsAppendEntriesError() const { return code() == kAppendEntriesError; }

  /** Returns true if the status indicates an InstallSnapshot error. */
  bool IsInstallSnapshotError() const {
    return code() == kInstallSnapshotError;
  }

  /** Returns true if the status indicates a DeSerialize error. */
  bool IsDeSerializeError() const { return code() == kDeSerializeError; }

  /** Returns true if the status indicates a Serialize error. */
  bool IsSerializeError() const { return code() == kSerializeError; }

  /** Returns true if the status indicates a Protocol error. */
  bool IsProtocolError() const { return code() == kProtocolError; }

  /** Returns true if the status indicates a NotLeader error. */
  bool IsNotLeader() const { return code() == kNotLeader; }

  /** Returns true if the status indicates a RaftNodeStart error. */
  bool IsRaftNodeStartError() const { return code() == kRaftNodeStartError; }

  /**
   * Return a string representation of this status.
   * @return "OK" for success, or error code with message
   */
  std::string ToString() const;

  /** Get the error message without code prefix. */
  std::string GetMessage() const;

  /** Get the error code as integer. */
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
    kRaftNodeStartError,
    kGenericError,
    kNotLeader
  };

  Code code() const {
    return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
  }

  Status(Code code, const std::string& msg, const std::string& msg2);
  static const char* CopyState(const char* s);

  // OK status has a null state_. Otherwise, state_ is a new[] array with:
  // state_[0..3] == length of message (little-endian uint32_t)
  // state_[4]    == code (Code enum value)
  // state_[5..]  == message (not null-terminated)
  const char* state_;
};

inline Status::Status(const Status& rhs) {
  state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

inline Status& Status::operator=(const Status& rhs) {
  // Handle both aliasing (this == &rhs) and both being OK
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
