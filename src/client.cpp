/**
 * @file client.cpp
 * @brief Client implementation
 */

#include "rollingraft/client.h"

#include <algorithm>
#include <assert.h>
#include <atomic>
#include <chrono>
#include <random>
#include <thread>

#include "rollingraft/logger.h"
#include "rollingraft/rpc.h"
#include "client/leader_tracker.h"
#include "client/retry_policy.h"
#include "client/connection_pool.h"

namespace rollingraft {

// ========== ClientResult ==========

ClientResult::ClientResult(const std::string& response) : response_(response) {}

ClientResult::ClientResult(Status error) : error_(std::move(error)) {}

const std::string& ClientResult::value() const {
  // As documented in header: precondition is ok() must be true
  // In debug builds, assert. In release, return empty string.
  assert(ok() && "Called value() on error result");
  return response_;
}

const Status& ClientResult::error() const {
  static Status ok_status = Status::OK();
  if (error_.has_value()) {
    return *error_;
  }
  return ok_status;
}

std::string ClientResult::error_message() const {
  if (error_.has_value()) {
    return error_->ToString();
  }
  return "";
}

// ========== Client::Impl ==========

class Client::Impl {
 public:
  Impl(const std::vector<std::string>& servers, const ClientOptions& options)
      : servers_(servers),
        options_(options),
        client_id_(options.client_id == 0 ? GenerateClientId()
                                          : options.client_id),
        leader_tracker_(options.leader_cache_ttl),
        retry_policy_(options.max_retries,
                      options.initial_retry_delay,
                      options.max_retry_delay,
                      options.retry_backoff_multiplier),
        connection_pool_(options.connect_timeout),
        seq_counter_(0),
        shutdown_(false) {}

  ~Impl() {
    shutdown_ = true;
    // Wait for all async operations to complete
    std::unique_lock<std::mutex> lock(async_mutex_);
    for (auto& thread : async_threads_) {
      if (thread.joinable()) {
        lock.unlock();
        thread.join();
        lock.lock();
      }
    }
  }

  ClientResult Execute(const std::string& command,
                       std::chrono::milliseconds timeout);

  ClientResult Query(const std::string& query,
                     std::chrono::milliseconds timeout);

  void ExecuteAsync(const std::string& command,
                    std::function<void(ClientResult)> callback,
                    std::chrono::milliseconds timeout);

  void RefreshLeader() { leader_tracker_.ClearLeader(); }

  std::string GetLeaderAddr() const {
    auto leader = leader_tracker_.GetLeader();
    return leader.value_or("");
  }

  bool IsHealthy() const {
    for (const auto& server : servers_) {
      if (connection_pool_.IsHealthy(server)) {
        return true;
      }
    }
    return false;
  }

  uint64_t GetClientId() const { return client_id_; }

 private:
  ClientResult DoExecute(const std::string& command,
                         bool read_only,
                         std::chrono::milliseconds timeout);

  ClientResult TryExecuteOnServer(const std::string& server,
                                  const ClientRequest& req,
                                  std::chrono::milliseconds timeout);

  static uint64_t GenerateClientId() {
    static std::atomic<uint64_t> counter{0};
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    // Combine random number with counter for uniqueness
    return dis(gen) + counter.fetch_add(1);
  }

  std::vector<std::string> servers_;
  ClientOptions options_;
  uint64_t client_id_;
  LeaderTracker leader_tracker_;
  RetryPolicy retry_policy_;
  ConnectionPool connection_pool_;
  std::atomic<uint64_t> seq_counter_;

  // Async operation tracking
  std::atomic<bool> shutdown_;
  std::mutex async_mutex_;
  std::vector<std::thread> async_threads_;
};

ClientResult Client::Impl::Execute(const std::string& command,
                                   std::chrono::milliseconds timeout) {
  return DoExecute(command, false, timeout);
}

ClientResult Client::Impl::Query(const std::string& query,
                                 std::chrono::milliseconds timeout) {
  return DoExecute(query, true, timeout);
}

ClientResult Client::Impl::DoExecute(const std::string& command,
                                     bool read_only,
                                     std::chrono::milliseconds timeout) {
  // Build request
  ClientRequest req;
  req.command = command;
  req.client_id = client_id_;
  req.seq = seq_counter_.fetch_add(1);
  req.read_only = read_only;

  // Track overall deadline
  auto deadline = std::chrono::steady_clock::now() + timeout;

  // Try cached leader first
  if (auto leader = leader_tracker_.GetLeader()) {
    auto result = TryExecuteOnServer(*leader, req, timeout);
    if (result.ok()) {
      return result;
    }
    // If failed, clear leader and retry
    leader_tracker_.ClearLeader();
  }

  // Retry loop
  for (int attempt = 0; attempt <= options_.max_retries; ++attempt) {
    // Check if we've exceeded the deadline
    if (std::chrono::steady_clock::now() >= deadline) {
      return ClientResult(Status::Error("Request timeout"));
    }

    // Try each server
    for (const auto& server : servers_) {
      auto result = TryExecuteOnServer(server, req, timeout);

      if (result.ok()) {
        // Success - update leader cache
        leader_tracker_.UpdateLeader(server);
        return result;
      }

      // Check if we got NotLeader with hint
      if (result.has_error()) {
        auto& status = result.error();

        if (status.IsNotLeader()) {
          // Try to extract leader address from error or response
          // For now, clear cache and let next attempt find it
          leader_tracker_.ClearLeader();
        }

        if (!RetryPolicy::IsRetryableError(status)) {
          // Non-retryable error
          return result;
        }
      }

      // Check deadline after each server attempt
      if (std::chrono::steady_clock::now() >= deadline) {
        return ClientResult(Status::Error("Request timeout"));
      }
    }

    // Wait before retry (except on last attempt)
    if (attempt < options_.max_retries) {
      auto delay = retry_policy_.GetDelay(attempt);
      // Don't sleep past the deadline
      auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
          deadline - std::chrono::steady_clock::now());
      if (remaining.count() <= 0) {
        return ClientResult(Status::Error("Request timeout"));
      }
      if (delay > remaining) {
        delay = remaining;
      }
      if (delay.count() > 0) {
        std::this_thread::sleep_for(delay);
      }
    }
  }

  return ClientResult(Status::Error("Max retries exceeded"));
}

ClientResult Client::Impl::TryExecuteOnServer(
    const std::string& server,
    const ClientRequest& req,
    std::chrono::milliseconds /*timeout*/) {
  // Use RpcCall (synchronous)
  // TODO: Pass timeout to RpcCall once it supports configurable timeout
  ClientResponse resp;
  auto status = RpcCall(server, req, resp);

  if (!status.ok()) {
    return ClientResult(status);
  }

  if (resp.success) {
    return ClientResult(resp.response);
  }

  // Command failed on server
  if (!resp.leader_addr.empty()) {
    // Got leader hint
    leader_tracker_.UpdateLeader(resp.leader_addr);
    return ClientResult(Status::NotLeader(resp.leader_id, resp.leader_addr));
  }

  return ClientResult(Status::Error(resp.error));
}

void Client::Impl::ExecuteAsync(const std::string& command,
                                std::function<void(ClientResult)> callback,
                                std::chrono::milliseconds timeout) {
  // Launch in background thread with proper lifecycle management
  std::thread thread([this, command, callback, timeout]() {
    if (shutdown_.load()) {
      return;
    }
    auto result = Execute(command, timeout);
    if (!shutdown_.load()) {
      callback(std::move(result));
    }
  });

  std::lock_guard<std::mutex> lock(async_mutex_);
  // Clean up completed threads first
  async_threads_.erase(
      std::remove_if(async_threads_.begin(), async_threads_.end(),
                     [](const std::thread& t) { return !t.joinable(); }),
      async_threads_.end());
  async_threads_.push_back(std::move(thread));
}

// ========== Client Public API ==========

Client::Client(const std::vector<std::string>& servers)
    : Client(servers, ClientOptions{}) {}

Client::Client(const std::vector<std::string>& servers,
               const ClientOptions& options)
    : impl_(std::make_unique<Impl>(servers, options)) {}

Client::~Client() = default;

Client::Client(Client&&) noexcept = default;
Client& Client::operator=(Client&&) noexcept = default;

ClientResult Client::Execute(const std::string& command,
                             std::chrono::milliseconds timeout) {
  return impl_->Execute(command, timeout);
}

ClientResult Client::Execute(const std::string& command) {
  return Execute(command, std::chrono::milliseconds(5000));
}

ClientResult Client::Query(const std::string& query,
                           std::chrono::milliseconds timeout) {
  return impl_->Query(query, timeout);
}

ClientResult Client::Query(const std::string& query) {
  return Query(query, std::chrono::milliseconds(5000));
}

void Client::ExecuteAsync(const std::string& command,
                          std::function<void(ClientResult)> callback,
                          std::chrono::milliseconds timeout) {
  impl_->ExecuteAsync(command, std::move(callback), timeout);
}

void Client::ExecuteAsync(const std::string& command,
                          std::function<void(ClientResult)> callback) {
  ExecuteAsync(command, std::move(callback), std::chrono::milliseconds(5000));
}

void Client::RefreshLeader() { impl_->RefreshLeader(); }

std::string Client::GetLeaderAddr() const { return impl_->GetLeaderAddr(); }

bool Client::IsHealthy() const { return impl_->IsHealthy(); }

uint64_t Client::GetClientId() const { return impl_->GetClientId(); }

}  // namespace rollingraft
