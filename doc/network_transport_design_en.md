# NetworkTransport Design Document

> **RollingRaft Network Transport Layer Design**  
> **Version**: v1.0  
> **Date**: 2026-03-26

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture Design](#2-architecture-design)
3. [Interface Design](#3-interface-design)
4. [AsioNetworkTransport Implementation](#4-asionetworktransport-implementation)
5. [Protocol and Message Format](#5-protocol-and-message-format)
6. [Connection Management](#6-connection-management)
7. [Threading Model](#7-threading-model)
8. [Error Handling](#8-error-handling)
9. [Performance Optimization](#9-performance-optimization)
10. [Extension Guide](#10-extension-guide)

---

## 1. Overview

### 1.1 Design Goals

NetworkTransport is the network transport layer abstraction for RollingRaft, responsible for:

- **RPC Communication**: RPC calls between nodes such as RequestVote, AppendEntries, and InstallSnapshot
- **Connection Management**: Maintaining TCP connections with other nodes with connection reuse support
- **Asynchronous Non-blocking**: Full async IO based on Asio to avoid blocking the Raft state machine
- **Replaceability**: Abstract interface design supporting replacement with other network implementations (e.g., libuv, RDMA)

### 1.2 Design Constraints

| Constraint | Description |
|------------|-------------|
| Thread Safety | All public methods are thread-safe and can be called from any thread |
| Non-blocking | Network IO is fully asynchronous, non-blocking for calling threads |
| Reliable Transport | Based on TCP, ensures reliable message delivery |
| Timeout Control | Supports RPC call timeout mechanism |

---

## 2. Architecture Design

### 2.1 Class Hierarchy

```
┌─────────────────────────────────────────┐
│         NetworkTransport (Abstract)     │
│  - Pure virtual interface defining      │
│    network layer contract               │
│  - Decoupled from specific              │
│    implementations, supports Mock       │
│    testing                              │
└─────────────────┬───────────────────────┘
                  │
          ┌───────┴───────┐
          ▼               ▼
┌─────────────────┐  ┌─────────────────┐
│AsioNetwork      │  │ Other           │
│Transport        │  │ Implementations │
│ (Asio-based)    │  │ (libuv/RDMA/..) │
└─────────────────┘  └─────────────────┘
```

### 2.2 Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| NetworkTransport | Defines network layer abstract interface |
| AsioNetworkTransport | Default Asio-based implementation |
| Session | Manages lifecycle of a single TCP connection |
| RpcRequestHandler | Callback for handling received RPC requests |
| RpcResponseCallback | Callback for handling RPC responses |

### 2.3 Position in Raft Architecture

```
┌─────────────────────────────────────────┐
│           RaftNode::Impl                │
│  ┌─────────────────────────────────┐   │
│  │      Raft Core Implementation   │   │
│  │  (Election, Log Replication,    │   │
│  │   Snapshot)                     │   │
│  └─────────────┬───────────────────┘   │
│                │ SendRpc / Handler      │
│                ▼                        │
│  ┌─────────────────────────────────┐   │
│  │    NetworkTransport             │   │
│  │  ┌─────────┐    ┌─────────┐    │   │
│  │  │  Server │    │ Client  │    │   │
│  │  │(Listener│    │(Connect │    │   │
│  │  │  ...)   │    │  ...)   │    │   │
│  │  └─────────┘    └─────────┘    │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

---

## 3. Interface Design

### 3.1 Core Interface

```cpp
// include/rollingraft/network_transport.h
#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <chrono>

#include "rollingraft/status.h"

namespace rollingraft {

using RaftNodeId = int32_t;
using NodeAddr = std::string;

// RPC response callback
// - response_data: Response data (serialized)
// - success: Whether the call succeeded
// - error_msg: Error message (if failed)
using RpcResponseCallback = std::function<void(
    const std::string& response_data,
    bool success,
    const std::string& error_msg)>;

// RPC request handler
// - from: Source node ID
// - request_data: Request data (serialized)
// - response_data: Output response data (needs serialization)
using RpcRequestHandler = std::function<void(
    RaftNodeId from,
    const std::string& request_data,
    std::string& response_data)>;

// Connection state callback
// - peer_id: Peer node ID
// - addr: Peer address
// - connected: true=connected, false=disconnected
using ConnectionCallback = std::function<void(
    RaftNodeId peer_id,
    const NodeAddr& addr,
    bool connected)>;

class NetworkTransport {
 public:
  virtual ~NetworkTransport() = default;

  // Initialize transport layer
  // @param listen_addr: Listen address (e.g., "0.0.0.0:8001")
  // @param handler: Callback for handling received requests
  // @return Status::OK() indicates success
  virtual Status Initialize(const NodeAddr& listen_addr,
                            RpcRequestHandler handler) = 0;

  // Set connection state callback (optional)
  virtual void SetConnectionCallback(ConnectionCallback callback) = 0;

  // Start transport layer, begin listening
  virtual Status Start() = 0;

  // Stop transport layer, close all connections
  virtual Status Stop() = 0;

  // Send RPC request (async)
  // @param to: Target node ID
  // @param addr: Target address
  // @param request_data: Request data (serialized)
  // @param timeout: Timeout duration
  // @param callback: Response callback (executed in IO thread)
  virtual void SendRpc(RaftNodeId to,
                       const NodeAddr& addr,
                       const std::string& request_data,
                       std::chrono::milliseconds timeout,
                       RpcResponseCallback callback) = 0;
};

}  // namespace rollingraft
```

### 3.2 Interface Usage Example

```cpp
// 1. Create and initialize
auto transport = std::make_unique<AsioNetworkTransport>();

// 2. Set request handler (implemented by RaftNode)
auto handler = [](RaftNodeId from, const std::string& req, std::string& resp) {
    // Parse request
    // Process request
    // Serialize response to resp
};
transport->Initialize("0.0.0.0:8001", handler);

// 3. Start
transport->Start();

// 4. Send RPC (async)
transport->SendRpc(2, "192.168.1.2:8001", request_data,
    std::chrono::milliseconds(1000),
    [](const std::string& resp, bool success, const std::string& err) {
        if (success) {
            // Handle response
        } else {
            // Handle error
        }
    });

// 5. Stop
transport->Stop();
```

---

## 4. AsioNetworkTransport Implementation

### 4.1 Class Design

```cpp
// src/asio/asio_network_transport.h
#pragma once

#include "rollingraft/network_transport.h"
#include <asio.hpp>
#include <thread>
#include <atomic>
#include <set>
#include <mutex>

namespace rollingraft {

class AsioNetworkTransport : public NetworkTransport {
 public:
  AsioNetworkTransport();
  ~AsioNetworkTransport() override;

  Status Initialize(const NodeAddr& listen_addr,
                    RpcRequestHandler handler) override;
  void SetConnectionCallback(ConnectionCallback callback) override;
  Status Start() override;
  Status Stop() override;
  void SendRpc(RaftNodeId to, const NodeAddr& addr,
               const std::string& request_data,
               std::chrono::milliseconds timeout,
               RpcResponseCallback callback) override;

 private:
  // Internal Session class managing single TCP connection
  class Session;
  
  // Asio core components
  asio::io_context io_context_;
  std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
  std::thread io_thread_;
  std::atomic<bool> running_{false};
  
  // Callbacks
  RpcRequestHandler request_handler_;
  ConnectionCallback connection_callback_;
  
  // Active session management
  std::mutex sessions_mutex_;
  std::set<std::shared_ptr<Session>> sessions_;
  
  void DoAccept();
  void RemoveSession(std::shared_ptr<Session> session);
};

// Session implementation
class AsioNetworkTransport::Session 
    : public std::enable_shared_from_this<Session> {
 public:
  Session(asio::ip::tcp::socket socket, 
          RpcRequestHandler handler,
          std::function<void(std::shared_ptr<Session>)> on_close);
  
  void Start();
  void Write(const std::string& data);
  void Close();

 private:
  asio::ip::tcp::socket socket_;
  RpcRequestHandler request_handler_;
  std::function<void(std::shared_ptr<Session>)> on_close_;
  
  // Read/write buffers
  std::array<char, 4096> read_buffer_;
  std::vector<char> write_buffer_;
  
  // Message parsing state
  uint32_t expected_length_ = 0;
  std::vector<char> message_buffer_;
  
  void DoRead();
  void DoWrite();
  void ProcessMessage(const std::vector<char>& data);
};

}  // namespace rollingraft
```

### 4.2 Key Implementation Details

#### 4.2.1 Lifecycle Management

```cpp
AsioNetworkTransport::AsioNetworkTransport() = default;

AsioNetworkTransport::~AsioNetworkTransport() {
  if (running_) {
    Stop();
  }
}

Status AsioNetworkTransport::Start() {
  if (running_.exchange(true)) {
    return Status::Error("Already started");
  }
  
  // Start accepting connections
  DoAccept();
  
  // Start IO thread
  io_thread_ = std::thread([this]() {
    try {
      io_context_.run();
    } catch (const std::exception& e) {
      LOG_ERROR("IO context error: {}", e.what());
    }
  });
  
  return Status::OK();
}

Status AsioNetworkTransport::Stop() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }
  
  // Stop io_context
  io_context_.stop();
  
  // Wait for IO thread to finish
  if (io_thread_.joinable()) {
    io_thread_.join();
  }
  
  // Close all sessions
  {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    for (auto& session : sessions_) {
      session->Close();
    }
    sessions_.clear();
  }
  
  return Status::OK();
}
```

#### 4.2.2 Connection Acceptance

```cpp
void AsioNetworkTransport::DoAccept() {
  acceptor_->async_accept(
      [this](std::error_code ec, asio::ip::tcp::socket socket) {
        if (!ec) {
          // Create new session
          auto session = std::make_shared<Session>(
              std::move(socket),
              request_handler_,
              [this](auto s) { RemoveSession(s); });
          
          {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            sessions_.insert(session);
          }
          
          session->Start();
          
          // Notify connection established
          if (connection_callback_) {
            connection_callback_(0, socket.remote_endpoint().address().to_string(), true);
          }
        } else if (running_) {
          LOG_ERROR("Accept error: {}", ec.message());
        }
        
        // Continue accepting next connection
        if (running_) {
          DoAccept();
        }
      });
}
```

#### 4.2.3 Client RPC Sending

```cpp
void AsioNetworkTransport::SendRpc(
    RaftNodeId to, const NodeAddr& addr,
    const std::string& request_data,
    std::chrono::milliseconds timeout,
    RpcResponseCallback callback) {
  
  auto socket = std::make_shared<asio::ip::tcp::socket>(io_context_);
  
  // Parse address
  auto pos = addr.find(':');
  if (pos == std::string::npos) {
    callback("", false, "Invalid address format");
    return;
  }
  
  std::string ip = addr.substr(0, pos);
  uint16_t port = std::stoi(addr.substr(pos + 1));
  
  asio::ip::tcp::endpoint endpoint(asio::ip::make_address(ip), port);
  
  // Set timeout timer
  auto timer = std::make_shared<asio::steady_timer>(io_context_);
  timer->expires_after(timeout);
  timer->async_wait([socket, callback](std::error_code ec) {
    if (!ec) {
      socket->close();
      callback("", false, "Request timeout");
    }
  });
  
  // Async connect
  socket->async_connect(endpoint,
      [this, socket, timer, request_data, callback](std::error_code ec) {
        if (ec) {
          timer->cancel();
          callback("", false, ec.message());
          return;
        }
        
        // Send length prefix + data
        uint32_t len = request_data.size();
        std::vector<char> header(sizeof(len));
        memcpy(header.data(), &len, sizeof(len));
        
        auto write_buf = std::make_shared<std::vector<char>>();
        write_buf->insert(write_buf->end(), header.begin(), header.end());
        write_buf->insert(write_buf->end(), request_data.begin(), 
                         request_data.end());
        
        asio::async_write(*socket, asio::buffer(*write_buf),
            [socket, timer, callback](std::error_code ec, size_t) {
              if (ec) {
                timer->cancel();
                callback("", false, ec.message());
                return;
              }
              
              // Read response length
              auto header_buf = std::make_shared<std::array<char, 4>>();
              asio::async_read(*socket, asio::buffer(*header_buf),
                  [socket, timer, header_buf, callback](std::error_code ec, size_t) {
                    if (ec) {
                      timer->cancel();
                      callback("", false, ec.message());
                      return;
                    }
                    
                    uint32_t len;
                    memcpy(&len, header_buf->data(), sizeof(len));
                    
                    // Read response body
                    auto body_buf = std::make_shared<std::vector<char>>(len);
                    asio::async_read(*socket, asio::buffer(*body_buf),
                        [socket, timer, body_buf, callback](std::error_code ec, size_t) {
                          timer->cancel();
                          if (ec) {
                            callback("", false, ec.message());
                          } else {
                            callback(std::string(body_buf->begin(), 
                                                body_buf->end()), 
                                    true, "");
                          }
                          // Close connection
                          std::error_code close_ec;
                          socket->close(close_ec);
                        });
                  });
            });
      });
}
```

---

## 5. Protocol and Message Format

### 5.1 Length-Prefixed Protocol

Using a simple length-prefixed protocol to solve TCP sticky packet issues:

```
┌─────────────────┬─────────────────┐
│   Length (4B)   │     Payload     │
│  (uint32_t, BE) │  (JSON/Binary)  │
└─────────────────┴─────────────────┘
```

- **Length**: 4-byte unsigned integer, indicates Payload length (big-endian)
- **Payload**: Actual message data, currently using JSON serialization

### 5.2 Message Parsing Flow

```cpp
void Session::DoRead() {
  auto self = shared_from_this();
  
  if (expected_length_ == 0) {
    // Read length prefix
    asio::async_read(socket_, asio::buffer(&expected_length_, sizeof(expected_length_)),
        [this, self](std::error_code ec, size_t) {
          if (ec) {
            if (ec != asio::error::eof) {
              LOG_ERROR("Read header error: {}", ec.message());
            }
            Close();
            return;
          }
          
          // Convert byte order (network -> host)
          expected_length_ = ntohl(expected_length_);
          if (expected_length_ > k_max_message_size) {
            LOG_ERROR("Message too large: {}", expected_length_);
            Close();
            return;
          }
          
          message_buffer_.resize(expected_length_);
          DoRead();  // Continue reading message body
        });
  } else {
    // Read message body
    asio::async_read(socket_, asio::buffer(message_buffer_),
        [this, self](std::error_code ec, size_t) {
          if (ec) {
            LOG_ERROR("Read body error: {}", ec.message());
            Close();
            return;
          }
          
          // Process complete message
          ProcessMessage(message_buffer_);
          
          // Reset state, prepare for next message
          expected_length_ = 0;
          message_buffer_.clear();
          DoRead();
        });
  }
}
```

---

## 6. Connection Management

### 6.1 Current Design: Short-Connection Mode

The current implementation uses **short-connection mode**, establishing a new connection for each RPC call:

**Advantages**:
- Simple implementation, no connection pool management needed
- No connection state synchronization issues
- Natural load balancing

**Disadvantages**:
- TCP handshake overhead for each RPC
- High concurrency generates many TIME_WAIT connections

### 6.2 Future Optimization: Connection Pool

Consider implementing connection pool optimization:

```cpp
class ConnectionPool {
 public:
  // Get or create connection to specified address
  std::shared_ptr<Connection> Acquire(const NodeAddr& addr);
  
  // Return connection
  void Release(const NodeAddr& addr, std::shared_ptr<Connection> conn);
  
  // Periodic cleanup of idle connections
  void CleanupIdleConnections();
  
 private:
  struct PoolEntry {
    std::shared_ptr<Connection> conn;
    std::chrono::steady_clock::time_point last_used;
    bool in_use;
  };
  
  std::mutex mutex_;
  std::unordered_map<NodeAddr, std::vector<PoolEntry>> pools_;
};
```

### 6.3 Session Management

```cpp
// Server-side session management
void AsioNetworkTransport::RemoveSession(std::shared_ptr<Session> session) {
  std::lock_guard<std::mutex> lock(sessions_mutex_);
  sessions_.erase(session);
  
  if (connection_callback_) {
    connection_callback_(0, "", false);
  }
}

// Session lifecycle
void Session::Close() {
  std::error_code ec;
  socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
  socket_.close(ec);
  if (on_close_) {
    on_close_(shared_from_this());
  }
}
```

---

## 7. Threading Model

### 7.1 Thread Responsibilities

| Thread | Responsibility | Callback Execution |
|--------|----------------|-------------------|
| Main Thread | User calls SendRpc | No |
| IO Thread | Network events, timers | Yes |

### 7.2 Thread Safety Guarantees

```cpp
// NetworkTransport guarantees the following thread safety:
// 1. SendRpc can be called from any thread
// 2. Initialize/Start/Stop must be called sequentially
// 3. Handler callbacks execute in IO thread, user needs to lock

class RaftNode::Impl {
  void HandleIncomingRpc(RaftNodeId from, const std::string& req, std::string& resp) {
    // IO thread execution, needs locking
    std::lock_guard<std::mutex> lock(mutex_);
    // Process request...
  }
};
```

### 7.3 Callback Execution Strategy

All callbacks execute in the Asio IO thread:

```cpp
// User must ensure callbacks don't block IO thread
// If long processing needed, dispatch to other threads

transport->SendRpc(peer, addr, data, timeout,
    [this](const std::string& resp, bool success, const std::string& err) {
      // Executed in IO thread
      // Fast processing or dispatch to worker thread
      
      // Option 1: Direct processing (if fast)
      HandleResponse(resp);
      
      // Option 2: Dispatch to thread pool (if time-consuming)
      thread_pool_.Post([resp]() {
        HeavyProcessing(resp);
      });
    });
```

---

## 8. Error Handling

### 8.1 Error Categories

| Error Type | Examples | Handling |
|------------|----------|----------|
| Network Error | Connection refused, Connection reset | Callback immediately with error |
| Timeout Error | RPC timeout | Callback with timeout error |
| Protocol Error | Message too large, Format error | Close connection, log error |
| System Error | Out of memory, fd exhaustion | Log error, attempt recovery |

### 8.2 Error Code Mapping

```cpp
// Status error codes
Status::NetworkError("Connection refused");    // Connection failed
Status::NetworkError("Connection reset");      // Connection reset
Status::TimeoutError("Request timeout");       // Timeout
Status::ProtocolError("Message too large");    // Protocol error
```

### 8.3 Timeout Handling

```cpp
void AsioNetworkTransport::SendRpc(...) {
  auto timer = std::make_shared<asio::steady_timer>(io_context_);
  timer->expires_after(timeout);
  
  // Timeout callback
  timer->async_wait([socket, callback](std::error_code ec) {
    if (!ec) {
      // Timeout occurred
      socket->close();
      callback("", false, "Request timeout");
    }
  });
  
  // Cancel timer on normal response
  // ...
  timer->cancel();
}
```

---

## 9. Performance Optimization

### 9.1 Current Optimizations

| Optimization | Implementation |
|--------------|----------------|
| Async IO | Full async based on Asio, no blocking |
| Zero Copy | Use asio::buffer to avoid copying |
| Buffer Reuse | Session uses fixed-size read_buffer |

### 9.2 Future Optimizations

| Optimization | Plan | Priority |
|--------------|------|----------|
| Connection Pool | Maintain long connections, reduce handshake overhead | P1 |
| Batch Sending | Leader batch sends AppendEntries | P1 |
| Zero-Copy Send | Use sendfile for snapshot transfer | P2 |
| Multi-IO Thread | Multi-threaded io_context for higher throughput | P2 |

### 9.3 Batch Sending Optimization

```cpp
// Leader batch processing for sending
void RaftNode::Impl::BroadcastAppendEntriesLocked() {
  // Collect nodes needing send
  std::vector<NodeId> peers_to_send;
  for (const auto& peer : config_.peers) {
    if (NeedSend(peer)) {
      peers_to_send.push_back(peer);
    }
  }
  
  // Serialize once, reuse
  std::string serialized_data;
  protocol_->SerializeRequest(request, serialized_data);
  
  // Concurrent send
  for (auto peer : peers_to_send) {
    network_->SendRpc(peer, addr, serialized_data, timeout, callback);
  }
}
```

---

## 10. Extension Guide

### 10.1 Implementing Custom NetworkTransport

```cpp
// Example: libuv-based implementation
class LibuvNetworkTransport : public NetworkTransport {
 public:
  Status Initialize(const NodeAddr& listen_addr,
                    RpcRequestHandler handler) override {
    // Initialize libuv
    loop_ = uv_default_loop();
    handler_ = handler;
    // ...
    return Status::OK();
  }
  
  void SendRpc(RaftNodeId to, const NodeAddr& addr,
               const std::string& request_data,
               std::chrono::milliseconds timeout,
               RpcResponseCallback callback) override {
    // Send using libuv
    // ...
  }
  
  // ... other method implementations
  
 private:
  uv_loop_t* loop_;
  RpcRequestHandler handler_;
};
```

### 10.2 Mock Implementation for Testing

```cpp
// Mock implementation for unit testing
class MockNetworkTransport : public NetworkTransport {
 public:
  Status Initialize(const NodeAddr&, RpcRequestHandler handler) override {
    handler_ = handler;
    return Status::OK();
  }
  
  void SendRpc(RaftNodeId to, const NodeAddr& addr,
               const std::string& request_data,
               std::chrono::milliseconds timeout,
               RpcResponseCallback callback) override {
    // Record call
    send_calls_.push_back({to, addr, request_data});
    
    // Simulate delay then callback
    if (simulate_delay_) {
      std::this_thread::sleep_for(simulate_delay_.value());
    }
    
    // Simulate response
    if (response_callback_) {
      response_callback_(to, request_data, callback);
    }
  }
  
  // Test helper methods
  void SetResponse(std::function<void(RaftNodeId, const std::string&, 
                                       RpcResponseCallback)> cb) {
    response_callback_ = cb;
  }
  
  void SimulatePartition(RaftNodeId node) {
    // Simulate network partition
  }
  
 private:
  RpcRequestHandler handler_;
  std::vector<SendCall> send_calls_;
  std::optional<std::chrono::milliseconds> simulate_delay_;
  std::function<void(RaftNodeId, const std::string&, RpcResponseCallback)> response_callback_;
};
```

### 10.3 Configuration Usage

```cpp
RaftNodeConfig config;
config.node_id = 1;
config.listen_addr = "0.0.0.0:8001";
config.peers = {"192.168.1.2:8001", "192.168.1.3:8001"};

// Use custom Transport
config.network_factory = []() {
  return std::make_unique<LibuvNetworkTransport>();
};

auto node = RaftNode::Create(config, state_machine);
```

---

## Summary

NetworkTransport as RollingRaft's network abstraction layer:

1. **Simple Interface**: Only 5 pure virtual methods, easy to understand and implement
2. **Fully Asynchronous**: Asio-based implementation ensuring high performance
3. **Testable**: Abstract design supports Mock for easy unit testing
4. **Extensible**: Supports replacement with other network implementations

The default AsioNetworkTransport provides a production-ready TCP transport implementation suitable for most scenarios. Future optimizations like connection pooling and batch sending can further improve performance.
