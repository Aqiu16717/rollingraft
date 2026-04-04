/**
 * @file types.h
 * @brief Core type definitions for RollingRaft
 *
 * Provides type aliases for Raft concepts:
 * - NodeId: Unique node identifier
 * - NodeAddr: Network address (string)
 * - Term: Raft term number
 * - Index: Log entry index
 */

#pragma once

#include <cstdint>
#include <string>

namespace rollingraft {

/** Unique identifier for a Raft node. */
using NodeId = int32_t;

/** Network address (e.g., "127.0.0.1:8001"). */
using NodeAddr = std::string;

/** Raft term (monotonically increasing epoch). */
using Term = uint32_t;

/** Log entry index (1-based, monotonically increasing per log). */
using Index = uint32_t;

}  // namespace rollingraft
