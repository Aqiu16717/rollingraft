#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include "rollingraft/status.h"

namespace rollingraft {

struct LogEntry;

class Log {
public:
    Log() = default;
    Status AppendLogEntry(const LogEntry& log_entry) {
        entries_.push_back(log_entry);
        return Status();
    }
    uint32_t LastLogIndex() const;
    uint32_t LastLogTerm() const;
private:
    std::vector<LogEntry> entries_;
};

struct LogEntry {
    LogEntry() = default;
    LogEntry(uint32_t index, uint32_t term, const std::string& command)
        : index_(index), term_(term), command_(command) {}

    uint32_t index_;
    uint32_t term_;
    std::string data_;
    std::string command_;
};

} // namespace rollingraft
