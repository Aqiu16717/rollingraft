#pragma once

#include <string>
#include <vector>
#include <cstdint>

namespace rollingraft {

struct LogEntry;

class Log {
public:
    Log() = default;
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

} // namespace RollingRaft
