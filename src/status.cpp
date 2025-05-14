#include "rolllingraft/status.h"
#include <cstdio>

namespace rollingraft {

const char* Status::CopyState(const char* state) {
    uint32_t size;
    std::memcpy(&size, state, sizeof(size));
    char* result = new char[size + 5];
    std::memcpy(result, state, size + 5);
    return result;
}

std::string Status::ToString() const {
    if (state_ == nullptr) {
        return "OK";
    } else {
        char tmp[30];
        const char* type;
        switch (code()) {
            case kOk:
                type = "OK";
                break;
            default:
                std::snprintf(tmp, sizeof(tmp),
                              "Unknown code(%d): ", static_cast<int>(code()));
                type = tmp;
                break;
        }
        std::string result(type);
        uint32_t length;
        std::memcpy(&length, state_, sizeof(length));
        result.append(state_ + 5, length);
        return result;
    }
}  // namespace rollingraft