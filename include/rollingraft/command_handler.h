#pragma once

#include <string>

namespace rollingraft {

class CommandHandler {
 public:
  virtual ~CommandHandler() = default;
  virtual void HandleCommand(const std::string& request,
                             std::string& response) = 0;
};

}  // namespace rollingraft
