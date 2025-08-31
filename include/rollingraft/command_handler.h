#pragma once

#include <string>
#include "rollingraft/status.h"

namespace rollingraft {

class CommandHandler {
 public:
  virtual ~CommandHandler() = default;
  virtual Status HandleCommand(const std::string& request,
                             std::string& response) = 0;
};

}  // namespace rollingraft
