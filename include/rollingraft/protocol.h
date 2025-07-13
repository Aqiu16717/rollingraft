#pragma once

namespace rollingraft {

class Protocol {
 public:
  virtual ~Protocol() = default;    
  virtual void Serialize() = 0;
  virtual void DeSerialize() = 0;
};

} // namespace rollingraft
