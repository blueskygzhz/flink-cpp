/*
 * flink-core : org.apache.flink.api.common.state.StateDescriptor
 *
 * 1.0 StateDescriptor<S extends State, T>:
 *   String name; TypeSerializer<T> serializer; T defaultValue;
 *   getName() / getSerializer() / getDefaultValue()
 *   bind(StateBackend) 由各子类实现（1.0 用它向后端申请具体 State 实例）
 */
#ifndef FLINK_API_COMMON_STATE_STATEDESCRIPTOR_H
#define FLINK_API_COMMON_STATE_STATEDESCRIPTOR_H

#include <memory>
#include <string>
#include <utility>

#include "flink/api/common/typeutils/TypeSerializer.h"

namespace flink {
namespace api {
namespace common {
namespace state {

template <class T>
class StateDescriptor {
 public:
  typedef flink::api::common::typeutils::TypeSerializer<T> SerializerType;

  StateDescriptor(std::string name, T defaultValue)
      : name_(std::move(name)), defaultValue_(std::move(defaultValue)) {}
  StateDescriptor(std::string name, T defaultValue,
                  std::shared_ptr<SerializerType> serializer)
      : name_(std::move(name)),
        defaultValue_(std::move(defaultValue)),
        serializer_(serializer) {}
  virtual ~StateDescriptor() {}

  const std::string& getName() const { return name_; }
  const T& getDefaultValue() const { return defaultValue_; }
  const std::shared_ptr<SerializerType>& getSerializer() const {
    return serializer_;
  }

 private:
  std::string name_;
  T defaultValue_;
  std::shared_ptr<SerializerType> serializer_;
};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_STATEDESCRIPTOR_H
