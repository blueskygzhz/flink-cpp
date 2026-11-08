/*
 * flink-core : org.apache.flink.api.common.state.ValueStateDescriptor
 * 1.0 ValueStateDescriptor<T>(String name, TypeSerializer<T>, T defaultValue)
 *   StreamGroupedReduce 用 new ValueStateDescriptor<>("_op_state", ...) 存滚动结果。
 */
#ifndef FLINK_API_COMMON_STATE_VALUESTATEDESCRIPTOR_H
#define FLINK_API_COMMON_STATE_VALUESTATEDESCRIPTOR_H

#include "flink/api/common/state/StateDescriptor.h"

namespace flink {
namespace api {
namespace common {
namespace state {

template <class T>
class ValueStateDescriptor : public StateDescriptor<T> {
 public:
  ValueStateDescriptor(std::string name, T defaultValue)
      : StateDescriptor<T>(std::move(name), std::move(defaultValue)) {}
  ValueStateDescriptor(
      std::string name, T defaultValue,
      std::shared_ptr<typename StateDescriptor<T>::SerializerType> serializer)
      : StateDescriptor<T>(std::move(name), std::move(defaultValue),
                           serializer) {}
};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_VALUESTATEDESCRIPTOR_H
