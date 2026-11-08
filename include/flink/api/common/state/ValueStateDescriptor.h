/*
 * flink-core : org.apache.flink.api.common.state.ValueStateDescriptor
 *
 * 1.0 ValueStateDescriptor.java:33
 *   class ValueStateDescriptor<T> extends StateDescriptor<ValueState<T>, T>
 *   ValueStateDescriptor(String name, TypeSerializer<T> typeSerializer,
 *                        T defaultValue)
 *   ValueState<T> bind(StateBackend) { return backend.createValueState(this); }
 *
 * StreamGroupedReduce.java:48 用 new ValueStateDescriptor<>("_op_state",
 * serializer, null) 存滚动聚合结果。
 */
#ifndef FLINK_API_COMMON_STATE_VALUESTATEDESCRIPTOR_H
#define FLINK_API_COMMON_STATE_VALUESTATEDESCRIPTOR_H

#include "flink/api/common/state/StateDescriptor.h"
#include "flink/api/common/state/ValueState.h"

namespace flink {
namespace api {
namespace common {
namespace state {

template <class T>
class ValueStateDescriptor : public StateDescriptor<ValueState<T>, T> {
 public:
  typedef StateDescriptor<ValueState<T>, T> Base;

  ValueStateDescriptor(std::string name, T defaultValue)
      : Base(std::move(name), std::move(defaultValue)) {}
  ValueStateDescriptor(
      std::string name, T defaultValue,
      std::shared_ptr<typename Base::SerializerType> serializer)
      : Base(std::move(name), std::move(defaultValue), serializer) {}
};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_VALUESTATEDESCRIPTOR_H
