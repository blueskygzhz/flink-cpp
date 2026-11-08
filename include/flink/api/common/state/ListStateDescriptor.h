/*
 * flink-core : org.apache.flink.api.common.state.ListStateDescriptor
 * 1.0 ListStateDescriptor<T>(String name, TypeSerializer<T>)：无默认值（默认空列表）。
 *   WindowedStream.java:262 用 new ListStateDescriptor<>("window-contents", ...)
 *   存窗口内容。
 */
#ifndef FLINK_API_COMMON_STATE_LISTSTATEDESCRIPTOR_H
#define FLINK_API_COMMON_STATE_LISTSTATEDESCRIPTOR_H

#include "flink/api/common/state/StateDescriptor.h"

namespace flink {
namespace api {
namespace common {
namespace state {

template <class T>
class ListStateDescriptor : public StateDescriptor<T> {
 public:
  explicit ListStateDescriptor(std::string name)
      : StateDescriptor<T>(std::move(name), T()) {}
  ListStateDescriptor(
      std::string name,
      std::shared_ptr<typename StateDescriptor<T>::SerializerType> serializer)
      : StateDescriptor<T>(std::move(name), T(), serializer) {}
};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_LISTSTATEDESCRIPTOR_H
