/*
 * flink-core : org.apache.flink.api.common.state.ListStateDescriptor
 *
 * 1.0 ListStateDescriptor.java:33
 *   class ListStateDescriptor<T> extends StateDescriptor<ListState<T>, T>
 *   ListStateDescriptor(String name, TypeSerializer<T> typeSerializer)
 *       -> super(name, typeSerializer, null)   // 无默认值
 *   ListState<T> bind(StateBackend) { return backend.createListState(this); }
 *
 * WindowedStream.java:262 用 new ListStateDescriptor<>("window-contents",
 * serializer) 存窗口内容。
 */
#ifndef FLINK_API_COMMON_STATE_LISTSTATEDESCRIPTOR_H
#define FLINK_API_COMMON_STATE_LISTSTATEDESCRIPTOR_H

#include "flink/api/common/state/ListState.h"
#include "flink/api/common/state/StateDescriptor.h"

namespace flink {
namespace api {
namespace common {
namespace state {

template <class T>
class ListStateDescriptor : public StateDescriptor<ListState<T>, T> {
 public:
  typedef StateDescriptor<ListState<T>, T> Base;

  // 1.0 的 defaultValue 传 null；T() 在这里只是占位，ListState 不读它
  explicit ListStateDescriptor(std::string name)
      : Base(std::move(name), T()) {}
  ListStateDescriptor(std::string name,
                      std::shared_ptr<typename Base::SerializerType> serializer)
      : Base(std::move(name), T(), serializer) {}
};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_LISTSTATEDESCRIPTOR_H
