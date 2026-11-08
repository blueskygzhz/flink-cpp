/*
 * flink-core : org.apache.flink.api.common.state.ValueState
 * 1.0 ValueState.java:
 *   T value()            当前 key 下的值，未设置时返回 StateDescriptor 的默认值
 *   void update(T value) 覆盖写
 */
#ifndef FLINK_API_COMMON_STATE_VALUESTATE_H
#define FLINK_API_COMMON_STATE_VALUESTATE_H

#include "flink/api/common/state/State.h"

namespace flink {
namespace api {
namespace common {
namespace state {

template <class T>
class ValueState : public State {
 public:
  virtual T value() = 0;
  virtual void update(const T& value) = 0;
};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_VALUESTATE_H
