/*
 * flink-core : org.apache.flink.api.common.state.ValueState
 * 1.0 ValueState.java:
 *   T value() throws IOException          当前 key 下的值，未设置时返回
 *                                         StateDescriptor 的默认值
 *   void update(T value) throws IOException   覆盖写；传 null 等价于 clear()
 *
 * 【偏差 —— 新增 hasValue()】1.0 里「没有值」和「值是 null」是同一件事，
 * 于是 StreamGroupedReduce.processElement 直接写
 *     IN currentValue = values.value();
 *     if (currentValue != null) { ...reduce... } else { ...首个元素... }
 * StreamGroupedFold 同理（两者建 descriptor 时默认值都传 null）。
 * C++ 的 T 没有 null 这个值，默认值也必须是个真实的 T，所以判不出首元素。
 * 这里补一个 hasValue()：真正表达「当前 key 下有没有写过值」，让那两个算子的
 * 分支逻辑能原样复刻。1.0 无此方法。
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

  // 【偏差】见文件头：代替 1.0 的 `value() != null` 判定
  virtual bool hasValue() const = 0;
};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_VALUESTATE_H
