/*
 * 【与 Flink 1.0 的偏差 —— 新增类，1.0 无对应物】
 *
 * 1.0 的 MemValueState<K, N, V> 同时是 KvState（运行时／后端视角，类型已擦除）
 * 和 ValueState<V>（用户／算子视角，带类型）—— 一个类两副面孔，靠泛型擦除实现。
 * C++ 做不到：运行期流转的是 Any，编译期才知道 V。所以把两副面孔拆开：
 *   KvState（AbstractHeapState 一族）  —— 存 Any，负责快照
 *   TypedValueState<T>（本类）        —— 薄包装，装箱/拆箱 + 填默认值
 *
 * 默认值由这里填，对应 1.0 MemValueState.value() 里的
 * `return value != null ? value : stateDesc.getDefaultValue();`
 */
#ifndef FLINK_RUNTIME_STATE_TYPEDVALUESTATE_H
#define FLINK_RUNTIME_STATE_TYPEDVALUESTATE_H

#include <memory>
#include <utility>

#include "flink/api/common/state/ValueState.h"
#include "flink/runtime/state/KvState.h"

namespace flink {
namespace runtime {
namespace state {

template <class T>
class TypedValueState : public flink::api::common::state::ValueState<T> {
 public:
  TypedValueState(std::shared_ptr<KvState> kvState, T defaultValue)
      : kvState_(kvState), defaultValue_(std::move(defaultValue)) {}

  T value() override {
    flink::types::Any value = kvState_->value();
    return value.isNull() ? defaultValue_ : value.as<T>();
  }

  void update(const T& value) override {
    kvState_->update(flink::types::Any::of(value));
  }

  // 【偏差】见 ValueState.h：1.0 用 `value() != null` 表达同一判定
  bool hasValue() const override { return !kvState_->value().isNull(); }

  void clear() override { kvState_->clear(); }

 private:
  std::shared_ptr<KvState> kvState_;
  T defaultValue_;
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_TYPEDVALUESTATE_H
