/*
 * flink-runtime : org.apache.flink.runtime.state.memory.MemValueState
 *   （继承链 MemValueState -> AbstractMemState -> AbstractHeapState）
 *
 * 1.0 MemValueState<K, N, V> implements ValueState<V>：
 *   V value()          —— 取不到时返回 stateDesc.getDefaultValue()
 *   void update(V)     —— value == null 时等价于 clear()
 *   stateSerializer 就是 stateDesc.getSerializer()
 *
 * 【偏差】value() 取不到时这里返回 isNull() 的 Any 而不是默认值：默认值的类型只有
 * 编译期的算子模板（ValueStateDescriptor<T>）才知道，所以由算子侧的模板包装器填。
 * 被 StreamGroupedReduce / StreamGroupedFold 用来存滚动聚合结果。
 */
#ifndef FLINK_RUNTIME_STATE_HEAPVALUESTATE_H
#define FLINK_RUNTIME_STATE_HEAPVALUESTATE_H

#include "flink/runtime/state/AbstractHeapState.h"

namespace flink {
namespace runtime {
namespace state {

class HeapValueState : public AbstractHeapState<flink::types::Any> {
 public:
  HeapValueState(KvState::SerializerPtr keySerializer,
                 KvState::SerializerPtr namespaceSerializer,
                 KvState::SerializerPtr valueSerializer)
      : AbstractHeapState<flink::types::Any>(keySerializer, namespaceSerializer,
                                             valueSerializer) {}

  // 1.0 MemValueState.value()
  flink::types::Any value() const override {
    const NamespaceMap* nsState = currentNSStateForRead();
    if (nsState == NULL) return flink::types::Any();
    auto it = nsState->find(currentKey_);
    if (it == nsState->end()) return flink::types::Any();
    return it->second;
  }

  // 1.0 MemValueState.update(V)
  void update(const flink::types::Any& value) override {
    if (value.isNull()) {
      clear();
      return;
    }
    currentNSStateForWrite()[currentKey_] = value;
  }

 protected:
  void serializeStateValue(
      const flink::types::Any& value,
      flink::core::memory::DataOutputView& target) const override {
    valueSerializer_->serialize(value, target);
  }

  flink::types::Any deserializeStateValue(
      flink::core::memory::DataInputView& source) const override {
    return valueSerializer_->deserialize(source);
  }
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_HEAPVALUESTATE_H
