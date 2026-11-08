/*
 * flink-runtime : org.apache.flink.runtime.state.memory.MemListState
 *   （继承链 MemListState -> AbstractMemState -> AbstractHeapState）
 *
 * 1.0 MemListState<K, N, V> implements ListState<V>：
 *   SV = ArrayList<V>，每个 (namespace, key) 对应一个列表
 *   Iterable<V> get()  —— 无值时返回 Collections.emptyList()
 *   void add(V)        —— 列表不存在就先建
 *   stateSerializer = new ArrayListSerializer<>(stateDesc.getSerializer())
 *
 * WindowOperator 用它存「窗口内容」，namespace 就是窗口本身
 * （WindowedStream.java:262 的 ListStateDescriptor("window-contents")）。
 */
#ifndef FLINK_RUNTIME_STATE_HEAPLISTSTATE_H
#define FLINK_RUNTIME_STATE_HEAPLISTSTATE_H

#include <vector>

#include "flink/runtime/state/AbstractHeapState.h"

namespace flink {
namespace runtime {
namespace state {

class HeapListState
    : public AbstractHeapState<std::vector<flink::types::Any> > {
 public:
  typedef std::vector<flink::types::Any> ValueList;

  HeapListState(KvState::SerializerPtr keySerializer,
                KvState::SerializerPtr namespaceSerializer,
                KvState::SerializerPtr valueSerializer)
      : AbstractHeapState<ValueList>(keySerializer, namespaceSerializer,
                                     valueSerializer) {}

  // 1.0 MemListState.add(V)
  void add(const flink::types::Any& value) override {
    currentNSStateForWrite()[currentKey_].push_back(value);
  }

  // 1.0 MemListState.get()：无值返回空列表（1.0 是 Collections.emptyList()）
  std::vector<flink::types::Any> get() const override {
    const NamespaceMap* nsState = currentNSStateForRead();
    if (nsState == NULL) return ValueList();
    auto it = nsState->find(currentKey_);
    if (it == nsState->end()) return ValueList();
    return it->second;
  }

 protected:
  // 以下两个钩子即 1.0 ArrayListSerializer 的逻辑
  // （ArrayListSerializer.java:74 / :83）：先写 size，再逐元素
  void serializeStateValue(
      const ValueList& list,
      flink::core::memory::DataOutputView& target) const override {
    target.writeInt(static_cast<int32_t>(list.size()));
    for (const auto& value : list) {
      valueSerializer_->serialize(value, target);
    }
  }

  ValueList deserializeStateValue(
      flink::core::memory::DataInputView& source) const override {
    const int32_t size = source.readInt();
    ValueList list;
    list.reserve(static_cast<size_t>(size));
    for (int32_t i = 0; i < size; ++i) {
      list.push_back(valueSerializer_->deserialize(source));
    }
    return list;
  }
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_HEAPLISTSTATE_H
