/*
 * flink-runtime : org.apache.flink.runtime.state.AbstractHeapState
 *          +     org.apache.flink.runtime.state.memory.AbstractMemState
 *          +     org.apache.flink.runtime.state.memory.AbstractMemStateSnapshot
 *
 * 1.0 AbstractHeapState<K, N, SV, S, SD, Backend>：
 *   protected final HashMap<N, Map<K, SV>> state;   // 两层：namespace -> key
 *   protected Map<K, SV> currentNSState;            // 当前 namespace 的子表缓存
 *   void setCurrentKey(K) / void setCurrentNamespace(N)
 *   final void clear()  —— 删掉当前 (ns, key)，子表空了连 ns 一起删
 *   final int size()    —— 跨 namespace 的条目总数
 *   void dispose()      —— state.clear()
 *
 * 快照字节格式照抄 AbstractMemState.snapshot（1.0 AbstractMemState.java:63）：
 *   [numNamespaces:int]
 *     ( [namespace] [numKeys:int] ( [key][stateValue] ) × numKeys ) × numNS
 * 恢复逻辑照抄 AbstractMemStateSnapshot.restoreState（同文件 :82）。
 *
 * 【偏差】1.0 的 snapshot() 产出 KvStateSnapshot 对象（内含 byte[]），再由
 * MemoryStateBackend 包成 StateHandle。本实现只有内存后端，直接读写
 * DataOutputView / DataInputView，少掉 KvStateSnapshot 这一层，字节序列不变。
 *
 * 【偏差】1.0 的 SV 由 stateSerializer（ValueState 用 V 的 serializer，
 * ListState 用 ArrayListSerializer<V>）统一序列化。C++ 这边 SV 是模板参数而非
 * Any，没法塞进 AnySerializer，所以抽出 serializeStateValue /
 * deserializeStateValue 两个钩子，由子类决定——HeapListState 的钩子就是
 * ArrayListSerializer 的逻辑。
 */
#ifndef FLINK_RUNTIME_STATE_ABSTRACTHEAPSTATE_H
#define FLINK_RUNTIME_STATE_ABSTRACTHEAPSTATE_H

#include <stdexcept>
#include <unordered_map>

#include "flink/runtime/state/KvState.h"

namespace flink {
namespace runtime {
namespace state {

template <class SV>
class AbstractHeapState : public KvState {
 public:
  typedef std::unordered_map<flink::types::Any, SV> NamespaceMap;

  AbstractHeapState(KvState::SerializerPtr keySerializer,
                    KvState::SerializerPtr namespaceSerializer,
                    KvState::SerializerPtr valueSerializer)
      : KvState(keySerializer, namespaceSerializer, valueSerializer),
        currentNSState_(NULL) {}

  void setCurrentKey(const flink::types::Any& key) override {
    currentKey_ = key;
  }

  // 1.0 AbstractHeapState.java:123
  void setCurrentNamespace(const flink::types::Any& ns) override {
    if (!ns.isNull() && ns == currentNamespace_) {
      return;
    }
    currentNamespace_ = ns;
    currentNSState_ = lookupNamespace();
  }

  // 1.0 AbstractHeapState.java:107
  void clear() override {
    if (currentNSState_ != NULL) {
      currentNSState_->erase(currentKey_);
      if (currentNSState_->empty()) {
        state_.erase(currentNamespace_);
        currentNSState_ = NULL;
      }
    }
  }

  // 1.0 AbstractHeapState.java:134
  size_t size() const override {
    size_t size = 0;
    for (const auto& ns : state_) {
      size += ns.second.size();
    }
    return size;
  }

  // 1.0 AbstractHeapState.java:143
  void dispose() override {
    state_.clear();
    currentNSState_ = NULL;
  }

  // —— 1.0 AbstractMemState.snapshot(checkpointId, timestamp) ——
  void snapshot(flink::core::memory::DataOutputView& target) const override {
    if (!isSnapshotable()) {
      throw std::logic_error("snapshot() called without serializers");
    }
    target.writeInt(static_cast<int32_t>(state_.size()));
    for (const auto& namespaceState : state_) {
      namespaceSerializer_->serialize(namespaceState.first, target);
      target.writeInt(static_cast<int32_t>(namespaceState.second.size()));
      for (const auto& entry : namespaceState.second) {
        keySerializer_->serialize(entry.first, target);
        serializeStateValue(entry.second, target);
      }
    }
  }

  // —— 1.0 AbstractMemStateSnapshot.restoreState(...) ——
  void restore(flink::core::memory::DataInputView& source) override {
    if (!isSnapshotable()) {
      throw std::logic_error("restore() called without serializers");
    }
    state_.clear();
    currentNSState_ = NULL;
    const int32_t numNamespaces = source.readInt();
    for (int32_t i = 0; i < numNamespaces; ++i) {
      flink::types::Any ns = namespaceSerializer_->deserialize(source);
      const int32_t numValues = source.readInt();
      NamespaceMap& namespaceMap = state_[ns];
      for (int32_t j = 0; j < numValues; ++j) {
        flink::types::Any key = keySerializer_->deserialize(source);
        namespaceMap[key] = deserializeStateValue(source);
      }
    }
    currentNSState_ = lookupNamespace();
  }

  // —— 未实现的语义一律抛异常：对应 1.0 里 `S extends State` 泛型约束
  //    在编译期就挡掉的调用（见 KvState.h 偏差 1）——
  flink::types::Any value() const override {
    throw std::logic_error("value() is not supported by this state type");
  }
  void update(const flink::types::Any&) override {
    throw std::logic_error("update() is not supported by this state type");
  }
  void add(const flink::types::Any&) override {
    throw std::logic_error("add() is not supported by this state type");
  }
  std::vector<flink::types::Any> get() const override {
    throw std::logic_error("get() is not supported by this state type");
  }

 protected:
  // 1.0 里 value()/add() 开头那段 `if (currentNSState == null)
  // currentNSState = state.get(currentNamespace);` 的懒加载
  const NamespaceMap* currentNSStateForRead() const {
    if (currentNSState_ == NULL) {
      currentNSState_ = const_cast<AbstractHeapState*>(this)->lookupNamespace();
    }
    return currentNSState_;
  }

  // 1.0 里 `currentNSState = new HashMap<>(); state.put(currentNamespace, ...)`
  NamespaceMap& currentNSStateForWrite() {
    if (currentNSState_ == NULL) {
      currentNSState_ = &state_[currentNamespace_];
    }
    return *currentNSState_;
  }

  virtual void serializeStateValue(
      const SV& value, flink::core::memory::DataOutputView& target) const = 0;
  virtual SV deserializeStateValue(
      flink::core::memory::DataInputView& source) const = 0;

  std::unordered_map<flink::types::Any, NamespaceMap> state_;
  flink::types::Any currentKey_;
  flink::types::Any currentNamespace_;

 private:
  NamespaceMap* lookupNamespace() {
    auto it = state_.find(currentNamespace_);
    return it == state_.end() ? NULL : &it->second;
  }

  // unordered_map 的 rehash 不会使指向元素的指针失效，所以缓存子表指针是安全的
  // （对应 1.0 缓存 Map<K, SV> 引用）
  mutable NamespaceMap* currentNSState_;
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_ABSTRACTHEAPSTATE_H
