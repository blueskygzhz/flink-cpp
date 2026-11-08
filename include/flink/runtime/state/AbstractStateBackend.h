/*
 * flink-runtime : org.apache.flink.runtime.state.AbstractStateBackend
 *
 * 1.0 的字段与方法（AbstractStateBackend.java:49）：
 *   protected TypeSerializer<?> keySerializer;
 *   protected Object currentKey;
 *   private KvState<?,?,?,?,?>[] keyValueStates;              // setCurrentKey 快路径
 *   private HashMap<String, KvState<...>> keyValueStatesByName;
 *   private String lastName; private KvState lastState;       // 上次访问缓存
 *
 *   void initializeForJob(Environment, String operatorIdentifier,
 *                         TypeSerializer<?> keySerializer)
 *   abstract void disposeAllStateForCurrentJob()
 *   abstract void close()
 *   void dispose()
 *   protected abstract <N,T> ValueState<T> createValueState(
 *       TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<T>)
 *   protected abstract <N,T> ListState<T>  createListState(...)
 *   protected abstract <N,T> ReducingState<T> createReducingState(...)
 *   protected abstract <N,T,ACC> FoldingState<T,ACC> createFoldingState(...)
 *   void setCurrentKey(Object) / Object getCurrentKey()
 *   <N,S extends State> S getPartitionedState(N namespace,
 *       TypeSerializer<N> namespaceSerializer, StateDescriptor<S,?>)
 *   HashMap<String, KvStateSnapshot<...>> snapshotPartitionedState(long, long)
 *   void notifyOfCompletedCheckpoint(long checkpointId)
 *   final void injectKeyValueStateSnapshots(
 *       HashMap<String, KvStateSnapshot>, long recoveryTimestamp)
 *   abstract <S extends Serializable> StateHandle<S>
 *       checkpointStateSerializable(S state, long checkpointID, long timestamp)
 *   abstract CheckpointStateOutputStream createCheckpointStateOutputStream(long, long)
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 取代 StateDescriptor.bind(StateBackend)】
 * 1.0 的分派链是：getPartitionedState(desc) -> desc.bind(匿名 StateBackend)
 * -> backend.createValueState(desc)。这条链存在的唯一理由是让 flink-core 的
 * StateDescriptor 能创建 flink-runtime 的状态对象而不产生编译依赖，靠的是
 * Java 泛型 + 擦除。C++ 的等价物是**重载决议**：这里给 getPartitionedState
 * 开两个模板重载（ValueStateDescriptor<T> / ListStateDescriptor<T>），编译期
 * 就选中了要建哪种状态。因此 1.0 flink-core 里的 StateBackend 接口和
 * StateDescriptor.bind 在本实现中没有对应物。
 *
 * 【偏差 2 —— 只实现 Value / List 两种状态】
 * 1.0 还有 ReducingState 与 FoldingState，但它们在 1.0 的内存后端里
 * （MemReducingState / MemFoldingState）本质就是「HeapValueState + 一次
 * ReduceFunction/FoldFunction 调用」。本实现把那次调用放到算子里
 * （StreamGroupedReduce / StreamGroupedFold 直接用 ValueState），所以不复刻
 * 这两种 State 类型。
 *
 * 【偏差 3 —— StateHandle 代替 KvStateSnapshot】
 * 见 KvState.h 偏差 2。snapshotPartitionedState 返回 name -> StateHandle
 * （内含序列化字节），injectKeyValueStateSnapshots 把字节存下来**延迟恢复**
 * —— 这正是 1.0 该方法注释里写的 "Injects K/V state snapshots for lazy
 * restore"：状态对象在 getPartitionedState 第一次创建它时才吃进字节。
 * 1.0 能立刻恢复是因为快照对象自己会 createMemState()，而本实现的句柄只有字节。
 *
 * 【偏差 4 —— 去掉的东西】Environment / ClassLoader / CheckpointStateOutputStream
 * （非分区状态的输出流，本实现不做非分区状态）。
 */
#ifndef FLINK_RUNTIME_STATE_ABSTRACTSTATEBACKEND_H
#define FLINK_RUNTIME_STATE_ABSTRACTSTATEBACKEND_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "flink/api/common/state/ListStateDescriptor.h"
#include "flink/api/common/state/State.h"
#include "flink/api/common/state/ValueStateDescriptor.h"
#include "flink/api/common/typeutils/TypedAnySerializer.h"
#include "flink/runtime/state/CheckpointListener.h"
#include "flink/runtime/state/KvState.h"
#include "flink/runtime/state/StateHandle.h"
#include "flink/runtime/state/TypedListState.h"
#include "flink/runtime/state/TypedValueState.h"
#include "flink/runtime/util/DataInputDeserializer.h"
#include "flink/runtime/util/DataOutputSerializer.h"
#include "flink/types/Any.h"

namespace flink {
namespace runtime {
namespace state {

class AbstractStateBackend {
 public:
  typedef KvState::SerializerPtr SerializerPtr;
  typedef std::unordered_map<std::string, StateHandle> StateHandleMap;

  AbstractStateBackend() {}
  virtual ~AbstractStateBackend() {}

  // ----------------------------------------------------------------------
  //  initialization and cleanup
  // ----------------------------------------------------------------------

  // 1.0 initializeForJob(Environment, String, TypeSerializer<?>)
  virtual void initializeForJob(const std::string& operatorIdentifier,
                                SerializerPtr keySerializer) {
    operatorIdentifier_ = operatorIdentifier;
    keySerializer_ = keySerializer;
  }

  virtual void disposeAllStateForCurrentJob() = 0;
  virtual void close() = 0;

  // 1.0 dispose(): 非抽象，逐个 dispose 掉持有的 KvState
  virtual void dispose() {
    for (auto& entry : keyValueStatesByName_) {
      entry.second->dispose();
    }
    keyValueStatesByName_.clear();
    typedStatesByName_.clear();
    restoredState_.clear();
    lastName_.clear();
    lastState_.reset();
  }

  // ----------------------------------------------------------------------
  //  key/value state
  // ----------------------------------------------------------------------

  // 1.0 setCurrentKey(Object)：顺带推给所有已建状态（1.0 为此专门存了一份
  // keyValueStates 数组做快路径）
  void setCurrentKey(const flink::types::Any& currentKey) {
    currentKey_ = currentKey;
    for (auto& entry : keyValueStatesByName_) {
      entry.second->setCurrentKey(currentKey);
    }
  }

  const flink::types::Any& getCurrentKey() const { return currentKey_; }

  // 1.0 getPartitionedState(namespace, namespaceSerializer, descriptor)
  // 的 ValueStateDescriptor 重载（见偏差 1）
  template <class T>
  std::shared_ptr<flink::api::common::state::ValueState<T> >
  getPartitionedState(
      const flink::types::Any& ns, SerializerPtr namespaceSerializer,
      const flink::api::common::state::ValueStateDescriptor<T>& stateDesc) {
    const std::string& name = stateDesc.getName();
    std::shared_ptr<KvState> kvState =
        getOrCreateKvState(name, VALUE_STATE, namespaceSerializer,
                           resolveSerializer<T>(stateDesc));
    kvState->setCurrentNamespace(ns);

    auto cached = typedStatesByName_.find(name);
    if (cached != typedStatesByName_.end()) {
      return std::static_pointer_cast<
          flink::api::common::state::ValueState<T> >(cached->second);
    }
    auto typed = std::make_shared<TypedValueState<T> >(
        kvState, stateDesc.getDefaultValue());
    typedStatesByName_[name] = typed;
    return typed;
  }

  // 1.0 getPartitionedState(...) 的 ListStateDescriptor 重载
  template <class T>
  std::shared_ptr<flink::api::common::state::ListState<T> > getPartitionedState(
      const flink::types::Any& ns, SerializerPtr namespaceSerializer,
      const flink::api::common::state::ListStateDescriptor<T>& stateDesc) {
    const std::string& name = stateDesc.getName();
    std::shared_ptr<KvState> kvState =
        getOrCreateKvState(name, LIST_STATE, namespaceSerializer,
                           resolveSerializer<T>(stateDesc));
    kvState->setCurrentNamespace(ns);

    auto cached = typedStatesByName_.find(name);
    if (cached != typedStatesByName_.end()) {
      return std::static_pointer_cast<flink::api::common::state::ListState<T> >(
          cached->second);
    }
    auto typed = std::make_shared<TypedListState<T> >(kvState);
    typedStatesByName_[name] = typed;
    return typed;
  }

  // ----------------------------------------------------------------------
  //  checkpointing
  // ----------------------------------------------------------------------

  // 1.0 snapshotPartitionedState(checkpointId, timestamp)
  StateHandleMap snapshotPartitionedState(int64_t checkpointId,
                                          int64_t timestamp) {
    StateHandleMap snapshots;
    for (const auto& entry : keyValueStatesByName_) {
      // 拿不到 TypeSerializer 的状态只能留在内存里（见 TypeExtractor 说明），
      // 1.0 没有这个分支：那边 TypeInformation 总能给出 serializer（兜底 Kryo）
      if (!entry.second->isSnapshotable()) {
        continue;
      }
      flink::runtime::util::DataOutputSerializer out;
      entry.second->snapshot(out);
      snapshots[entry.first] = checkpointStateSerializable(
          out.getByteArray(), checkpointId, timestamp);
    }
    return snapshots;
  }

  // 1.0 notifyOfCompletedCheckpoint(long)：只通知实现了 CheckpointListener 的状态
  void notifyOfCompletedCheckpoint(int64_t checkpointId) {
    for (auto& entry : keyValueStatesByName_) {
      CheckpointListener* listener =
          dynamic_cast<CheckpointListener*>(entry.second.get());
      if (listener != NULL) {
        listener->notifyCheckpointComplete(checkpointId);
      }
    }
  }

  // 1.0 injectKeyValueStateSnapshots(HashMap<String, KvStateSnapshot>,
  //                                  long recoveryTimestamp)
  // 1.0 里是 final 方法，所以这里不声明 virtual
  void injectKeyValueStateSnapshots(const StateHandleMap& keyValueStateSnapshots,
                                    int64_t recoveryTimestamp) {
    (void)recoveryTimestamp;  // 1.0 透传给 KvStateSnapshot.restoreState
    for (const auto& entry : keyValueStateSnapshots) {
      auto existing = keyValueStatesByName_.find(entry.first);
      if (existing != keyValueStatesByName_.end()) {
        // 状态已存在（serializer 已知），立即恢复
        restoreInto(*existing->second, entry.second);
      } else {
        // 延迟恢复：等 getPartitionedState 建出状态时再吃字节（见偏差 3）
        restoredState_[entry.first] = entry.second;
      }
    }
  }

  // 1.0 checkpointStateSerializable(S state, long checkpointID, long timestamp)
  virtual StateHandle checkpointStateSerializable(const std::string& bytes,
                                                  int64_t checkpointId,
                                                  int64_t timestamp) = 0;

 protected:
  // 【偏差 1】取代 StateDescriptor.bind 的分派标签
  enum StateKind { VALUE_STATE, LIST_STATE };

  // 1.0 protected abstract <N,T> ValueState<T> createValueState(
  //         TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<T>)
  virtual std::shared_ptr<KvState> createValueState(
      SerializerPtr namespaceSerializer, SerializerPtr valueSerializer) = 0;

  // 1.0 protected abstract <N,T> ListState<T> createListState(...)
  virtual std::shared_ptr<KvState> createListState(
      SerializerPtr namespaceSerializer, SerializerPtr valueSerializer) = 0;

  SerializerPtr keySerializer_;
  std::string operatorIdentifier_;

 private:
  // 1.0 getPartitionedState 的主体：lastName/lastState 缓存 + 按名字复用 +
  // 新建后立刻 setCurrentKey（AbstractStateBackend.java:196-258）
  std::shared_ptr<KvState> getOrCreateKvState(
      const std::string& name, StateKind kind,
      SerializerPtr namespaceSerializer, SerializerPtr valueSerializer) {
    if (lastState_ && lastName_ == name) {
      return lastState_;
    }

    auto previous = keyValueStatesByName_.find(name);
    if (previous != keyValueStatesByName_.end()) {
      lastName_ = name;
      lastState_ = previous->second;
      return previous->second;
    }

    std::shared_ptr<KvState> kvState =
        kind == VALUE_STATE
            ? createValueState(namespaceSerializer, valueSerializer)
            : createListState(namespaceSerializer, valueSerializer);

    keyValueStatesByName_[name] = kvState;
    lastName_ = name;
    lastState_ = kvState;

    kvState->setCurrentKey(currentKey_);

    // 延迟恢复（见偏差 3）
    auto pending = restoredState_.find(name);
    if (pending != restoredState_.end()) {
      restoreInto(*kvState, pending->second);
      restoredState_.erase(pending);
    }
    return kvState;
  }

  static void restoreInto(KvState& kvState, const StateHandle& handle) {
    flink::runtime::util::DataInputDeserializer in(handle.getState());
    kvState.restore(in);
  }

  // 1.0 getPartitionedState 开头的
  // `if (!stateDescriptor.isSerializerInitialized())
  //      stateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());`
  // —— descriptor 没显式带 serializer 时，按类型推导一个
  template <class T, class SD>
  static SerializerPtr resolveSerializer(const SD& stateDesc) {
    typedef flink::api::common::typeutils::TypedAnySerializer<T> Typed;
    if (stateDesc.getSerializer()) {
      return std::make_shared<const Typed>(stateDesc.getSerializer());
    }
    return Typed::create();
  }

  flink::types::Any currentKey_;

  std::unordered_map<std::string, std::shared_ptr<KvState> >
      keyValueStatesByName_;
  std::unordered_map<std::string,
                     std::shared_ptr<flink::api::common::state::State> >
      typedStatesByName_;
  StateHandleMap restoredState_;

  std::string lastName_;
  std::shared_ptr<KvState> lastState_;
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_ABSTRACTSTATEBACKEND_H
