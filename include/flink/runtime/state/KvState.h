/*
 * flink-runtime : org.apache.flink.runtime.state.KvState
 *
 * 1.0 KvState<K, N, S extends State, SD extends StateDescriptor<S, ?>,
 *              Backend extends AbstractStateBackend>:
 *   void setCurrentKey(K key);
 *   void setCurrentNamespace(N namespace);
 *   KvStateSnapshot<K, N, S, SD, Backend> snapshot(long checkpointId,
 *                                                  long timestamp);
 *   void dispose();
 *
 * 【偏差 1 —— 泛型参数】1.0 的 5 个类型参数（key / namespace / State 接口 /
 * descriptor / backend）在字节码里被擦除，运行期靠 TypeSerializer 工作。这里做成
 * 非泛型基类：key / namespace / value 统一用 Any，具体语义由 HeapValueState /
 * HeapListState 提供，算子侧再用模板包装成 ValueState<T> / ListState<T>。
 * 代价是 ValueState 与 ListState 的方法都出现在同一个基类上，调用不属于自己语义的
 * 方法会抛异常（见 AbstractHeapState）—— 对应 1.0 里由 `S extends State` 泛型约束
 * 在编译期拦掉的那部分。
 *
 * 【偏差 2 —— 快照对象】1.0 的 snapshot() 返回 KvStateSnapshot（可以是文件句柄、
 * 异步句柄等）。本实现只有内存后端，直接把字节写进 DataOutputView，省掉
 * KvStateSnapshot / AbstractMemStateSnapshot 这一层，字节格式与
 * AbstractMemState.snapshot 保持一致。
 *
 * 序列化器在构造时注入，对应 1.0 AbstractHeapState 的构造参数
 * (keySerializer, namespaceSerializer, stateSerializer, stateDesc)。
 * keySerializer / valueSerializer 任一为空表示该类型拿不到 TypeSerializer
 * （见 TypeExtractor 说明），此时 isSnapshotable() 为 false，快照跳过该状态。
 */
#ifndef FLINK_RUNTIME_STATE_KVSTATE_H
#define FLINK_RUNTIME_STATE_KVSTATE_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "flink/api/common/typeutils/AnySerializer.h"
#include "flink/types/Any.h"

namespace flink {
namespace runtime {
namespace state {

class KvState {
 public:
  typedef std::shared_ptr<const flink::api::common::typeutils::AnySerializer>
      SerializerPtr;

  KvState(SerializerPtr keySerializer, SerializerPtr namespaceSerializer,
          SerializerPtr valueSerializer)
      : keySerializer_(keySerializer),
        namespaceSerializer_(namespaceSerializer),
        valueSerializer_(valueSerializer) {}
  virtual ~KvState() {}

  // 1.0 setCurrentKey(K) / setCurrentNamespace(N)
  virtual void setCurrentKey(const flink::types::Any& key) = 0;
  virtual void setCurrentNamespace(const flink::types::Any& ns) = 0;

  // —— ValueState 语义（1.0 HeapValueState）——
  // 当前 (namespace, key) 下无值时返回 isNull() 为真的 Any（对应 Java 的 null）
  virtual flink::types::Any value() const = 0;
  virtual void update(const flink::types::Any& value) = 0;

  // —— ListState / 窗口内容语义（1.0 HeapListState）——
  virtual void add(const flink::types::Any& value) = 0;
  virtual std::vector<flink::types::Any> get() const = 0;

  // 1.0 State.clear()：只清除**当前 (namespace, key)** 下的状态
  virtual void clear() = 0;

  // 1.0 KvState.snapshot(checkpointId, timestamp) / KvStateSnapshot.restoreState
  virtual void snapshot(flink::core::memory::DataOutputView& target) const = 0;
  virtual void restore(flink::core::memory::DataInputView& source) = 0;

  // 1.0 dispose()：丢掉整张状态表
  virtual void dispose() = 0;

  // 三个 serializer 都齐备才能落盘
  bool isSnapshotable() const {
    return static_cast<bool>(keySerializer_) &&
           static_cast<bool>(namespaceSerializer_) &&
           static_cast<bool>(valueSerializer_);
  }

  // 1.0 AbstractHeapState.getKeySerializer() / getNamespaceSerializer()
  const SerializerPtr& getKeySerializer() const { return keySerializer_; }
  const SerializerPtr& getNamespaceSerializer() const {
    return namespaceSerializer_;
  }

  // 1.0 AbstractHeapState.size()：跨 namespace 的状态条目总数
  virtual size_t size() const = 0;

 protected:
  SerializerPtr keySerializer_;
  SerializerPtr namespaceSerializer_;
  SerializerPtr valueSerializer_;
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_KVSTATE_H
