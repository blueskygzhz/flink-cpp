/*
 * flink-runtime : org.apache.flink.runtime.state.KvState
 *
 * 1.0 KvState<K, N, S, SD, Backend>:
 *   void setCurrentKey(K key) / void setCurrentNamespace(N namespace)
 *   KvStateSnapshot<...> snapshot(long checkpointId, long timestamp)
 *   void dispose()
 *
 * 【偏差】1.0 有 5 个泛型参数（key / namespace / State 接口 / descriptor /
 * backend），靠类型擦除在运行时工作。这里做成非泛型基类：value 统一用 Any，
 * 由上层 HeapValueState / HeapListState 持有具体语义，算子侧再用模板包装。
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
  virtual ~KvState() {}

  // 1.0 setCurrentKey(K)：切换当前 key 作用域
  virtual void setCurrentKey(const flink::types::Any& key) = 0;

  // —— ValueState 语义（1.0 HeapValueState）——
  // 当前 key 下无值时返回 isNull() 为真的 Any（对应 Java 的 null）
  virtual flink::types::Any value() const = 0;
  virtual void update(const flink::types::Any& value) = 0;

  // —— ListState / 窗口内容语义（1.0 HeapListState）——
  virtual void add(const flink::types::Any& value) = 0;
  virtual std::vector<flink::types::Any> get() const = 0;

  // 1.0 State.clear()：清除**当前 key** 下的状态
  virtual void clear() = 0;

  // 1.0 KvState.snapshot(checkpointId, timestamp)。serializer 为空时表示
  // 该类型没有可用的 TypeSerializer（见 TypeExtractor 说明），此时快照会
  // 跳过该状态并由 MemoryStateBackend 记录告警。
  virtual void snapshot(
      const std::shared_ptr<const flink::api::common::typeutils::AnySerializer>&
          serializer,
      flink::core::memory::DataOutputView& target) const = 0;
  virtual void restore(
      const std::shared_ptr<const flink::api::common::typeutils::AnySerializer>&
          serializer,
      flink::core::memory::DataInputView& source) = 0;

  // 已登记的 key 数量，供快照与监控用
  virtual size_t getNumberOfKeys() const = 0;
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_KVSTATE_H
