/*
 * flink-runtime : org.apache.flink.runtime.state.AbstractStateBackend
 *
 * 1.0 AbstractStateBackend 的职责：
 *   initializeForJob(Environment, String, TypeSerializer<?> keySerializer)
 *   setCurrentKey(Object key)            —— keyed state 的当前 key 上下文
 *   <N, S> S getPartitionedState(N namespace, TypeSerializer<N>, StateDescriptor)
 *   snapshotPartitionedState(long checkpointId, long timestamp)
 *   injectKeyValueStateSnapshots(HashMap<String, KvStateSnapshot>)
 *   dispose() / close()
 *
 * 【偏差】1.0 用 `getPartitionedState(descriptor)` 泛型方法 + 反射式的
 * `descriptor.bind(backend)` 创建具体 State 实例。C++ 没有运行期反射，
 * 这里把「按名字取/建状态」做成非模板的 getKvState(name)，返回统一的
 * KvState 基类，再由算子侧用模板包装成 ValueState<T> / ListState<T>。
 * 命名空间（窗口用的 namespace 维度）合并进 state 名：name + "#" + ns。
 */
#ifndef FLINK_RUNTIME_STATE_ABSTRACTSTATEBACKEND_H
#define FLINK_RUNTIME_STATE_ABSTRACTSTATEBACKEND_H

#include <memory>
#include <string>

#include "flink/runtime/state/KvState.h"
#include "flink/runtime/state/StateHandle.h"
#include "flink/types/Any.h"

namespace flink {
namespace runtime {
namespace state {

class AbstractStateBackend {
 public:
  virtual ~AbstractStateBackend() {}

  // 1.0 initializeForJob(...)：本实现只需知道算子标识，用于快照分组
  virtual void initializeForJob(const std::string& operatorIdentifier) = 0;

  // 1.0 setCurrentKey(Object)：后续所有 state 读写都作用于该 key
  virtual void setCurrentKey(const flink::types::Any& key) = 0;
  virtual const flink::types::Any& getCurrentKey() const = 0;

  // 1.0 getPartitionedState(...) 的非模板版本（见文件头说明）
  virtual std::shared_ptr<KvState> getKvState(const std::string& stateName) = 0;

  // 1.0 snapshotPartitionedState(checkpointId, timestamp)
  virtual StateHandle snapshotPartitionedState(int64_t checkpointId,
                                               int64_t timestamp) = 0;
  // 1.0 injectKeyValueStateSnapshots(...)：恢复时把快照塞回后端
  virtual void injectKeyValueStateSnapshots(const StateHandle& handle) = 0;

  virtual void dispose() = 0;
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_ABSTRACTSTATEBACKEND_H
