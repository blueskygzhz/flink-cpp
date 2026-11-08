/*
 * flink-runtime : org.apache.flink.runtime.jobgraph.tasks.StatefulTask
 *
 * 1.0 interface StatefulTask<T extends StateHandle<?>>（StatefulTask.java:27）：
 *   void setInitialState(T stateHandle, long recoveryTimestamp);
 *   boolean triggerCheckpoint(long checkpointId, long timestamp);
 *   void notifyCheckpointComplete(long checkpointId);
 *
 * 1.0 注释：「This interface must be implemented by any invokable that has
 * recoverable state and participates in checkpointing」。CheckpointCoordinator
 * 通过它驱动一次 checkpoint：triggerCheckpoint 只发给 source（其余算子靠屏障
 * 传播触发），notifyCheckpointComplete 在全局完成后广播给所有 task。
 *
 * 【偏差】1.0 的泛型参数 T 固定为 StreamTaskStateList（StreamTask 的实参），
 * 这里直接写死，不做模板。
 */
#ifndef FLINK_RUNTIME_JOBGRAPH_TASKS_STATEFULTASK_H
#define FLINK_RUNTIME_JOBGRAPH_TASKS_STATEFULTASK_H

#include <cstdint>

#include "flink/streaming/runtime/tasks/StreamTaskStateList.h"

namespace flink {
namespace runtime {
namespace jobgraph {
namespace tasks {

class StatefulTask {
 public:
  typedef flink::streaming::runtime::tasks::StreamTaskStateList
      StreamTaskStateList;

  virtual ~StatefulTask() {}

  virtual void setInitialState(const StreamTaskStateList& stateHandle,
                               int64_t recoveryTimestamp) = 0;
  virtual bool triggerCheckpoint(int64_t checkpointId, int64_t timestamp) = 0;
  virtual void notifyCheckpointComplete(int64_t checkpointId) = 0;
};

}  // namespace tasks
}  // namespace jobgraph
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_JOBGRAPH_TASKS_STATEFULTASK_H
