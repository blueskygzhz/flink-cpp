/*
 * flink-runtime : org.apache.flink.runtime.execution.Environment
 *
 * 1.0 Environment 是 task 与 TaskManager 之间的全部接口（约 30 个方法）：
 * JobID/JobVertexID、TaskConfiguration、UserClassLoader、MemoryManager、
 * IOManager、BroadcastVariableManager、AccumulatorRegistry、InputGate[]、
 * ResultPartitionWriter[]、以及
 *   void acknowledgeCheckpoint(long checkpointId);
 *   void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state);
 *
 * 本实现只保留 StreamTask 真正用到的那几项：
 *   TaskInfo 相关（算子标识、并行度）—— 1.0 走 getTaskInfo()
 *   acknowledgeCheckpoint 两个重载 —— 1.0 走 ActorGateway 发给 JobManager，
 *                                     本实现直接回调到 CheckpointCoordinator
 *
 * 【偏差】1.0 的 acknowledgeCheckpoint 第二个重载收 StateHandle<?>（泛型，所以
 * flink-runtime 不需要知道 streaming 层的 StreamTaskStateList）。本实现没有泛型
 * 擦除可用，这里直接按 StreamTaskStateList 声明，用前置声明避免 include
 * streaming 层的头文件。
 *
 * 【偏差】MemoryManager / IOManager 不复刻：1.0 的算子要在托管内存里做排序和
 * 溢写，本实现的状态全在堆上（只做 MemoryStateBackend）。
 */
#ifndef FLINK_RUNTIME_EXECUTION_ENVIRONMENT_H
#define FLINK_RUNTIME_EXECUTION_ENVIRONMENT_H

#include <cstdint>
#include <string>

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {
class StreamTaskStateList;
}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink

namespace flink {
namespace runtime {
namespace execution {

class Environment {
 public:
  virtual ~Environment() {}

  // 1.0 getTaskInfo().getTaskName() / getTaskNameWithSubtasks()
  virtual std::string getTaskName() const = 0;
  virtual std::string getTaskNameWithSubtasks() const = 0;

  // 1.0 getTaskInfo().getIndexOfThisSubtask() / getNumberOfParallelSubtasks()
  virtual int getIndexInSubtaskGroup() const = 0;
  virtual int getNumberOfSubtasks() const = 0;

  // 1.0 acknowledgeCheckpoint(long)：本次 checkpoint 该 task 没有状态
  virtual void acknowledgeCheckpoint(int64_t checkpointId) = 0;

  // 1.0 acknowledgeCheckpoint(long, StateHandle<?>)
  virtual void acknowledgeCheckpoint(
      int64_t checkpointId,
      const flink::streaming::runtime::tasks::StreamTaskStateList& state) = 0;
};

}  // namespace execution
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_EXECUTION_ENVIRONMENT_H
