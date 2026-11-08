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
 *
 * 【偏差】getWriter(int) / getAllWriters() / getInputGate(int) /
 * getAllInputGates() 这几项 1.0 返回 ResultPartitionWriter / InputGate
 * （由 TaskManager 按 JobGraph 的连接关系建好再注入 Task）。OperatorChain 要用
 * getWriter(outputIndex)（1.0 OperatorChain.java:284），OneInputStreamTask 要用
 * getAllInputGates()（1.0 OneInputStreamTask.java:44）。用前置声明避免
 * Environment 依赖 network 头文件。getAllWriters() / getAllInputGates() 在 1.0
 * 返回数组，这里改为「个数 + 按下标取」两个纯虚方法，再由非虚的
 * getAllInputGates() 拼出 vector —— 这样派生类少写一个方法，语义不变。
 */
#ifndef FLINK_RUNTIME_EXECUTION_ENVIRONMENT_H
#define FLINK_RUNTIME_EXECUTION_ENVIRONMENT_H

#include <cstdint>
#include <string>
#include <vector>

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {
class StreamTaskStateList;
}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
namespace runtime {
namespace io {
namespace network {
namespace api {
namespace writer {
class ResultPartitionWriter;
}  // namespace writer
}  // namespace api
namespace partition {
namespace consumer {
class InputGate;
}  // namespace consumer
}  // namespace partition
}  // namespace network
}  // namespace io
}  // namespace runtime
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

  // 1.0 getWriter(int index)：取第 index 个输出分区的 writer。
  // OperatorChain.createStreamOutput 按 outEdgesInOrder 的下标取
  // （1.0 OperatorChain.java:284）。返回 NULL 表示没有这个输出。
  virtual flink::runtime::io::network::api::writer::ResultPartitionWriter*
  getWriter(int index) = 0;

  // 1.0 getAllWriters()：StreamTask 在失败时用来 clearBuffers
  virtual int getNumberOfWriters() const = 0;

  // 1.0 getInputGate(int index)：取第 index 个输入 gate
  virtual flink::runtime::io::network::partition::consumer::InputGate*
  getInputGate(int index) = 0;

  virtual int getNumberOfInputGates() const = 0;

  // 1.0 getAllInputGates()：OneInputStreamTask.init() 把它整个交给
  // StreamInputProcessor（1.0 OneInputStreamTask.java:44）
  std::vector<flink::runtime::io::network::partition::consumer::InputGate*>
  getAllInputGates() {
    std::vector<flink::runtime::io::network::partition::consumer::InputGate*>
        gates;
    const int count = getNumberOfInputGates();
    gates.reserve(count > 0 ? static_cast<size_t>(count) : 0);
    for (int i = 0; i < count; ++i) {
      gates.push_back(getInputGate(i));
    }
    return gates;
  }

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
