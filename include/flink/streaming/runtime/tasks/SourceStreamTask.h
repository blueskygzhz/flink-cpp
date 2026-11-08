/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.tasks.SourceStreamTask
 *
 * 1.0 class SourceStreamTask<OUT, SRC extends SourceFunction<OUT>,
 *                            OP extends StreamSource<OUT, SRC>>
 *       extends StreamTask<OUT, OP>            (SourceStreamTask.java:40)
 *
 * 1.0 原注释：「Task for executing streaming sources. One important aspect of
 * this is that the checkpointing and the emission of elements must never occur
 * at the same time. The execution must be serial. This is achieved by having
 * the contract with the StreamFunction that it must only modify its state or
 * emit elements in a synchronized block that locks on the lock Object. Also,
 * the modification of the state and the emission of elements must happen in
 * the same block of code that is protected by the synchronized block.」
 *
 * 全文（除注释）就四个方法：
 *   init()        {}   1.0 注释：「does not hold any resources, so no
 *                      initialization needed」
 *   cleanup()     {}   1.0 注释：「does not hold any resources, so no cleanup
 *                      needed」
 *   run()         headOperator.run(getCheckpointLock(), getHeadOutput());  (:56)
 *   cancelTask()  headOperator.cancel();                                   (:61)
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 取 headOperator 的方式】1.0 的泛型上界
 * `OP extends StreamSource<OUT, SRC>` 让 headOperator 字段本身就是源算子。
 * 这里 StreamTask::headOperator_ 是 StreamOperator*，所以用 dynamic_cast 取
 * SourceOperator*（见 SourceOperator.h），转不过去就抛 —— 等价于 1.0 编译期
 * 就不允许把非 StreamSource 的算子配给 SourceStreamTask。
 *
 * 【偏差 2 —— cancelTask() 的时序】1.0 的 cancelTask() 由 TaskManager 的
 * 另一条线程调用，此时 headOperator 一定已经建好（invoke() 里先建链再 run）。
 * 本实现里 StreamTask::cancel() 也可能在 invoke() 之前被调到（测试就会这么
 * 做），那时 headOperator_ 还是 NULL，所以这里多一个空指针判断直接返回。
 */
#ifndef FLINK_STREAMING_RUNTIME_TASKS_SOURCESTREAMTASK_H
#define FLINK_STREAMING_RUNTIME_TASKS_SOURCESTREAMTASK_H

#include "flink/streaming/api/operators/SourceOperator.h"
#include "flink/streaming/runtime/tasks/StreamTask.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

class SourceStreamTask : public StreamTask {
 public:
  typedef flink::streaming::api::operators::SourceOperator SourceOperator;

  SourceStreamTask(Environment* environment, const StreamConfig& configuration)
      : StreamTask(environment, configuration) {}

 protected:
  // 1.0: "does not hold any resources, so no initialization needed"
  void init() override {}

  // 1.0: "does not hold any resources, so no cleanup needed"
  void cleanup() override {}

  // 1.0 run()（:56）
  void run() override;

  // 1.0 cancelTask()（:61）
  void cancelTask() override;

 private:
  // 【偏差 1】1.0 由泛型上界保证
  SourceOperator* sourceOperator() const;
};

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_TASKS_SOURCESTREAMTASK_H
