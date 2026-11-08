/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.tasks.OneInputStreamTask
 *
 * 1.0 class OneInputStreamTask<IN, OUT>
 *       extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>>
 * （OneInputStreamTask.java:30）—— 全文很短，就是把 StreamTask 的四个模板方法
 * 接到 StreamInputProcessor 上。
 *
 * 字段（:32-34）：
 *   private StreamInputProcessor<IN> inputProcessor;
 *   private volatile boolean running = true;
 *
 * init()（:37）：
 *   inSerializer   = configuration.getTypeSerializerIn1(cl)
 *   numberOfInputs = configuration.getNumberOfInputs()
 *   if (numberOfInputs > 0) {
 *     inputGates = getEnvironment().getAllInputGates();
 *     inputProcessor = new StreamInputProcessor<>(inputGates, inSerializer,
 *         getCheckpointBarrierListener(), configuration.getCheckpointMode(),
 *         getEnvironment().getIOManager(), isSerializingTimestamps());
 *     inputProcessor.setReporter(registry.getReadWriteReporter());
 *   }
 *
 * run()（:59）：
 *   1.0 注释：「cache some references on the stack, to make the code more JIT
 *   friendly」
 *   while (running && inputProcessor.processInput(operator, lock)) {
 *       checkTimerException();
 *   }
 *
 * cleanup()（:71）：inputProcessor.cleanup();
 * cancelTask()（:76）：running = false;
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— StreamInputProcessor 的构造参数】1.0 传 inSerializer /
 * IOManager / enableWatermarkMultiplexing 三项，本实现的 StreamInputProcessor
 * 都不需要（没有反序列化层与溢写，见 StreamInputProcessor.h 偏差 1/2）。
 * configuration.getTypeSerializerIn1() 与 isSerializingTimestamps() 的取值
 * 动作一并省掉。
 *
 * 【偏差 2 —— numberOfInputs == 0 的分支】1.0 在没有输入时不建
 * inputProcessor，于是 run() 里的 `inputProcessor.processInput(...)` 会 NPE
 * —— 实际上 OneInputStreamTask 不会被用于没有输入的顶点（那是
 * SourceStreamTask 的活）。这个判定原样保留，但 run()/cleanup() 里加了空指针
 * 检查，把 1.0 的 NPE 变成一条明确的错误信息。
 *
 * 【偏差 3 —— AccumulatorRegistry.Reporter 不复刻】同 OperatorChain.h 偏差 3。
 *
 * 【偏差 4 —— headOperator 的类型】1.0 的泛型上界把 headOperator 直接定成
 * OneInputStreamOperator<IN,OUT>。这里 StreamTask::headOperator_ 是
 * StreamOperator*，run() 里用 dynamic_cast 取 OneInputStreamOperator*，
 * 转不过去就抛 —— 等价于 1.0 编译期就不允许这种配置。
 */
#ifndef FLINK_STREAMING_RUNTIME_TASKS_ONEINPUTSTREAMTASK_H
#define FLINK_STREAMING_RUNTIME_TASKS_ONEINPUTSTREAMTASK_H

#include <memory>

#include "flink/streaming/runtime/io/StreamInputProcessor.h"
#include "flink/streaming/runtime/tasks/StreamTask.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

class OneInputStreamTask : public StreamTask {
 public:
  typedef flink::streaming::runtime::io::StreamInputProcessor
      StreamInputProcessor;
  typedef flink::streaming::api::operators::OneInputStreamOperator
      OneInputStreamOperator;

  OneInputStreamTask(Environment* environment, const StreamConfig& configuration)
      : StreamTask(environment, configuration), running_(true) {}

  // 1.0 的 init() 是 public（唯一一个），其余三个 protected
  void init() override;

 protected:
  void run() override;
  void cleanup() override;
  void cancelTask() override;

 private:
  std::unique_ptr<StreamInputProcessor> inputProcessor_;
  volatile bool running_;
};

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_TASKS_ONEINPUTSTREAMTASK_H
