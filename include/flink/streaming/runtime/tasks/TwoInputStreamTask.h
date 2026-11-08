/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask
 *
 * 1.0 class TwoInputStreamTask<IN1, IN2, OUT>
 *       extends StreamTask<OUT, TwoInputStreamOperator<IN1, IN2, OUT>>
 * （TwoInputStreamTask.java:33）—— 与 OneInputStreamTask 一样短，就是把
 * StreamTask 的四个模板方法接到 StreamTwoInputProcessor 上，唯一多出来的活是
 * **把输入 gate 按 typeNumber 分成两组**。
 *
 * 字段（:35-37）：
 *   private StreamTwoInputProcessor<IN1, IN2> inputProcessor;
 *   private volatile boolean running = true;
 *
 * init()（:40）：
 *   inputDeserializer1 = configuration.getTypeSerializerIn1(cl)
 *   inputDeserializer2 = configuration.getTypeSerializerIn2(cl)
 *   numberOfInputs     = configuration.getNumberOfInputs()
 *   inEdges            = configuration.getInPhysicalEdges(cl)
 *   for (int i = 0; i < numberOfInputs; i++) {
 *       int inputType = inEdges.get(i).getTypeNumber();
 *       InputGate reader = getEnvironment().getInputGate(i);
 *       switch (inputType) {
 *           case 1:  inputList1.add(reader); break;
 *           case 2:  inputList2.add(reader); break;
 *           default: throw new RuntimeException(
 *                        "Invalid input type number: " + inputType);
 *       }
 *   }
 *   inputProcessor = new StreamTwoInputProcessor<>(inputList1, inputList2,
 *       inputDeserializer1, inputDeserializer2,
 *       getCheckpointBarrierListener(), configuration.getCheckpointMode(),
 *       getEnvironment().getIOManager(), isSerializingTimestamps());
 *   inputProcessor.setReporter(registry.getReadWriteReporter());
 *
 *   —— StreamEdge.typeNumber 就是 1.0 在 StreamGraph 里给「connect 的第几条
 *   输入」打的标记（1 = 第一条，2 = 第二条，单输入算子一律 0 或 1）。
 *   第 i 条物理入边对应 Environment.getInputGate(i)，这个下标对应关系与
 *   OperatorChain 那边 outEdgesInOrder[i] ↔ Environment.getWriter(i) 完全同构。
 *
 * run()（:83）：
 *   1.0 注释：「cache some references on the stack, to make the code more JIT
 *   friendly」
 *   while (running && inputProcessor.processInput(operator, lock)) {
 *       checkTimerException();
 *   }
 *
 * cleanup()（:95）：inputProcessor.cleanup();
 * cancelTask()（:100）：running = false;
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— StreamTwoInputProcessor 的构造参数】1.0 传 inputDeserializer1/2 /
 * IOManager / enableWatermarkMultiplexing 五项，本实现的
 * StreamTwoInputProcessor 都不需要（没有反序列化层与溢写，见
 * StreamTwoInputProcessor.h 偏差 1/2）。configuration.getTypeSerializerIn1/2()
 * 与 isSerializingTimestamps() 的取值动作一并省掉。
 *
 * 【偏差 2 —— inEdges 的下标越界】1.0 的循环直接 `inEdges.get(i)`，若
 * numberOfInputs 与 inPhysicalEdges 的长度不一致就 IndexOutOfBounds；
 * getEnvironment().getInputGate(i) 同理。这里给出明确的错误信息，语义等价
 * （都是「StreamConfig 配错了」）。
 *
 * 【偏差 3 —— AccumulatorRegistry.Reporter 不复刻】同 OperatorChain.h 偏差 3。
 *
 * 【偏差 4 —— headOperator 的类型】1.0 的泛型上界把 headOperator 直接定成
 * TwoInputStreamOperator<IN1,IN2,OUT>。这里 StreamTask::headOperator_ 是
 * StreamOperator*，run() 里用 dynamic_cast 取 TwoInputStreamOperator*，
 * 转不过去就抛 —— 等价于 1.0 编译期就不允许这种配置（同 OneInputStreamTask.h
 * 偏差 4）。
 */
#ifndef FLINK_STREAMING_RUNTIME_TASKS_TWOINPUTSTREAMTASK_H
#define FLINK_STREAMING_RUNTIME_TASKS_TWOINPUTSTREAMTASK_H

#include <memory>

#include "flink/streaming/runtime/io/StreamTwoInputProcessor.h"
#include "flink/streaming/runtime/tasks/StreamTask.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

class TwoInputStreamTask : public StreamTask {
 public:
  typedef flink::streaming::runtime::io::StreamTwoInputProcessor
      StreamTwoInputProcessor;
  typedef flink::streaming::api::operators::TwoInputStreamOperator
      TwoInputStreamOperator;

  TwoInputStreamTask(Environment* environment, const StreamConfig& configuration)
      : StreamTask(environment, configuration), running_(true) {}

  // 1.0 的 init() 是 public（与 OneInputStreamTask 一致），其余三个 protected
  void init() override;

 protected:
  void run() override;
  void cleanup() override;
  void cancelTask() override;

 private:
  std::unique_ptr<StreamTwoInputProcessor> inputProcessor_;
  volatile bool running_;
};

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_TASKS_TWOINPUTSTREAMTASK_H
