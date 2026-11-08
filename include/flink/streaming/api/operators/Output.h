/*
 * flink-streaming-java : org.apache.flink.streaming.api.operators.Output
 *
 * 1.0 Output.java:
 *   interface Output<T> extends Collector<T>  (T = StreamRecord<OUT>)
 *   void emitWatermark(Watermark mark)
 * 另外加上 1.0 RecordWriterOutput.broadcastEvent(AbstractEvent)：
 * StreamTask 在完成快照后用它把 CheckpointBarrier 广播给下游，
 * 以及 subtask 结束时广播 EndOfPartitionEvent。1.0 里这个方法在
 * RecordWriterOutput 上而不在 Output 接口上（StreamTask 直接持有
 * RecordWriterOutput[] streamOutputs）；这里为了少一层转换放进接口，
 * 但**给一个抛异常的默认实现**：1.0 的 ChainingOutput /
 * BroadcastingOutputCollector / DirectedOutput 这些链内输出确实没有这个方法，
 * 对它们调用 broadcastEvent 在 1.0 里是编译错误，在这里就是 logic_error。
 *
 * flush() 同理：1.0 在 RecordWriterOutput 上（:102），由
 * OperatorChain.flushOutputs() 调用（StreamTask.invoke() 的
 * StreamTask.java:265）。给它一个默认空实现，这样链内的 ChainingOutput
 * 与测试替身都不必关心——对应 1.0 里「只有链尾的 RecordWriterOutput
 * 才需要 flush」这一事实。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_OUTPUT_H
#define FLINK_STREAMING_API_OPERATORS_OUTPUT_H

#include <memory>
#include <stdexcept>

#include "flink/runtime/event/AbstractEvent.h"
#include "flink/streaming/api/watermark/Watermark.h"
#include "flink/streaming/runtime/streamrecord/StreamRecord.h"
#include "flink/util/Collector.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

class Output : public flink::util::Collector<
                   flink::streaming::runtime::streamrecord::StreamRecord> {
 public:
  virtual void emitWatermark(
      const flink::streaming::api::watermark::Watermark& mark) = 0;

  // 1.0 RecordWriterOutput.broadcastEvent(AbstractEvent)；链内输出没有这个
  // 能力（1.0 里它们压根没这个方法）
  virtual void broadcastEvent(
      const std::shared_ptr<const flink::runtime::event::AbstractEvent>&
          event) {
    (void)event;
    throw std::logic_error(
        "broadcastEvent is only supported on RecordWriterOutput");
  }

  // 1.0 RecordWriterOutput.flush()；链内输出无缓冲，故默认空实现
  virtual void flush() {}
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_OUTPUT_H
