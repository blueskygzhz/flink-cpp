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
 * RecordWriterOutput[] streamOutputs）；这里为了少一层转换放进接口。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_OUTPUT_H
#define FLINK_STREAMING_API_OPERATORS_OUTPUT_H

#include <memory>

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

  virtual void broadcastEvent(
      const std::shared_ptr<const flink::runtime::event::AbstractEvent>&
          event) = 0;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_OUTPUT_H
