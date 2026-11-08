/*
 * flink-streaming-java : org.apache.flink.streaming.api.operators.StreamSink
 *
 * 1.0 StreamSink.java:26 全文就这么点：
 *   class StreamSink<IN> extends AbstractUdfStreamOperator<Object,
 *                                  SinkFunction<IN>>
 *                        implements OneInputStreamOperator<IN, Object>
 *   StreamSink(SinkFunction<IN> sinkFunction) { super(sinkFunction);
 *                                               chainingStrategy = ALWAYS; }
 *   processElement(StreamRecord<IN> e) { userFunction.invoke(e.getValue()); }
 *   processWatermark(Watermark mark) {
 *       // ignore it for now, we are a sink, after all
 *   }
 *
 * 注意输出类型是 Object（sink 不往下游发东西），所以 processElement 里**不碰
 * output**；processWatermark 同理，1.0 的注释就是那句「we are a sink, after
 * all」。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_STREAMSINK_H
#define FLINK_STREAMING_API_OPERATORS_STREAMSINK_H

#include <memory>
#include <string>

#include "flink/streaming/api/functions/sink/SinkFunction.h"
#include "flink/streaming/api/operators/AbstractUdfStreamOperator.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

template <class IN>
class StreamSink
    : public AbstractUdfStreamOperator<
          flink::streaming::api::functions::sink::SinkFunction<IN> >,
      public OneInputStreamOperator {
 public:
  typedef flink::streaming::api::functions::sink::SinkFunction<IN> SinkFunction;
  typedef AbstractUdfStreamOperator<SinkFunction> Base;

  explicit StreamSink(std::shared_ptr<SinkFunction> sinkFunction)
      : Base(sinkFunction) {
    // 1.0: chainingStrategy = ChainingStrategy.ALWAYS;
    this->chainingStrategy_ = ChainingStrategy::ALWAYS;
  }

  std::string getSimpleName() const override { return "StreamSink"; }

  void processElement(const StreamRecord& element) override {
    this->userFunction_->invoke(element.template getValueAs<IN>());
  }

  void processWatermark(
      const flink::streaming::api::watermark::Watermark&) override {
    // 1.0: "ignore it for now, we are a sink, after all"
  }
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_STREAMSINK_H
