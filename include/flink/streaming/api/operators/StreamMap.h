/*
 * flink-streaming-java : org.apache.flink.streaming.api.operators.StreamMap
 *
 * 1.0 StreamMap.java 全文就这么点：
 *   class StreamMap<IN, OUT> extends AbstractUdfStreamOperator<OUT,
 *                                      MapFunction<IN, OUT>>
 *                            implements OneInputStreamOperator<IN, OUT>
 *   StreamMap(MapFunction<IN,OUT> mapper) { super(mapper);
 *                                           chainingStrategy = ALWAYS; }
 *   processElement(StreamRecord<IN> e) {
 *       output.collect(e.replace(userFunction.map(e.getValue()))); }
 *   processWatermark(Watermark mark) { output.emitWatermark(mark); }
 *
 * 注意 1.0 用的是 e.replace(...)：**保留输入记录的时间戳**，map 不改变事件时间。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_STREAMMAP_H
#define FLINK_STREAMING_API_OPERATORS_STREAMMAP_H

#include "flink/api/common/functions/MapFunction.h"
#include "flink/streaming/api/operators/AbstractUdfStreamOperator.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

template <class IN, class OUT>
class StreamMap
    : public AbstractUdfStreamOperator<
          flink::api::common::functions::MapFunction<IN, OUT> >,
      public OneInputStreamOperator {
 public:
  typedef flink::api::common::functions::MapFunction<IN, OUT> MapFunction;
  typedef AbstractUdfStreamOperator<MapFunction> Base;

  explicit StreamMap(std::shared_ptr<MapFunction> mapper) : Base(mapper) {
    // 1.0: chainingStrategy = ChainingStrategy.ALWAYS;
    this->chainingStrategy_ = ChainingStrategy::ALWAYS;
  }

  std::string getSimpleName() const override { return "StreamMap"; }

  void processElement(const StreamRecord& element) override {
    this->output_->collect(element.replace(flink::types::Any::of(
        this->userFunction_->map(element.template getValueAs<IN>()))));
  }

  void processWatermark(
      const flink::streaming::api::watermark::Watermark& mark) override {
    this->output_->emitWatermark(mark);
  }
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_STREAMMAP_H
