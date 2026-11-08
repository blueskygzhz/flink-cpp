/*
 * flink-streaming-java : org.apache.flink.streaming.api.operators.StreamFilter
 *
 * 1.0 StreamFilter.java:
 *   class StreamFilter<IN> extends AbstractUdfStreamOperator<IN,
 *                                    FilterFunction<IN>>
 *                          implements OneInputStreamOperator<IN, IN>
 *   StreamFilter(FilterFunction<IN> f) { super(f);
 *                                        chainingStrategy = ALWAYS; }
 *   processElement(StreamRecord<IN> e) {
 *       if (userFunction.filter(e.getValue())) output.collect(e); }
 *   processWatermark(Watermark mark) { output.emitWatermark(mark); }
 *
 * 注意 1.0 直接 collect(e) 原记录（不是 replace），连时间戳一起原样转发。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_STREAMFILTER_H
#define FLINK_STREAMING_API_OPERATORS_STREAMFILTER_H

#include "flink/api/common/functions/FilterFunction.h"
#include "flink/streaming/api/operators/AbstractUdfStreamOperator.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

template <class IN>
class StreamFilter
    : public AbstractUdfStreamOperator<
          flink::api::common::functions::FilterFunction<IN> >,
      public OneInputStreamOperator {
 public:
  typedef flink::api::common::functions::FilterFunction<IN> FilterFunction;
  typedef AbstractUdfStreamOperator<FilterFunction> Base;

  explicit StreamFilter(std::shared_ptr<FilterFunction> filterFunction)
      : Base(filterFunction) {
    this->chainingStrategy_ = ChainingStrategy::ALWAYS;
  }

  std::string getSimpleName() const override { return "StreamFilter"; }

  void processElement(const StreamRecord& element) override {
    if (this->userFunction_->filter(element.template getValueAs<IN>())) {
      this->output_->collect(element);
    }
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
#endif  // FLINK_STREAMING_API_OPERATORS_STREAMFILTER_H
