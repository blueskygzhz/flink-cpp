/*
 * flink-streaming-java : org.apache.flink.streaming.api.operators.StreamFlatMap
 *
 * 1.0 StreamFlatMap.java:
 *   class StreamFlatMap<IN, OUT> extends AbstractUdfStreamOperator<OUT,
 *                                          FlatMapFunction<IN, OUT>>
 *                                implements OneInputStreamOperator<IN, OUT>
 *   private transient TimestampedCollector<OUT> collector;
 *   StreamFlatMap(FlatMapFunction<IN,OUT> f) { super(f);
 *                                              chainingStrategy = ALWAYS; }
 *   open()  { super.open(); collector = new TimestampedCollector<>(output); }
 *   processElement(StreamRecord<IN> e) {
 *       collector.setTimestamp(e);
 *       userFunction.flatMap(e.getValue(), collector); }
 *   processWatermark(Watermark mark) { output.emitWatermark(mark); }
 *
 * collector 在 open() 里建（而不是构造函数里）：1.0 的算子要能被 Java 序列化
 * 发到 TaskManager，output 是 transient 的，只有 setup/open 之后才存在。
 * 本实现不需要序列化算子，但保留这个时序，因为 output_ 同样是 setup() 才填。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_STREAMFLATMAP_H
#define FLINK_STREAMING_API_OPERATORS_STREAMFLATMAP_H

#include <memory>

#include "flink/api/common/functions/FlatMapFunction.h"
#include "flink/streaming/api/operators/AbstractUdfStreamOperator.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"
#include "flink/streaming/api/operators/TimestampedCollector.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

template <class IN, class OUT>
class StreamFlatMap
    : public AbstractUdfStreamOperator<
          flink::api::common::functions::FlatMapFunction<IN, OUT> >,
      public OneInputStreamOperator {
 public:
  typedef flink::api::common::functions::FlatMapFunction<IN, OUT>
      FlatMapFunction;
  typedef AbstractUdfStreamOperator<FlatMapFunction> Base;

  explicit StreamFlatMap(std::shared_ptr<FlatMapFunction> flatMapper)
      : Base(flatMapper) {
    this->chainingStrategy_ = ChainingStrategy::ALWAYS;
  }

  std::string getSimpleName() const override { return "StreamFlatMap"; }

  void open() override {
    Base::open();
    collector_.reset(new TimestampedCollector<OUT>(this->output_));
  }

  void processElement(const StreamRecord& element) override {
    collector_->setTimestamp(element);
    this->userFunction_->flatMap(element.template getValueAs<IN>(),
                                 *collector_);
  }

  void processWatermark(
      const flink::streaming::api::watermark::Watermark& mark) override {
    this->output_->emitWatermark(mark);
  }

 private:
  std::unique_ptr<TimestampedCollector<OUT> > collector_;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_STREAMFLATMAP_H
