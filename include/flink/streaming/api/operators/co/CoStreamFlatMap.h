/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.operators.co.CoStreamFlatMap
 *
 * 1.0 CoStreamFlatMap.java:32
 *   class CoStreamFlatMap<IN1, IN2, OUT>
 *       extends AbstractUdfStreamOperator<OUT, CoFlatMapFunction<IN1,IN2,OUT>>
 *       implements TwoInputStreamOperator<IN1, IN2, OUT>
 *
 *   private transient TimestampedCollector<OUT> collector;             (:38)
 *   private long combinedWatermark = Long.MIN_VALUE;                   (:42)
 *   private long input1Watermark   = Long.MIN_VALUE;
 *   private long input2Watermark   = Long.MIN_VALUE;
 *
 *   CoStreamFlatMap(CoFlatMapFunction<IN1,IN2,OUT> flatMapper) {
 *       super(flatMapper); }                                           (:46)
 *     —— 同 CoStreamMap：不设 chainingStrategy，留默认 HEAD
 *   open()  { super.open(); collector = new TimestampedCollector<>(output); }
 *   processElement1(e) { collector.setTimestamp(e);
 *                        userFunction.flatMap1(e.getValue(), collector); } (:56)
 *   processElement2(e) { collector.setTimestamp(e);
 *                        userFunction.flatMap2(e.getValue(), collector); } (:62)
 *   processWatermark1 / processWatermark2：与 CoStreamMap 逐字相同的
 *       combinedWatermark = min(input1Watermark, input2Watermark) 推进
 *   protected TimestampedCollector<OUT> getCollector()                 (:86)
 *
 * collector 在 open() 里建而不是构造函数里（原因同 StreamFlatMap.h 的说明：
 * 1.0 的 output 是 transient，只有 setup/open 之后才存在；本实现的 output_
 * 同样是 setup() 才填）。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_CO_COSTREAMFLATMAP_H
#define FLINK_STREAMING_API_OPERATORS_CO_COSTREAMFLATMAP_H

#include <algorithm>
#include <limits>
#include <memory>

#include "flink/streaming/api/functions/co/CoFlatMapFunction.h"
#include "flink/streaming/api/operators/AbstractUdfStreamOperator.h"
#include "flink/streaming/api/operators/TimestampedCollector.h"
#include "flink/streaming/api/operators/TwoInputStreamOperator.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {
namespace co {

template <class IN1, class IN2, class OUT>
class CoStreamFlatMap
    : public AbstractUdfStreamOperator<
          flink::streaming::api::functions::co::CoFlatMapFunction<IN1, IN2,
                                                                  OUT> >,
      public TwoInputStreamOperator {
 public:
  typedef flink::streaming::api::functions::co::CoFlatMapFunction<IN1, IN2, OUT>
      CoFlatMapFunction;
  typedef AbstractUdfStreamOperator<CoFlatMapFunction> Base;
  typedef flink::streaming::api::watermark::Watermark Watermark;

  explicit CoStreamFlatMap(std::shared_ptr<CoFlatMapFunction> flatMapper)
      : Base(flatMapper),
        combinedWatermark_(std::numeric_limits<int64_t>::min()),
        input1Watermark_(std::numeric_limits<int64_t>::min()),
        input2Watermark_(std::numeric_limits<int64_t>::min()) {}

  std::string getSimpleName() const override { return "CoStreamFlatMap"; }

  // 1.0:50
  void open() override {
    Base::open();
    collector_.reset(new TimestampedCollector<OUT>(this->output_));
  }

  // 1.0:56
  void processElement1(const StreamRecord& element) override {
    collector_->setTimestamp(element);
    this->userFunction_->flatMap1(element.template getValueAs<IN1>(),
                                  *collector_);
  }

  // 1.0:62
  void processElement2(const StreamRecord& element) override {
    collector_->setTimestamp(element);
    this->userFunction_->flatMap2(element.template getValueAs<IN2>(),
                                  *collector_);
  }

  // 1.0:68
  void processWatermark1(const Watermark& mark) override {
    input1Watermark_ = mark.getTimestamp();
    const int64_t newMin = std::min(input1Watermark_, input2Watermark_);
    if (newMin > combinedWatermark_) {
      combinedWatermark_ = newMin;
      this->output_->emitWatermark(Watermark(combinedWatermark_));
    }
  }

  // 1.0:77
  void processWatermark2(const Watermark& mark) override {
    input2Watermark_ = mark.getTimestamp();
    const int64_t newMin = std::min(input1Watermark_, input2Watermark_);
    if (newMin > combinedWatermark_) {
      combinedWatermark_ = newMin;
      this->output_->emitWatermark(Watermark(combinedWatermark_));
    }
  }

 protected:
  // 1.0:86 protected TimestampedCollector<OUT> getCollector()
  TimestampedCollector<OUT>* getCollector() { return collector_.get(); }

 private:
  std::unique_ptr<TimestampedCollector<OUT> > collector_;
  int64_t combinedWatermark_;
  int64_t input1Watermark_;
  int64_t input2Watermark_;
};

}  // namespace co
}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_CO_COSTREAMFLATMAP_H
