/*
 * flink-streaming-java : org.apache.flink.streaming.api.operators.co.CoStreamMap
 *
 * 1.0 CoStreamMap.java:31
 *   class CoStreamMap<IN1, IN2, OUT>
 *       extends AbstractUdfStreamOperator<OUT, CoMapFunction<IN1, IN2, OUT>>
 *       implements TwoInputStreamOperator<IN1, IN2, OUT>
 *
 * 字段（:37-41），1.0 的原注释：
 *   // We keep track of watermarks from both inputs, the combined input is the
 *   // minimum
 *   // Once the minimum advances we emit a new watermark for downstream
 *   // operators
 *   private long combinedWatermark = Long.MIN_VALUE;
 *   private long input1Watermark   = Long.MIN_VALUE;
 *   private long input2Watermark   = Long.MIN_VALUE;
 *
 * CoStreamMap(CoMapFunction<IN1,IN2,OUT> mapper) { super(mapper); }   (:43)
 *   —— 注意 1.0 这里**没有**设 chainingStrategy，所以沿用
 *   AbstractStreamOperator 的默认值 ChainingStrategy.HEAD（与 StreamMap /
 *   StreamFlatMap 构造里显式设 ALWAYS 不同）。本实现照搬：不动
 *   chainingStrategy_。
 *
 * processElement1(StreamRecord<IN1> e)（:48）：
 *   output.collect(e.replace(userFunction.map1(e.getValue())));
 * processElement2(StreamRecord<IN2> e)（:53）：
 *   output.collect(e.replace(userFunction.map2(e.getValue())));
 *   —— replace 保留输入记录的时间戳，这就是 1.0 TwoInputStreamTaskTest
 *   「CoStreamMap assigns the input timestamp to emitted elements」那条注释
 *   （TwoInputStreamTaskTest.java:57）说的事。
 *
 * processWatermark1(Watermark mark)（:58）/ processWatermark2(:67)：
 *   inputNWatermark = mark.getTimestamp();
 *   long newMin = Math.min(input1Watermark, input2Watermark);
 *   if (newMin > combinedWatermark) {
 *       combinedWatermark = newMin;
 *       output.emitWatermark(new Watermark(combinedWatermark));
 *   }
 *   —— 两条输入之间取 min 是**算子**的责任；各自输入内部多通道取 min 是
 *   StreamTwoInputProcessor 的责任。两层 min 叠起来才是完整的水位线语义。
 *
 * 【偏差】1.0 的 <IN1, IN2, OUT> 三个类型参数这里保留为模板参数，但接口
 * TwoInputStreamOperator 上不带（见该头文件的偏差说明）。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_CO_COSTREAMMAP_H
#define FLINK_STREAMING_API_OPERATORS_CO_COSTREAMMAP_H

#include <algorithm>
#include <limits>
#include <memory>

#include "flink/streaming/api/functions/co/CoMapFunction.h"
#include "flink/streaming/api/operators/AbstractUdfStreamOperator.h"
#include "flink/streaming/api/operators/TwoInputStreamOperator.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {
namespace co {

template <class IN1, class IN2, class OUT>
class CoStreamMap
    : public AbstractUdfStreamOperator<
          flink::streaming::api::functions::co::CoMapFunction<IN1, IN2, OUT> >,
      public TwoInputStreamOperator {
 public:
  typedef flink::streaming::api::functions::co::CoMapFunction<IN1, IN2, OUT>
      CoMapFunction;
  typedef AbstractUdfStreamOperator<CoMapFunction> Base;
  typedef flink::streaming::api::watermark::Watermark Watermark;

  explicit CoStreamMap(std::shared_ptr<CoMapFunction> mapper)
      : Base(mapper),
        combinedWatermark_(std::numeric_limits<int64_t>::min()),
        input1Watermark_(std::numeric_limits<int64_t>::min()),
        input2Watermark_(std::numeric_limits<int64_t>::min()) {
    // 1.0 CoStreamMap 的构造函数只有 super(mapper)：chainingStrategy 留默认
    // 的 HEAD，这里同样不动
  }

  std::string getSimpleName() const override { return "CoStreamMap"; }

  // 1.0:48
  void processElement1(const StreamRecord& element) override {
    this->output_->collect(element.replace(flink::types::Any::of(
        this->userFunction_->map1(element.template getValueAs<IN1>()))));
  }

  // 1.0:53
  void processElement2(const StreamRecord& element) override {
    this->output_->collect(element.replace(flink::types::Any::of(
        this->userFunction_->map2(element.template getValueAs<IN2>()))));
  }

  // 1.0:58
  void processWatermark1(const Watermark& mark) override {
    input1Watermark_ = mark.getTimestamp();
    const int64_t newMin = std::min(input1Watermark_, input2Watermark_);
    if (newMin > combinedWatermark_) {
      combinedWatermark_ = newMin;
      this->output_->emitWatermark(Watermark(combinedWatermark_));
    }
  }

  // 1.0:67
  void processWatermark2(const Watermark& mark) override {
    input2Watermark_ = mark.getTimestamp();
    const int64_t newMin = std::min(input1Watermark_, input2Watermark_);
    if (newMin > combinedWatermark_) {
      combinedWatermark_ = newMin;
      this->output_->emitWatermark(Watermark(combinedWatermark_));
    }
  }

 private:
  int64_t combinedWatermark_;
  int64_t input1Watermark_;
  int64_t input2Watermark_;
};

}  // namespace co
}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_CO_COSTREAMMAP_H
