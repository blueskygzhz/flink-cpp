/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.operators.StreamGroupedReduce
 *
 * 1.0 StreamGroupedReduce.java:29
 *   class StreamGroupedReduce<IN> extends AbstractUdfStreamOperator<IN,
 *                                          ReduceFunction<IN>>
 *                                implements OneInputStreamOperator<IN, IN>
 *   private static final String STATE_NAME = "_op_state";
 *   private transient ValueState<IN> values;
 *   private TypeSerializer<IN> serializer;
 *
 *   StreamGroupedReduce(ReduceFunction<IN> reducer, TypeSerializer<IN> ser)
 *   open() { super.open();
 *            ValueStateDescriptor<IN> stateId =
 *                new ValueStateDescriptor<>(STATE_NAME, serializer, null);
 *            values = getPartitionedState(stateId); }
 *   processElement(StreamRecord<IN> e) {
 *       IN value = e.getValue();
 *       IN currentValue = values.value();
 *       if (currentValue != null) {
 *           IN reduced = userFunction.reduce(currentValue, value);
 *           values.update(reduced);
 *           output.collect(e.replace(reduced));
 *       } else {
 *           values.update(value);
 *           output.collect(e.replace(value));     // 首个元素原样输出
 *       }
 *   }
 *   processWatermark(Watermark mark) { output.emitWatermark(mark); }
 *
 * 注意 1.0 **没有**设 chainingStrategy，所以用的是 AbstractStreamOperator 的
 * 默认值 HEAD —— keyBy 之后必然要跨网络 shuffle，算子只能当链头。
 *
 * 【偏差】1.0 的 descriptor 默认值传 null，靠 `value() != null` 判首元素；
 * 这里用 ValueState.hasValue()，原因见 ValueState.h。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_STREAMGROUPEDREDUCE_H
#define FLINK_STREAMING_API_OPERATORS_STREAMGROUPEDREDUCE_H

#include <memory>
#include <string>

#include "flink/api/common/functions/ReduceFunction.h"
#include "flink/api/common/state/ValueStateDescriptor.h"
#include "flink/api/common/typeutils/TypeSerializer.h"
#include "flink/streaming/api/operators/AbstractUdfStreamOperator.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

template <class IN>
class StreamGroupedReduce
    : public AbstractUdfStreamOperator<
          flink::api::common::functions::ReduceFunction<IN> >,
      public OneInputStreamOperator {
 public:
  typedef flink::api::common::functions::ReduceFunction<IN> ReduceFunction;
  typedef AbstractUdfStreamOperator<ReduceFunction> Base;
  typedef flink::api::common::typeutils::TypeSerializer<IN> SerializerType;

  // 1.0 STATE_NAME = "_op_state"
  static const char* STATE_NAME() { return "_op_state"; }

  StreamGroupedReduce(std::shared_ptr<ReduceFunction> reducer,
                      std::shared_ptr<SerializerType> serializer)
      : Base(reducer), serializer_(serializer) {}

  std::string getSimpleName() const override { return "StreamGroupedReduce"; }

  void open() override {
    Base::open();
    flink::api::common::state::ValueStateDescriptor<IN> stateId(
        STATE_NAME(), IN(), serializer_);
    values_ = this->getPartitionedState(stateId);
  }

  void processElement(const StreamRecord& element) override {
    const IN& value = element.template getValueAs<IN>();

    if (values_->hasValue()) {
      IN reduced = this->userFunction_->reduce(values_->value(), value);
      values_->update(reduced);
      this->output_->collect(element.replace(flink::types::Any::of(reduced)));
    } else {
      values_->update(value);
      this->output_->collect(element.replace(flink::types::Any::of(value)));
    }
  }

  void processWatermark(
      const flink::streaming::api::watermark::Watermark& mark) override {
    this->output_->emitWatermark(mark);
  }

 private:
  std::shared_ptr<SerializerType> serializer_;
  std::shared_ptr<flink::api::common::state::ValueState<IN> > values_;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_STREAMGROUPEDREDUCE_H
