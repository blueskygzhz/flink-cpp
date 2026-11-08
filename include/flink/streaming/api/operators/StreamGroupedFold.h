/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.operators.StreamGroupedFold
 *
 * 1.0 StreamGroupedFold.java:37
 *   class StreamGroupedFold<IN, OUT, KEY>
 *       extends AbstractUdfStreamOperator<OUT, FoldFunction<IN, OUT>>
 *       implements OneInputStreamOperator<IN, OUT>, OutputTypeConfigurable<OUT>
 *   private static final String STATE_NAME = "_op_state";
 *   private transient ValueState<OUT> values;
 *   private transient OUT initialValue;
 *   private byte[] serializedInitialValue;
 *   private TypeSerializer<OUT> outTypeSerializer;
 *
 *   StreamGroupedFold(FoldFunction<IN,OUT> folder, OUT initialValue)
 *   open() { super.open();
 *            if (serializedInitialValue == null) throw ...;
 *            initialValue = outTypeSerializer.deserialize(in);
 *            values = getPartitionedState(
 *                new ValueStateDescriptor<>(STATE_NAME, outTypeSerializer,
 *                                           null)); }
 *   processElement(StreamRecord<IN> e) {
 *       OUT value = values.value();
 *       if (value != null) {
 *           OUT folded = userFunction.fold(outTypeSerializer.copy(value),
 *                                          e.getValue());
 *           values.update(folded);  output.collect(e.replace(folded));
 *       } else {
 *           OUT first = userFunction.fold(
 *               outTypeSerializer.copy(initialValue), e.getValue());
 *           values.update(first);   output.collect(e.replace(first));
 *       }
 *   }
 *   setOutputType(TypeInformation<OUT>, ExecutionConfig)
 *
 * 【偏差 1 —— 不复刻 serializedInitialValue / OutputTypeConfigurable】
 * 1.0 把 initialValue 先序列化成 byte[] 存着、open() 里再反序列化回来，是因为
 * 算子本身要被 Java 序列化下发到 TaskManager，而 initialValue 的类型未必
 * Serializable；serializer 也只有等 StreamGraph 定稿（setOutputType 回调）才拿得到。
 * 本实现进程内运行、serializer 由 TypeExtractor<OUT> 编译期给出，所以
 * initialValue 直接按值存着，open() 里那段 "No initial value was serialized
 * for the fold operator" 检查也随之消失。
 *
 * 【偏差 2 —— 不需要 outTypeSerializer.copy()】1.0 复制一份再交给 UDF，是怕
 * UDF 原地改掉状态里的对象（Java 是引用语义）。C++ 这边 ValueState<OUT>::value()
 * 按值返回，本就是副本。
 *
 * 【偏差 3 —— 去掉 KEY 泛型参数】1.0 的第三个参数 KEY 在类里完全没被用到
 * （历史遗留），这里不要。
 *
 * 【偏差 4】首元素判定用 hasValue()，原因见 ValueState.h。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_STREAMGROUPEDFOLD_H
#define FLINK_STREAMING_API_OPERATORS_STREAMGROUPEDFOLD_H

#include <memory>
#include <string>
#include <utility>

#include "flink/api/common/functions/FoldFunction.h"
#include "flink/api/common/state/ValueStateDescriptor.h"
#include "flink/api/common/typeutils/TypeSerializer.h"
#include "flink/streaming/api/operators/AbstractUdfStreamOperator.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

template <class IN, class OUT>
class StreamGroupedFold
    : public AbstractUdfStreamOperator<
          flink::api::common::functions::FoldFunction<IN, OUT> >,
      public OneInputStreamOperator {
 public:
  typedef flink::api::common::functions::FoldFunction<IN, OUT> FoldFunction;
  typedef AbstractUdfStreamOperator<FoldFunction> Base;
  typedef flink::api::common::typeutils::TypeSerializer<OUT> SerializerType;

  // 1.0 STATE_NAME = "_op_state"
  static const char* STATE_NAME() { return "_op_state"; }

  StreamGroupedFold(std::shared_ptr<FoldFunction> folder, OUT initialValue)
      : Base(folder), initialValue_(std::move(initialValue)) {}

  // 【偏差 1】1.0 的 outTypeSerializer 来自 setOutputType 回调；这里可显式注入，
  // 不给就由 AbstractStateBackend 按 TypeExtractor<OUT> 推导
  StreamGroupedFold(std::shared_ptr<FoldFunction> folder, OUT initialValue,
                    std::shared_ptr<SerializerType> outTypeSerializer)
      : Base(folder),
        initialValue_(std::move(initialValue)),
        outTypeSerializer_(outTypeSerializer) {}

  std::string getSimpleName() const override { return "StreamGroupedFold"; }

  void open() override {
    Base::open();
    flink::api::common::state::ValueStateDescriptor<OUT> stateId(
        STATE_NAME(), OUT(), outTypeSerializer_);
    values_ = this->getPartitionedState(stateId);
  }

  void processElement(const StreamRecord& element) override {
    if (values_->hasValue()) {
      OUT folded = this->userFunction_->fold(
          values_->value(), element.template getValueAs<IN>());
      values_->update(folded);
      this->output_->collect(element.replace(flink::types::Any::of(folded)));
    } else {
      OUT first = this->userFunction_->fold(
          initialValue_, element.template getValueAs<IN>());
      values_->update(first);
      this->output_->collect(element.replace(flink::types::Any::of(first)));
    }
  }

  void processWatermark(
      const flink::streaming::api::watermark::Watermark& mark) override {
    this->output_->emitWatermark(mark);
  }

 private:
  OUT initialValue_;
  std::shared_ptr<SerializerType> outTypeSerializer_;
  std::shared_ptr<flink::api::common::state::ValueState<OUT> > values_;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_STREAMGROUPEDFOLD_H
