/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.OneInputTransformation
 *
 * 1.0 class OneInputTransformation<IN, OUT> extends StreamTransformation<OUT>
 * （OneInputTransformation.java:36）
 *
 * 1.0 类注释：「This Transformation represents the application of a
 * OneInputStreamOperator to one input StreamTransformation.」
 *
 *   private final StreamTransformation<IN> input;
 *   private final OneInputStreamOperator<IN, OUT> operator;
 *   private KeySelector<IN, ?> stateKeySelector;
 *   private TypeInformation<?> stateKeyType;
 *
 *   getInputType() -> input.getOutputType()
 *   setStateKeySelector / getStateKeySelector
 *   setStateKeyType    / getStateKeyType
 *   getTransitivePredecessors():
 *       result.add(this); result.addAll(input.getTransitivePredecessors());
 *   setChainingStrategy(s) -> operator.setChainingStrategy(s)
 *
 * stateKeySelector 的来路：DataStream.keyBy(...) 产出 KeyedStream，之后在它上面
 * 做的 map/flatMap 等会把 keySelector 传进这里，最终由
 * StreamGraphGenerator.transformOnInputTransform 调
 * streamGraph.setOneInputStateKey(...)。
 *
 * 【偏差】input 用 shared_ptr 持有（1.0 是 Java 引用 + GC）；operator 与
 * stateKeyType 的类型擦除见 StreamTransformation.h 偏差 1。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_ONEINPUTTRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_ONEINPUTTRANSFORMATION_H

#include "flink/api/java/functions/AnyKeySelector.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"
#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/api/transformations/StreamTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class OneInputTransformation : public StreamTransformation {
 public:
  typedef std::shared_ptr<flink::streaming::api::operators::StreamOperator>
      StreamOperatorPtr;
  typedef std::shared_ptr<StreamTransformation> TransformationPtr;
  typedef std::shared_ptr<flink::api::java::functions::AnyKeySelector>
      KeySelectorPtr;

  OneInputTransformation(const TransformationPtr& input,
                         const std::string& name,
                         const StreamOperatorPtr& streamOperator,
                         const SerializerPtr& outputType, int parallelism)
      : StreamTransformation(name, outputType, parallelism),
        input_(input),
        operator_(streamOperator) {
    // 1.0 由 OneInputStreamOperator<IN, OUT> 的类型保证
    if (!streamOperator ||
        dynamic_cast<flink::streaming::api::operators::OneInputStreamOperator*>(
            streamOperator.get()) == NULL) {
      throw std::invalid_argument(
          "The operator of a OneInputTransformation must be a "
          "OneInputStreamOperator");
    }
  }

  const TransformationPtr& getInput() const { return input_; }
  // 1.0 getInputType() { return input.getOutputType(); }
  const SerializerPtr& getInputType() const { return input_->getOutputType(); }

  const StreamOperatorPtr& getOperator() const { return operator_; }

  void setStateKeySelector(const KeySelectorPtr& stateKeySelector) {
    stateKeySelector_ = stateKeySelector;
  }
  const KeySelectorPtr& getStateKeySelector() const {
    return stateKeySelector_;
  }

  void setStateKeyType(const SerializerPtr& stateKeyType) {
    stateKeyType_ = stateKeyType;
  }
  const SerializerPtr& getStateKeyType() const { return stateKeyType_; }

  std::string getSimpleName() const override {
    return "OneInputTransformation";
  }

  std::vector<StreamTransformation*> getTransitivePredecessors() override {
    std::vector<StreamTransformation*> result;
    result.push_back(this);
    const std::vector<StreamTransformation*> inputPredecessors =
        input_->getTransitivePredecessors();
    result.insert(result.end(), inputPredecessors.begin(),
                  inputPredecessors.end());
    return result;
  }

  void setChainingStrategy(ChainingStrategy strategy) override {
    operator_->setChainingStrategy(strategy);
  }

 private:
  const TransformationPtr input_;
  const StreamOperatorPtr operator_;
  KeySelectorPtr stateKeySelector_;
  SerializerPtr stateKeyType_;
};

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_ONEINPUTTRANSFORMATION_H
