/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.TwoInputTransformation
 *
 * 1.0 class TwoInputTransformation<IN1, IN2, OUT>
 *         extends StreamTransformation<OUT>（TwoInputTransformation.java:37）
 *
 * 1.0 类注释：「This Transformation represents the application of a
 * TwoInputStreamOperator to two input StreamTransformations. The result is
 * again only one stream.」
 *
 *   private final StreamTransformation<IN1> input1;
 *   private final StreamTransformation<IN2> input2;
 *   private final TwoInputStreamOperator<IN1, IN2, OUT> operator;
 *   private KeySelector<IN1, ?> stateKeySelector1;
 *   private KeySelector<IN2, ?> stateKeySelector2;
 *   private TypeInformation<?> stateKeyType;
 *
 *   getInputType1() -> input1.getOutputType()
 *   getInputType2() -> input2.getOutputType()
 *   setStateKeySelectors(sel1, sel2)   —— 注意是**一起**设的（单个 setter 没有）
 *   getTransitivePredecessors():
 *       result.add(this);
 *       result.addAll(input1.getTransitivePredecessors());
 *       result.addAll(input2.getTransitivePredecessors());
 *   setChainingStrategy(s) -> operator.setChainingStrategy(s)
 *
 * 【偏差】同 OneInputTransformation.h。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_TWOINPUTTRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_TWOINPUTTRANSFORMATION_H

#include "flink/api/java/functions/AnyKeySelector.h"
#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/api/operators/TwoInputStreamOperator.h"
#include "flink/streaming/api/transformations/StreamTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class TwoInputTransformation : public StreamTransformation {
 public:
  typedef std::shared_ptr<flink::streaming::api::operators::StreamOperator>
      StreamOperatorPtr;
  typedef std::shared_ptr<StreamTransformation> TransformationPtr;
  typedef std::shared_ptr<flink::api::java::functions::AnyKeySelector>
      KeySelectorPtr;

  TwoInputTransformation(const TransformationPtr& input1,
                         const TransformationPtr& input2,
                         const std::string& name,
                         const StreamOperatorPtr& streamOperator,
                         const SerializerPtr& outputType, int parallelism)
      : StreamTransformation(name, outputType, parallelism),
        input1_(input1),
        input2_(input2),
        operator_(streamOperator) {
    // 1.0 由 TwoInputStreamOperator<IN1, IN2, OUT> 的类型保证
    if (!streamOperator ||
        dynamic_cast<flink::streaming::api::operators::TwoInputStreamOperator*>(
            streamOperator.get()) == NULL) {
      throw std::invalid_argument(
          "The operator of a TwoInputTransformation must be a "
          "TwoInputStreamOperator");
    }
  }

  const TransformationPtr& getInput1() const { return input1_; }
  const TransformationPtr& getInput2() const { return input2_; }
  const SerializerPtr& getInputType1() const {
    return input1_->getOutputType();
  }
  const SerializerPtr& getInputType2() const {
    return input2_->getOutputType();
  }

  const StreamOperatorPtr& getOperator() const { return operator_; }

  // 1.0 setStateKeySelectors(sel1, sel2) —— 两个一起设
  void setStateKeySelectors(const KeySelectorPtr& stateKeySelector1,
                            const KeySelectorPtr& stateKeySelector2) {
    stateKeySelector1_ = stateKeySelector1;
    stateKeySelector2_ = stateKeySelector2;
  }
  const KeySelectorPtr& getStateKeySelector1() const {
    return stateKeySelector1_;
  }
  const KeySelectorPtr& getStateKeySelector2() const {
    return stateKeySelector2_;
  }

  void setStateKeyType(const SerializerPtr& stateKeyType) {
    stateKeyType_ = stateKeyType;
  }
  const SerializerPtr& getStateKeyType() const { return stateKeyType_; }

  std::string getSimpleName() const override {
    return "TwoInputTransformation";
  }

  std::vector<StreamTransformation*> getTransitivePredecessors() override {
    std::vector<StreamTransformation*> result;
    result.push_back(this);
    const std::vector<StreamTransformation*> predecessors1 =
        input1_->getTransitivePredecessors();
    result.insert(result.end(), predecessors1.begin(), predecessors1.end());
    const std::vector<StreamTransformation*> predecessors2 =
        input2_->getTransitivePredecessors();
    result.insert(result.end(), predecessors2.begin(), predecessors2.end());
    return result;
  }

  void setChainingStrategy(ChainingStrategy strategy) override {
    operator_->setChainingStrategy(strategy);
  }

 private:
  const TransformationPtr input1_;
  const TransformationPtr input2_;
  const StreamOperatorPtr operator_;
  KeySelectorPtr stateKeySelector1_;
  KeySelectorPtr stateKeySelector2_;
  SerializerPtr stateKeyType_;
};

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_TWOINPUTTRANSFORMATION_H
