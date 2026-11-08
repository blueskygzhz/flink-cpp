/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.SinkTransformation
 *
 * 1.0 class SinkTransformation<T> extends StreamTransformation<Object>
 * （SinkTransformation.java:33）
 *
 * 1.0 类注释：「This Transformation represents a Sink.」
 *
 *   private final StreamTransformation<T> input;
 *   private final StreamSink<T> operator;
 *   // 1.0 原注释：
 *   // We need this because sinks can also have state that is partitioned by key
 *   private KeySelector<T, ?> stateKeySelector;
 *   private TypeInformation<?> stateKeyType;
 *
 *   SinkTransformation(input, name, StreamSink<T> operator, int parallelism) {
 *       super(name, TypeExtractor.getForClass(Object.class), parallelism);
 *       ...
 *   }
 *   —— 注意**输出类型固定是 Object**（继承的是 StreamTransformation<Object>），
 *   因为 sink 不产出任何东西。StreamGraphGenerator.transformSink 建顶点时传的
 *   outSerializer 也是 null。
 *
 *   setChainingStrategy(s) -> operator.setChainingStrategy(s)
 *
 * 【偏差】1.0 的 `TypeExtractor.getForClass(Object.class)` 这里传空
 * SerializerPtr：本仓库没有 TypeInformation 体系，而「Object 类型」本身也没有
 * 可用的 serializer（sink 的 outputType 从不被真正用到 —— generator 给
 * addSink 传的 out 是 null）。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_SINKTRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_SINKTRANSFORMATION_H

#include "flink/api/java/functions/AnyKeySelector.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"
#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/api/transformations/StreamTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class SinkTransformation : public StreamTransformation {
 public:
  typedef std::shared_ptr<flink::streaming::api::operators::StreamOperator>
      StreamOperatorPtr;
  typedef std::shared_ptr<StreamTransformation> TransformationPtr;
  typedef std::shared_ptr<flink::api::java::functions::AnyKeySelector>
      KeySelectorPtr;

  SinkTransformation(const TransformationPtr& input, const std::string& name,
                     const StreamOperatorPtr& streamOperator, int parallelism)
      // 1.0: super(name, TypeExtractor.getForClass(Object.class), parallelism)
      : StreamTransformation(name, SerializerPtr(), parallelism),
        input_(input),
        operator_(streamOperator) {
    // 1.0 由 StreamSink<T> 的类型保证（StreamSink implements
    // OneInputStreamOperator）
    if (!streamOperator ||
        dynamic_cast<flink::streaming::api::operators::OneInputStreamOperator*>(
            streamOperator.get()) == NULL) {
      throw std::invalid_argument(
          "The operator of a SinkTransformation must be a StreamSink");
    }
  }

  const TransformationPtr& getInput() const { return input_; }
  const StreamOperatorPtr& getOperator() const { return operator_; }

  // 1.0: "We need this because sinks can also have state that is partitioned
  // by key"
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

  std::string getSimpleName() const override { return "SinkTransformation"; }

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
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_SINKTRANSFORMATION_H
