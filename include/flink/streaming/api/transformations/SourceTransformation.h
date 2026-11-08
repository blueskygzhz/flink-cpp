/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.SourceTransformation
 *
 * 1.0 class SourceTransformation<T> extends StreamTransformation<T>
 * （SourceTransformation.java:30）
 *
 * 1.0 类注释：「This represents a Source. This does not actually transform
 * anything since it has no inputs but it is the root StreamTransformation of
 * any topology.」
 *
 *   private final StreamSource<T, ?> operator;
 *   SourceTransformation(String name, StreamSource<T,?> operator,
 *                        TypeInformation<T> outputType, int parallelism) {
 *       super(name, outputType, parallelism);  this.operator = operator; }
 *   getTransitivePredecessors() -> Collections.singleton(this)
 *       —— source 没有输入，所以只有自己
 *   setChainingStrategy(strategy) -> operator.setChainingStrategy(strategy)
 *
 * 【偏差】1.0 的 StreamSource<T, ?> 这里换成 StreamOperatorPtr（擦除类型，
 * 见 StreamTransformation.h 偏差 1）。构造里用 dynamic_cast 校验它确实是
 * 一个 SourceOperator —— 等价于 1.0 由类型系统保证。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_SOURCETRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_SOURCETRANSFORMATION_H

#include "flink/streaming/api/operators/SourceOperator.h"
#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/api/transformations/StreamTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class SourceTransformation : public StreamTransformation {
 public:
  typedef std::shared_ptr<flink::streaming::api::operators::StreamOperator>
      StreamOperatorPtr;

  SourceTransformation(const std::string& name,
                       const StreamOperatorPtr& streamOperator,
                       const SerializerPtr& outputType, int parallelism)
      : StreamTransformation(name, outputType, parallelism),
        operator_(streamOperator) {
    // 1.0 由 StreamSource<T,?> 的类型保证
    if (!streamOperator ||
        dynamic_cast<flink::streaming::api::operators::SourceOperator*>(
            streamOperator.get()) == NULL) {
      throw std::invalid_argument(
          "The operator of a SourceTransformation must be a StreamSource");
    }
  }

  const StreamOperatorPtr& getOperator() const { return operator_; }

  std::string getSimpleName() const override { return "SourceTransformation"; }

  // 1.0: Collections.<StreamTransformation<?>>singleton(this)
  std::vector<StreamTransformation*> getTransitivePredecessors() override {
    return std::vector<StreamTransformation*>(1, this);
  }

  void setChainingStrategy(ChainingStrategy strategy) override {
    operator_->setChainingStrategy(strategy);
  }

 private:
  const StreamOperatorPtr operator_;
};

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_SOURCETRANSFORMATION_H
