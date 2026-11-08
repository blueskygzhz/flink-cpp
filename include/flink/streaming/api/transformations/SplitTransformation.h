/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.SplitTransformation
 *
 * 1.0 class SplitTransformation<T> extends StreamTransformation<T>
 * （SplitTransformation.java:40）
 *
 * 1.0 类注释：「This transformation represents a split of one DataStream into
 * several DataStreams using an OutputSelector.
 * This does not create a physical operation, it only affects how upstream
 * operations are connected to downstream operations.」
 *
 * 构造（:52-57）：
 *   super("Split", input.getOutputType(), input.getParallelism());
 *   this.input = input;
 *   this.outputSelector = outputSelector;
 *
 * getTransitivePredecessors()（:73-79）：this + input 的递归结果。
 *
 * setChainingStrategy（:81-84）：
 *   throw new UnsupportedOperationException(
 *       "Cannot set chaining strategy on Split Transformation.");
 *
 * 注：split 本身**不**在 StreamGraph 里建虚拟节点，而是由
 * StreamGraphGenerator.transformSplit 把 outputSelector 挂到上游**真实顶点**上
 * （streamGraph.addOutputSelector(...)，穿透虚拟节点），再返回 input 的 id。
 * 所以 SplitTransformation 在 StreamGraph 里没有自己的 id。
 *
 * 【偏差】类型擦除（无 <T>）：OutputSelector<T> 换成 AnyOutputSelector
 * （与 StreamNode / StreamConfig 持有的类型一致，见 StreamNode.h 偏差 4）；
 * input 用 shared_ptr 持有。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_SPLITTRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_SPLITTRANSFORMATION_H

#include "flink/streaming/api/collector/selector/AnyOutputSelector.h"
#include "flink/streaming/api/transformations/StreamTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class SplitTransformation : public StreamTransformation {
 public:
  typedef std::shared_ptr<StreamTransformation> TransformationPtr;
  typedef std::shared_ptr<
      flink::streaming::api::collector::selector::AnyOutputSelector>
      OutputSelectorPtr;

  // 1.0 SplitTransformation(StreamTransformation<T> input,
  //                         OutputSelector<T> outputSelector)
  SplitTransformation(const TransformationPtr& input,
                      const OutputSelectorPtr& outputSelector)
      : StreamTransformation("Split", input->getOutputType(),
                             input->getParallelism()),
        input_(input),
        outputSelector_(outputSelector) {}

  // 1.0「Returns the input StreamTransformation.」
  const TransformationPtr& getInput() const { return input_; }

  // 1.0「Returns the OutputSelector」
  const OutputSelectorPtr& getOutputSelector() const { return outputSelector_; }

  std::string getSimpleName() const override { return "SplitTransformation"; }

  std::vector<StreamTransformation*> getTransitivePredecessors() override {
    std::vector<StreamTransformation*> result;
    result.push_back(this);
    const std::vector<StreamTransformation*> inputPredecessors =
        input_->getTransitivePredecessors();
    result.insert(result.end(), inputPredecessors.begin(),
                  inputPredecessors.end());
    return result;
  }

  void setChainingStrategy(ChainingStrategy) override {
    throw std::logic_error(
        "Cannot set chaining strategy on Split Transformation.");
  }

 private:
  const TransformationPtr input_;
  const OutputSelectorPtr outputSelector_;
};

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_SPLITTRANSFORMATION_H
