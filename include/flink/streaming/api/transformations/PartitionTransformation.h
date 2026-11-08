/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.PartitionTransformation
 *
 * 1.0 class PartitionTransformation<T> extends StreamTransformation<T>
 * （PartitionTransformation.java:39）
 *
 * 1.0 类注释：「This transformation represents a change of partitioning of the
 * input elements. This does not create a physical operation, it only affects
 * how upstream operations are connected to downstream operations.」
 *   —— 即 StreamGraph 的 virtuaPartitionNodes（见 StreamGraph.h 机制 ①）。
 *
 * 构造（:51-55）：
 *   super("Partition", input.getOutputType(), input.getParallelism());
 *
 * getTransitivePredecessors()（:72-78）：this + input 的递归结果。
 *
 * setChainingStrategy（:80-83）：
 *   throw new UnsupportedOperationException(
 *       "Cannot set chaining strategy on Union Transformation.");
 *
 * 【注意 —— 1.0 原文的笔误】上面这句文案写的是「Union Transformation」，
 * 但这里是 PartitionTransformation。这是 1.0 的复制粘贴笔误（同样的笔误还出现在
 * FeedbackTransformation / CoFeedbackTransformation 里，那两处写的是「Split
 * Transformation」）。本移植**照搬原文**，不做订正 —— 移植的目标是行为与文案都
 * 与 1.0 一致，而不是修 1.0 的 bug。
 *
 * 【偏差】类型擦除（无 <T>）+ input 用 shared_ptr 持有，见
 * StreamTransformation.h 偏差 1；partitioner 用已移植的 StreamPartitioner
 * （本仓库的 StreamPartitioner 同样不带模板参数）。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_PARTITIONTRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_PARTITIONTRANSFORMATION_H

#include "flink/streaming/api/transformations/StreamTransformation.h"
#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class PartitionTransformation : public StreamTransformation {
 public:
  typedef std::shared_ptr<StreamTransformation> TransformationPtr;
  typedef std::shared_ptr<
      flink::streaming::runtime::partitioner::StreamPartitioner>
      PartitionerPtr;

  // 1.0 PartitionTransformation(StreamTransformation<T> input,
  //                             StreamPartitioner<T> partitioner)
  PartitionTransformation(const TransformationPtr& input,
                          const PartitionerPtr& partitioner)
      : StreamTransformation("Partition", input->getOutputType(),
                             input->getParallelism()),
        input_(input),
        partitioner_(partitioner) {}

  // 1.0 的 javadoc 这里也写错了：「Returns the input StreamTransformation of
  // this SinkTransformation.」（又一处复制粘贴笔误）
  const TransformationPtr& getInput() const { return input_; }

  // 1.0「Returns the StreamPartitioner that must be used for partitioning the
  // elements of the input StreamTransformation.」
  const PartitionerPtr& getPartitioner() const { return partitioner_; }

  std::string getSimpleName() const override {
    return "PartitionTransformation";
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

  // 1.0 的文案笔误见文件头注释，照搬
  void setChainingStrategy(ChainingStrategy) override {
    throw std::logic_error(
        "Cannot set chaining strategy on Union Transformation.");
  }

 private:
  const TransformationPtr input_;
  const PartitionerPtr partitioner_;
};

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_PARTITIONTRANSFORMATION_H
