/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.FeedbackTransformation
 *
 * 1.0 class FeedbackTransformation<T> extends StreamTransformation<T>
 * （FeedbackTransformation.java:49）
 *
 * 1.0 类注释（全文）：
 *   「This represents a feedback point in a topology.
 *    This is different from how iterations work in batch processing. Once a
 *    feedback point is defined you can connect one or several
 *    StreamTransformations as a feedback edges. Operations downstream from the
 *    feedback point will receive elements from the input of this feedback point
 *    and from the feedback edges.
 *    Both the partitioning of the input and the feedback edges is preserved.
 *    They can also have differing partitioning strategies. This requires,
 *    however, that the parallelism of the feedback StreamTransformations must
 *    match the parallelism of the input StreamTransformation.
 *    The type of the input StreamTransformation and the feedback
 *    StreamTransformation must match.」
 *
 * 字段（:51-55）：
 *   private final StreamTransformation<T> input;
 *   private final List<StreamTransformation<T>> feedbackEdges;
 *   private final Long waitTime;
 *
 * 构造（:64-69）：
 *   super("Feedback", input.getOutputType(), input.getParallelism());
 *   feedbackEdges = Lists.newArrayList();   // 空表，靠 addFeedbackEdge 填
 *   @param waitTime「The wait time of the feedback operator. After the time
 *   expires the operation will close and not receive any more feedback
 *   elements.」
 *
 * addFeedbackEdge（:85-95）：
 *   if (transform.getParallelism() != this.getParallelism()) {
 *       throw new UnsupportedOperationException(
 *           "Parallelism of the feedback stream must match the parallelism of
 *            the original stream. Parallelism of original stream: " +
 *            this.getParallelism() + "; parallelism of feedback stream: " +
 *            transform.getParallelism());
 *   }
 *   feedbackEdges.add(transform);
 *
 * getTransitivePredecessors()（:118-124）：this + input 的递归结果
 *   （**不含** feedbackEdges —— 1.0 就是这样：反馈边是「回头路」，把它算进前驱会
 *   成环。StreamGraphGenerator.transformFeedback 单独遍历 getFeedbackEdges()。）
 *
 * setChainingStrategy（:113-116）：
 *   throw new UnsupportedOperationException(
 *       "Cannot set chaining strategy on Split Transformation.");
 *
 * 【注意 —— 1.0 原文的笔误】文案写的是「Split Transformation」，但这里是
 * FeedbackTransformation。与 PartitionTransformation 的「Union Transformation」
 * 同属 1.0 的复制粘贴笔误，照搬不订正。
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 类型擦除】无 <T>，input / feedbackEdges 用 shared_ptr 持有，
 * 见 StreamTransformation.h 偏差 1。
 *
 * 【偏差 2 —— Long waitTime 换成 int64_t】1.0 的 `Long` 可以是 null，但实际
 * 永远不是：DataStream.iterate() 调 iterate(0)，iterate(long) 把值直接传进来。
 * 下游 StreamGraph.createIterationSourceAndSink 的签名本就是原始 long
 * （见 StreamGraph.h:337），所以用 int64_t 无损。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_FEEDBACKTRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_FEEDBACKTRANSFORMATION_H

#include "flink/streaming/api/transformations/StreamTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class FeedbackTransformation : public StreamTransformation {
 public:
  typedef std::shared_ptr<StreamTransformation> TransformationPtr;

  // 1.0 FeedbackTransformation(StreamTransformation<T> input, Long waitTime)
  FeedbackTransformation(const TransformationPtr& input, int64_t waitTime)
      : StreamTransformation("Feedback", input->getOutputType(),
                             input->getParallelism()),
        input_(input),
        waitTime_(waitTime) {}

  // 1.0「Returns the input StreamTransformation of this
  // FeedbackTransformation.」
  const TransformationPtr& getInput() const { return input_; }

  // 1.0「Adds a feedback edge. The parallelism of the StreamTransformation must
  // match the parallelism of the input StreamTransformation of this
  // FeedbackTransformation」
  void addFeedbackEdge(const TransformationPtr& transform) {
    if (transform->getParallelism() != getParallelism()) {
      // 1.0 的文案（三段拼接）原样照抄
      throw std::logic_error(
          "Parallelism of the feedback stream must match the parallelism of "
          "the original stream. Parallelism of original stream: " +
          std::to_string(getParallelism()) +
          "; parallelism of feedback stream: " +
          std::to_string(transform->getParallelism()));
    }
    feedbackEdges_.push_back(transform);
  }

  // 1.0「Returns the list of feedback StreamTransformations.」
  const std::vector<TransformationPtr>& getFeedbackEdges() const {
    return feedbackEdges_;
  }

  // 1.0「Returns the wait time. This is the amount of time that the feedback
  // operator keeps listening for feedback elements. Once the time expires the
  // operation will close and will not receive further elements.」
  int64_t getWaitTime() const { return waitTime_; }

  std::string getSimpleName() const override {
    return "FeedbackTransformation";
  }

  // 1.0 的文案笔误见文件头注释，照搬
  void setChainingStrategy(ChainingStrategy) override {
    throw std::logic_error(
        "Cannot set chaining strategy on Split Transformation.");
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

 private:
  const TransformationPtr input_;
  std::vector<TransformationPtr> feedbackEdges_;
  const int64_t waitTime_;
};

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_FEEDBACKTRANSFORMATION_H
