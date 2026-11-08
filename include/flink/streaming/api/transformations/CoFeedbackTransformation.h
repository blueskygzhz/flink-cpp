/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.CoFeedbackTransformation
 *
 * 1.0 class CoFeedbackTransformation<F> extends StreamTransformation<F>
 * （CoFeedbackTransformation.java:56）
 *
 * 1.0 类注释（全文）：
 *   「This represents a feedback point in a topology. The type of the feedback
 *    elements need not match the type of the upstream StreamTransformation
 *    because the only allowed operations after a CoFeedbackTransformation are
 *    TwoInputTransformations. The upstream StreamTransformation will be
 *    connected to the first input of the Co-Transform while the feedback edges
 *    will be connected to the second input.
 *    Both the partitioning of the input and the feedback edges is preserved.
 *    They can also have differing partitioning strategies. This requires,
 *    however, that the parallelism of the feedback StreamTransformations must
 *    match the parallelism of the input StreamTransformation.
 *    The upstream StreamTransformation is not wired to this
 *    CoFeedbackTransformation. It is instead directly wired to the
 *    TwoInputTransformation after this CoFeedbackTransformation.
 *    This is different from Iterations in batch processing.」
 *
 * 字段（:58-60）：
 *   private final List<StreamTransformation<F>> feedbackEdges;
 *   private final Long waitTime;
 *   —— 注意**没有** input 字段（见上面类注释最后一段的解释）。
 *
 * 构造（:71-77）：
 *   super("CoFeedback", feedbackType, parallelism);
 *   @param parallelism「The parallelism of the upstream StreamTransformatino
 *   and the feedback edges.」（1.0 原文把 StreamTransformation 拼错了）
 *   @param feedbackType「The type of the feedback edges」
 *
 * addFeedbackEdge（:86-96）：与 FeedbackTransformation 完全相同的并行度校验与
 *   完全相同的异常文案。
 *
 * getTransitivePredecessors()（:119-122）：
 *   return Collections.<StreamTransformation<?>>singleton(this);
 *   —— **只有 this**，既不含 input（根本没有 input 字段）也不含 feedbackEdges。
 *
 * setChainingStrategy（:114-117）：
 *   throw new UnsupportedOperationException(
 *       "Cannot set chaining strategy on Split Transformation.");
 *   —— 又一处 1.0 的复制粘贴笔误（应为 CoFeedback），照搬不订正。
 *
 * 【偏差】类型擦除：无 <F>，TypeInformation<F> feedbackType 换成 SerializerPtr；
 * waitTime 的 Long -> int64_t 理由同 FeedbackTransformation.h 偏差 2。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_COFEEDBACKTRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_COFEEDBACKTRANSFORMATION_H

#include "flink/streaming/api/transformations/StreamTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class CoFeedbackTransformation : public StreamTransformation {
 public:
  typedef std::shared_ptr<StreamTransformation> TransformationPtr;

  // 1.0 CoFeedbackTransformation(int parallelism,
  //                              TypeInformation<F> feedbackType,
  //                              Long waitTime)
  CoFeedbackTransformation(int parallelism, const SerializerPtr& feedbackType,
                           int64_t waitTime)
      : StreamTransformation("CoFeedback", feedbackType, parallelism),
        waitTime_(waitTime) {}

  // 1.0「Adds a feedback edge. The parallelism of the StreamTransformation must
  // match the parallelism of the input StreamTransformation of the upstream
  // StreamTransformation.」
  void addFeedbackEdge(const TransformationPtr& transform) {
    if (transform->getParallelism() != getParallelism()) {
      // 与 FeedbackTransformation 一字不差的文案（1.0 两处是复制的）
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

  // 1.0「Returns the wait time. ...」
  int64_t getWaitTime() const { return waitTime_; }

  std::string getSimpleName() const override {
    return "CoFeedbackTransformation";
  }

  // 1.0 的文案笔误见文件头注释，照搬
  void setChainingStrategy(ChainingStrategy) override {
    throw std::logic_error(
        "Cannot set chaining strategy on Split Transformation.");
  }

  // 1.0: return Collections.singleton(this);  —— 只有自己
  std::vector<StreamTransformation*> getTransitivePredecessors() override {
    return std::vector<StreamTransformation*>(1, this);
  }

 private:
  std::vector<TransformationPtr> feedbackEdges_;
  const int64_t waitTime_;
};

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_COFEEDBACKTRANSFORMATION_H
