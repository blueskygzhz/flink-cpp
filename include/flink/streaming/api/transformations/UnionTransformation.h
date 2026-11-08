/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.UnionTransformation
 *
 * 1.0 class UnionTransformation<T> extends StreamTransformation<T>
 * （UnionTransformation.java:38）
 *
 * 1.0 类注释：「This transformation represents a union of several input
 * StreamTransformations. This does not create a physical operation, it only
 * affects how upstream operations are connected to downstream operations.」
 *
 * 构造（:49-59）：
 *   super("Union", inputs.get(0).getOutputType(), inputs.get(0).getParallelism());
 *   for (StreamTransformation<T> input: inputs) {
 *       if (!input.getOutputType().equals(getOutputType())) {
 *           throw new UnsupportedOperationException("Type mismatch in input " + input);
 *       }
 *   }
 *   this.inputs = Lists.newArrayList(inputs);
 *   —— 注意 inputs 是**拷贝**一份（Lists.newArrayList），不是直接持有传入的 List。
 *   1.0 javadoc：「The input StreamTransformations must all have the same type.」
 *
 * getTransitivePredecessors()（:68-76）：this + 每个 input 的递归结果（按顺序）。
 *
 * setChainingStrategy（:78-81）：
 *   throw new UnsupportedOperationException(
 *       "Cannot set chaining strategy on Union Transformation.");
 *   —— 这里的文案是对的（PartitionTransformation 那边才是笔误）。
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 类型一致性校验退化为「指针相等」】1.0 比的是
 * `input.getOutputType().equals(getOutputType())`，即 TypeInformation 的值相等。
 * 本仓库没有 TypeInformation，outputType 是 AnySerializer 的 shared_ptr，而
 * AnySerializer 没有 operator==。所以这里比的是 **SerializerPtr 是否指向同一
 * 对象**（或两边都为空）。实际链路上 union 的各输入类型相同时，serializer 都来自
 * 同一个 `AnySerializer::of<T>()` 单例，所以指针相等恰好等价于 1.0 的 equals；
 * 类型不同则必然是不同对象，照样抛异常。
 * 另外 1.0 的异常消息里 `+ input` 会调 StreamTransformation.toString()，
 * 本实现用等价的 toString()（见 StreamTransformation.h 偏差 1）。
 *
 * 【偏差 2 —— 空 inputs】1.0 的 `inputs.get(0)` 在空 List 上会抛
 * IndexOutOfBoundsException；C++ 的 `inputs[0]` 是未定义行为。这里显式检查并抛
 * std::invalid_argument。调用方（DataStream.union）永远不会传空。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_UNIONTRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_UNIONTRANSFORMATION_H

#include "flink/streaming/api/transformations/StreamTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class UnionTransformation : public StreamTransformation {
 public:
  typedef std::shared_ptr<StreamTransformation> TransformationPtr;

  // 1.0 UnionTransformation(List<StreamTransformation<T>> inputs)
  explicit UnionTransformation(const std::vector<TransformationPtr>& inputs)
      : StreamTransformation("Union", firstOutputType(inputs),
                             firstParallelism(inputs)),
        inputs_(inputs) {  // 1.0: Lists.newArrayList(inputs)，拷贝一份
    for (std::size_t i = 0; i < inputs_.size(); ++i) {
      // 1.0: if (!input.getOutputType().equals(getOutputType()))
      // 【偏差 1】比指针
      if (inputs_[i]->getOutputType() != getOutputType()) {
        throw std::logic_error("Type mismatch in input " +
                               inputs_[i]->toString());
      }
    }
  }

  // 1.0「Returns the list of input StreamTransformations.」
  const std::vector<TransformationPtr>& getInputs() const { return inputs_; }

  std::string getSimpleName() const override { return "UnionTransformation"; }

  std::vector<StreamTransformation*> getTransitivePredecessors() override {
    std::vector<StreamTransformation*> result;
    result.push_back(this);
    for (std::size_t i = 0; i < inputs_.size(); ++i) {
      const std::vector<StreamTransformation*> inputPredecessors =
          inputs_[i]->getTransitivePredecessors();
      result.insert(result.end(), inputPredecessors.begin(),
                    inputPredecessors.end());
    }
    return result;
  }

  void setChainingStrategy(ChainingStrategy) override {
    throw std::logic_error(
        "Cannot set chaining strategy on Union Transformation.");
  }

 private:
  // 1.0 直接写 inputs.get(0).getOutputType() / .getParallelism()；这里要在成员
  // 初始化列表里用，所以拆成两个静态辅助（基类构造实参的求值顺序未定义，故两边
  // 都做【偏差 2】的空检查）
  static void checkNotEmpty(const std::vector<TransformationPtr>& inputs) {
    if (inputs.empty()) {
      throw std::invalid_argument(
          "A UnionTransformation must have at least one input.");
    }
  }
  static SerializerPtr firstOutputType(
      const std::vector<TransformationPtr>& inputs) {
    checkNotEmpty(inputs);
    return inputs[0]->getOutputType();
  }
  static int firstParallelism(const std::vector<TransformationPtr>& inputs) {
    checkNotEmpty(inputs);
    return inputs[0]->getParallelism();
  }

  const std::vector<TransformationPtr> inputs_;
};

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_UNIONTRANSFORMATION_H
