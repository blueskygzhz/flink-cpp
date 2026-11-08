/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.SelectTransformation
 *
 * 1.0 class SelectTransformation<T> extends StreamTransformation<T>
 * （SelectTransformation.java:39）
 *
 * 1.0 类注释：「This transformation represents a selection of only certain
 * upstream elements. This must follow a SplitTransformation that splits
 * elements into several logical streams with assigned names.
 * This does not create a physical operation, it only affects how upstream
 * operations are connected to downstream operations.」
 *   —— 即 StreamGraph 的 virtualSelectNodes（见 StreamGraph.h 机制 ①）。
 *
 * 构造（:52-57）：
 *   super("Select", input.getOutputType(), input.getParallelism());
 *   1.0 javadoc：「Creates a new SelectionTransformation from the given input
 *   that only selects the streams with the selected names.」
 *   @param selectedNames「The names from the upstream SplitTransformation that
 *   this SelectTransformation selects.」
 *
 * getTransitivePredecessors()（:73-79）：this + input 的递归结果。
 *
 * setChainingStrategy（:81-84）：
 *   throw new UnsupportedOperationException(
 *       "Cannot set chaining strategy on Select Transformation.");
 *
 * 【偏差】类型擦除（无 <T>）+ input 用 shared_ptr 持有，见
 * StreamTransformation.h 偏差 1；selectedNames 从 List<String> 换成
 * std::vector<std::string>（1.0 也只是顺序遍历一次塞进
 * StreamGraph.addEdge(..., selectedNames)）。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_SELECTTRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_SELECTTRANSFORMATION_H

#include "flink/streaming/api/transformations/StreamTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class SelectTransformation : public StreamTransformation {
 public:
  typedef std::shared_ptr<StreamTransformation> TransformationPtr;

  // 1.0 SelectTransformation(StreamTransformation<T> input,
  //                          List<String> selectedNames)
  SelectTransformation(const TransformationPtr& input,
                       const std::vector<std::string>& selectedNames)
      : StreamTransformation("Select", input->getOutputType(),
                             input->getParallelism()),
        input_(input),
        selectedNames_(selectedNames) {}

  // 1.0「Returns the input StreamTransformation.」
  const TransformationPtr& getInput() const { return input_; }

  // 1.0「Returns the names of the split streams that this SelectTransformation
  // selects.」
  const std::vector<std::string>& getSelectedNames() const {
    return selectedNames_;
  }

  std::string getSimpleName() const override { return "SelectTransformation"; }

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
        "Cannot set chaining strategy on Select Transformation.");
  }

 private:
  const TransformationPtr input_;
  const std::vector<std::string> selectedNames_;
};

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_SELECTTRANSFORMATION_H
