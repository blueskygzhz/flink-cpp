/*
 * 【与 Flink 1.0 的偏差】把编译期已知的 OutputSelector<OUT> 适配成
 * AnyOutputSelector。见 AnyOutputSelector.h 的说明。
 *
 * 1.0 对应的不是某个类，而是 DataStream.split(OutputSelector) -> StreamGraph
 * -> StreamNode.addOutputSelector -> StreamConfig.setOutputSelectors
 * 这条「把用户的 OutputSelector 存进配置、类型信息随擦除丢掉」的路径。
 */
#ifndef FLINK_STREAMING_API_COLLECTOR_SELECTOR_TYPEDANYOUTPUTSELECTOR_H
#define FLINK_STREAMING_API_COLLECTOR_SELECTOR_TYPEDANYOUTPUTSELECTOR_H

#include <memory>

#include "flink/streaming/api/collector/selector/AnyOutputSelector.h"
#include "flink/streaming/api/collector/selector/OutputSelector.h"

namespace flink {
namespace streaming {
namespace api {
namespace collector {
namespace selector {

template <class OUT>
class TypedAnyOutputSelector : public AnyOutputSelector {
 public:
  explicit TypedAnyOutputSelector(
      std::shared_ptr<OutputSelector<OUT> > outputSelector)
      : outputSelector_(outputSelector) {}

  std::vector<std::string> select(const flink::types::Any& value) override {
    return outputSelector_->select(value.as<OUT>());
  }

 private:
  std::shared_ptr<OutputSelector<OUT> > outputSelector_;
};

template <class OUT>
std::shared_ptr<AnyOutputSelector> eraseOutputSelector(
    std::shared_ptr<OutputSelector<OUT> > outputSelector) {
  return std::make_shared<TypedAnyOutputSelector<OUT> >(outputSelector);
}

}  // namespace selector
}  // namespace collector
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_COLLECTOR_SELECTOR_TYPEDANYOUTPUTSELECTOR_H
