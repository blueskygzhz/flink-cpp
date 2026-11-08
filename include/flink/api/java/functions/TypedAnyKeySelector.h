/*
 * 【与 Flink 1.0 的偏差】把编译期已知的 KeySelector<IN, KEY> 适配成
 * AnyKeySelector。见 AnyKeySelector.h 的说明。
 *
 * 1.0 对应的不是某个类，而是 DataStream.keyBy(KeySelector) -> StreamGraph
 * -> StreamConfig.setStatePartitioner(0, keySelector) 这条「把用户的
 * KeySelector 存进配置、类型信息随擦除丢掉」的路径。
 */
#ifndef FLINK_API_JAVA_FUNCTIONS_TYPEDANYKEYSELECTOR_H
#define FLINK_API_JAVA_FUNCTIONS_TYPEDANYKEYSELECTOR_H

#include <memory>

#include "flink/api/java/functions/AnyKeySelector.h"
#include "flink/api/java/functions/KeySelector.h"

namespace flink {
namespace api {
namespace java {
namespace functions {

template <class IN, class KEY>
class TypedAnyKeySelector : public AnyKeySelector {
 public:
  explicit TypedAnyKeySelector(
      std::shared_ptr<KeySelector<IN, KEY> > keySelector)
      : keySelector_(keySelector) {}

  flink::types::Any getKey(const flink::types::Any& value) override {
    return flink::types::Any::of(keySelector_->getKey(value.as<IN>()));
  }

 private:
  std::shared_ptr<KeySelector<IN, KEY> > keySelector_;
};

template <class IN, class KEY>
std::shared_ptr<AnyKeySelector> eraseKeySelector(
    std::shared_ptr<KeySelector<IN, KEY> > keySelector) {
  return std::make_shared<TypedAnyKeySelector<IN, KEY> >(keySelector);
}

}  // namespace functions
}  // namespace java
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_JAVA_FUNCTIONS_TYPEDANYKEYSELECTOR_H
