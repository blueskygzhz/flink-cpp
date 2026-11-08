/*
 * 【与 Flink 1.0 的偏差】类型擦除后的 OutputSelector。
 *
 * 1.0 的 StreamNode / StreamConfig 存的是 `List<OutputSelector<?>>`，
 * DirectedOutput 再 raw cast 成 `OutputSelector<OUT>` 调
 * select(record.getValue())。靠的是 Java 的 Object 根类型 + 泛型擦除。
 *
 * C++ 这边 StreamRecord 的值是 Any，DirectedOutput 也不知道 OUT 的具体类型，
 * 所以需要一个「不带类型参数」的 OutputSelector 接口，由 api 层在编译期把
 * OutputSelector<OUT> 包进来（见 TypedAnyOutputSelector）—— 与
 * AnyKeySelector / AnySerializer 完全同构的手法。
 *
 * 对应 Java: OutputSelector<?> 的使用位置。
 */
#ifndef FLINK_STREAMING_API_COLLECTOR_SELECTOR_ANYOUTPUTSELECTOR_H
#define FLINK_STREAMING_API_COLLECTOR_SELECTOR_ANYOUTPUTSELECTOR_H

#include <string>
#include <vector>

#include "flink/types/Any.h"

namespace flink {
namespace streaming {
namespace api {
namespace collector {
namespace selector {

class AnyOutputSelector {
 public:
  virtual ~AnyOutputSelector() {}

  virtual std::vector<std::string> select(const flink::types::Any& value) = 0;
};

}  // namespace selector
}  // namespace collector
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_COLLECTOR_SELECTOR_ANYOUTPUTSELECTOR_H
