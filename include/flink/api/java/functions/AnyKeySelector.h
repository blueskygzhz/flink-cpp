/*
 * 【与 Flink 1.0 的偏差】类型擦除后的 KeySelector。
 *
 * 1.0 的 StreamConfig 存的是 `KeySelector<?, Serializable>`，调用点
 * （AbstractStreamOperator.setKeyContextElement1）再 raw cast 成
 * `(KeySelector) stateKeySelector1` 调 getKey(record.getValue())。
 * 这条路走得通是因为 Java 有 Object 根类型 + 泛型擦除。
 *
 * C++ 这边 StreamRecord 的值是 Any，算子也不知道 IN/KEY 的具体类型，所以需要一个
 * 「不带类型参数」的 KeySelector 接口，由 api 层在编译期把 KeySelector<IN, KEY>
 * 包进来（见 TypedAnyKeySelector）—— 与 AnySerializer / TypedAnySerializer 完全
 * 同构的手法。
 *
 * 对应 Java: KeySelector<?, Serializable> 的使用位置。
 */
#ifndef FLINK_API_JAVA_FUNCTIONS_ANYKEYSELECTOR_H
#define FLINK_API_JAVA_FUNCTIONS_ANYKEYSELECTOR_H

#include "flink/api/common/functions/Function.h"
#include "flink/types/Any.h"

namespace flink {
namespace api {
namespace java {
namespace functions {

class AnyKeySelector : public flink::api::common::functions::Function {
 public:
  virtual flink::types::Any getKey(const flink::types::Any& value) = 0;
};

}  // namespace functions
}  // namespace java
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_JAVA_FUNCTIONS_ANYKEYSELECTOR_H
