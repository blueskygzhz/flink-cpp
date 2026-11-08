/*
 * 【与 Flink 1.0 的偏差】类型擦除后的 TypeSerializer。
 *
 * 1.0 的算子是泛型类（WindowOperator<K, IN, OUT, W>），编译后靠类型擦除 +
 * 运行时持有的 TypeSerializer<IN> 工作，算子代码本身可以是非泛型的字节码。
 * C++ 这边运行时流转的是 Any，所以需要一个「不带类型参数」的序列化器接口，
 * 由 api 层在编译期把具体的 TypeSerializer<T> 包进来（见 TypedAnySerializer）。
 *
 * 对应 Java: TypeSerializer<Object> 的使用位置（状态快照 / 网络序列化）。
 */
#ifndef FLINK_API_COMMON_TYPEUTILS_ANYSERIALIZER_H
#define FLINK_API_COMMON_TYPEUTILS_ANYSERIALIZER_H

#include "flink/core/memory/DataInputView.h"
#include "flink/core/memory/DataOutputView.h"
#include "flink/types/Any.h"

namespace flink {
namespace api {
namespace common {
namespace typeutils {

class AnySerializer {
 public:
  virtual ~AnySerializer() {}
  virtual void serialize(const flink::types::Any& record,
                         flink::core::memory::DataOutputView& target) const = 0;
  virtual flink::types::Any deserialize(
      flink::core::memory::DataInputView& source) const = 0;
};

}  // namespace typeutils
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_TYPEUTILS_ANYSERIALIZER_H
