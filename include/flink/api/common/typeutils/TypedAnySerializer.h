/*
 * 【与 Flink 1.0 的偏差】把编译期已知的 TypeSerializer<T> 适配成 AnySerializer。
 * 见 AnySerializer.h 的说明。create() 对应 1.0 里
 * `TypeInformation.createSerializer(executionConfig)` 的调用点。
 */
#ifndef FLINK_API_COMMON_TYPEUTILS_TYPEDANYSERIALIZER_H
#define FLINK_API_COMMON_TYPEUTILS_TYPEDANYSERIALIZER_H

#include <memory>

#include "flink/api/common/typeutils/AnySerializer.h"
#include "flink/api/common/typeutils/TypeSerializer.h"
#include "flink/api/java/typeutils/TypeExtractor.h"

namespace flink {
namespace api {
namespace common {
namespace typeutils {

template <class T>
class TypedAnySerializer : public AnySerializer {
 public:
  explicit TypedAnySerializer(std::shared_ptr<TypeSerializer<T> > serializer)
      : serializer_(serializer) {}

  void serialize(const flink::types::Any& record,
                 flink::core::memory::DataOutputView& target) const override {
    serializer_->serialize(record.as<T>(), target);
  }

  flink::types::Any deserialize(
      flink::core::memory::DataInputView& source) const override {
    return flink::types::Any::of(serializer_->deserialize(source));
  }

  // 拿不到 TypeSerializer<T> 时返回空指针（见 TypeExtractor 的说明）
  static std::shared_ptr<AnySerializer> create() {
    std::shared_ptr<TypeSerializer<T> > typed =
        flink::api::java::typeutils::TypeExtractor<T>::createSerializer();
    if (!typed) return std::shared_ptr<AnySerializer>();
    return std::make_shared<TypedAnySerializer<T> >(typed);
  }

 private:
  std::shared_ptr<TypeSerializer<T> > serializer_;
};

}  // namespace typeutils
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_TYPEUTILS_TYPEDANYSERIALIZER_H
