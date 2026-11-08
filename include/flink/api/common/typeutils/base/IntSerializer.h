/*
 * flink-core : org.apache.flink.api.common.typeutils.base.IntSerializer
 * 1.0 IntSerializer.java: isImmutableType()=true, getLength()=4
 */
#ifndef FLINK_API_COMMON_TYPEUTILS_BASE_INTSERIALIZER_H
#define FLINK_API_COMMON_TYPEUTILS_BASE_INTSERIALIZER_H

#include "flink/api/common/typeutils/TypeSerializer.h"

namespace flink {
namespace api {
namespace common {
namespace typeutils {
namespace base {

class IntSerializer : public TypeSerializer<int32_t> {
 public:
  bool isImmutableType() const override { return true; }
  int32_t createInstance() const override { return 0; }
  int getLength() const override { return 4; }

  void serialize(const int32_t& record,
                 flink::core::memory::DataOutputView& target) const override {
    target.writeInt(record);
  }
  int32_t deserialize(
      flink::core::memory::DataInputView& source) const override {
    return source.readInt();
  }
};

}  // namespace base
}  // namespace typeutils
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_TYPEUTILS_BASE_INTSERIALIZER_H
