/*
 * flink-core : org.apache.flink.api.common.typeutils.base.BooleanSerializer
 * 1.0 BooleanSerializer.java: isImmutableType()=true, getLength()=1
 */
#ifndef FLINK_API_COMMON_TYPEUTILS_BASE_BOOLEANSERIALIZER_H
#define FLINK_API_COMMON_TYPEUTILS_BASE_BOOLEANSERIALIZER_H

#include "flink/api/common/typeutils/TypeSerializer.h"

namespace flink {
namespace api {
namespace common {
namespace typeutils {
namespace base {

class BooleanSerializer : public TypeSerializer<bool> {
 public:
  bool isImmutableType() const override { return true; }
  bool createInstance() const override { return false; }
  int getLength() const override { return 1; }

  void serialize(const bool& record,
                 flink::core::memory::DataOutputView& target) const override {
    target.writeBoolean(record);
  }
  bool deserialize(flink::core::memory::DataInputView& source) const override {
    return source.readBoolean();
  }
};

}  // namespace base
}  // namespace typeutils
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_TYPEUTILS_BASE_BOOLEANSERIALIZER_H
