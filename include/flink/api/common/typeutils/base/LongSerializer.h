/*
 * flink-core : org.apache.flink.api.common.typeutils.base.LongSerializer
 * 1.0 LongSerializer.java: isImmutableType()=true, getLength()=8
 */
#ifndef FLINK_API_COMMON_TYPEUTILS_BASE_LONGSERIALIZER_H
#define FLINK_API_COMMON_TYPEUTILS_BASE_LONGSERIALIZER_H

#include "flink/api/common/typeutils/TypeSerializer.h"

namespace flink {
namespace api {
namespace common {
namespace typeutils {
namespace base {

class LongSerializer : public TypeSerializer<int64_t> {
 public:
  bool isImmutableType() const override { return true; }
  int64_t createInstance() const override { return 0; }
  int getLength() const override { return 8; }

  void serialize(const int64_t& record,
                 flink::core::memory::DataOutputView& target) const override {
    target.writeLong(record);
  }
  int64_t deserialize(
      flink::core::memory::DataInputView& source) const override {
    return source.readLong();
  }
};

}  // namespace base
}  // namespace typeutils
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_TYPEUTILS_BASE_LONGSERIALIZER_H
