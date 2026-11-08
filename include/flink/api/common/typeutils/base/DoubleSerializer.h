/*
 * flink-core : org.apache.flink.api.common.typeutils.base.DoubleSerializer
 * 1.0 DoubleSerializer.java: isImmutableType()=true, getLength()=8
 */
#ifndef FLINK_API_COMMON_TYPEUTILS_BASE_DOUBLESERIALIZER_H
#define FLINK_API_COMMON_TYPEUTILS_BASE_DOUBLESERIALIZER_H

#include "flink/api/common/typeutils/TypeSerializer.h"

namespace flink {
namespace api {
namespace common {
namespace typeutils {
namespace base {

class DoubleSerializer : public TypeSerializer<double> {
 public:
  bool isImmutableType() const override { return true; }
  double createInstance() const override { return 0.0; }
  int getLength() const override { return 8; }

  void serialize(const double& record,
                 flink::core::memory::DataOutputView& target) const override {
    target.writeDouble(record);
  }
  double deserialize(
      flink::core::memory::DataInputView& source) const override {
    return source.readDouble();
  }
};

}  // namespace base
}  // namespace typeutils
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_TYPEUTILS_BASE_DOUBLESERIALIZER_H
