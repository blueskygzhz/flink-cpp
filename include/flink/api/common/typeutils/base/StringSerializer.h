/*
 * flink-core : org.apache.flink.api.common.typeutils.base.StringSerializer
 * 1.0 StringSerializer.java: isImmutableType()=true, getLength()=-1（变长）
 */
#ifndef FLINK_API_COMMON_TYPEUTILS_BASE_STRINGSERIALIZER_H
#define FLINK_API_COMMON_TYPEUTILS_BASE_STRINGSERIALIZER_H

#include <string>

#include "flink/api/common/typeutils/TypeSerializer.h"

namespace flink {
namespace api {
namespace common {
namespace typeutils {
namespace base {

class StringSerializer : public TypeSerializer<std::string> {
 public:
  bool isImmutableType() const override { return true; }
  std::string createInstance() const override { return std::string(); }
  int getLength() const override { return -1; }

  void serialize(const std::string& record,
                 flink::core::memory::DataOutputView& target) const override {
    target.writeUTF(record);
  }
  std::string deserialize(
      flink::core::memory::DataInputView& source) const override {
    return source.readUTF();
  }
};

}  // namespace base
}  // namespace typeutils
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_TYPEUTILS_BASE_STRINGSERIALIZER_H
