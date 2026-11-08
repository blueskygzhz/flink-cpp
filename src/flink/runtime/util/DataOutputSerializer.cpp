#include "flink/runtime/util/DataOutputSerializer.h"

namespace flink {
namespace runtime {
namespace util {

void DataOutputSerializer::write(const char* bytes, size_t length) {
  buffer_.append(bytes, length);
}

void DataOutputSerializer::writeBoolean(bool v) {
  const char b = v ? 1 : 0;
  buffer_.append(&b, 1);
}

void DataOutputSerializer::writeInt(int32_t v) {
  buffer_.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

void DataOutputSerializer::writeLong(int64_t v) {
  buffer_.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

void DataOutputSerializer::writeDouble(double v) {
  buffer_.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

// 1.0 DataOutput.writeUTF 是 2 字节长度 + modified UTF-8；这里用 4 字节长度 +
// 原始字节，以支持长度 > 65535 的字符串，且不做字符集转换。
void DataOutputSerializer::writeUTF(const std::string& v) {
  writeInt(static_cast<int32_t>(v.size()));
  if (!v.empty()) buffer_.append(v.data(), v.size());
}

}  // namespace util
}  // namespace runtime
}  // namespace flink
