/*
 * flink-runtime : org.apache.flink.runtime.util.DataOutputSerializer
 *
 * 1.0 DataOutputSerializer implements DataOutputView：往内部可增长的
 * ByteBuffer 里写，wrapAsByteBuffer() 取出结果。
 * 这里内部缓冲用 std::string（当字节容器），getByteArray() 对应 wrapAsByteBuffer。
 */
#ifndef FLINK_RUNTIME_UTIL_DATAOUTPUTSERIALIZER_H
#define FLINK_RUNTIME_UTIL_DATAOUTPUTSERIALIZER_H

#include "flink/core/memory/DataOutputView.h"

namespace flink {
namespace runtime {
namespace util {

class DataOutputSerializer : public flink::core::memory::DataOutputView {
 public:
  DataOutputSerializer() {}

  void write(const char* bytes, size_t length) override;
  void writeBoolean(bool v) override;
  void writeInt(int32_t v) override;
  void writeLong(int64_t v) override;
  void writeDouble(double v) override;
  void writeUTF(const std::string& v) override;

  // 1.0 wrapAsByteBuffer() / 序列化结果长度
  const std::string& getByteArray() const { return buffer_; }
  size_t length() const { return buffer_.size(); }
  void clear() { buffer_.clear(); }

 private:
  std::string buffer_;
};

}  // namespace util
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_UTIL_DATAOUTPUTSERIALIZER_H
