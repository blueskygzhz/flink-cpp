/*
 * flink-runtime : org.apache.flink.runtime.util.DataInputDeserializer
 *
 * 1.0 DataInputDeserializer implements DataInputView：包一段 byte[]，
 * 按位置游标读出各类型；读越界抛 EOFException（这里抛 std::runtime_error）。
 */
#ifndef FLINK_RUNTIME_UTIL_DATAINPUTDESERIALIZER_H
#define FLINK_RUNTIME_UTIL_DATAINPUTDESERIALIZER_H

#include "flink/core/memory/DataInputView.h"

namespace flink {
namespace runtime {
namespace util {

class DataInputDeserializer : public flink::core::memory::DataInputView {
 public:
  explicit DataInputDeserializer(const std::string& bytes)
      : buffer_(bytes), position_(0) {}

  void readFully(char* target, size_t length) override;
  void skipBytesToRead(size_t numBytes) override;

  bool readBoolean() override;
  int32_t readInt() override;
  int64_t readLong() override;
  double readDouble() override;
  std::string readUTF() override;

  size_t getPosition() const { return position_; }
  size_t available() const { return buffer_.size() - position_; }

 private:
  std::string buffer_;
  size_t position_;
};

}  // namespace util
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_UTIL_DATAINPUTDESERIALIZER_H
