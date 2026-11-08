/*
 * flink-core : org.apache.flink.core.memory.DataInputView
 *
 * 1.0 DataInputView extends java.io.DataInput + skipBytesToRead(int)。
 * 这里保留状态反序列化真正用到的读方法。
 */
#ifndef FLINK_CORE_MEMORY_DATAINPUTVIEW_H
#define FLINK_CORE_MEMORY_DATAINPUTVIEW_H

#include <cstdint>
#include <string>

namespace flink {
namespace core {
namespace memory {

class DataInputView {
 public:
  virtual ~DataInputView() {}

  virtual void readFully(char* target, size_t length) = 0;
  virtual void skipBytesToRead(size_t numBytes) = 0;

  virtual bool readBoolean() = 0;
  virtual int32_t readInt() = 0;
  virtual int64_t readLong() = 0;
  virtual double readDouble() = 0;
  virtual std::string readUTF() = 0;
};

}  // namespace memory
}  // namespace core
}  // namespace flink
#endif  // FLINK_CORE_MEMORY_DATAINPUTVIEW_H
