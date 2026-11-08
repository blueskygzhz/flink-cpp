/*
 * flink-core : org.apache.flink.core.memory.DataOutputView
 *
 * 1.0 DataOutputView extends java.io.DataOutput：这里只保留状态序列化真正用到的
 * 写方法（Java 的 DataOutput 有 14 个，其余是 writeChar/writeFloat 等同构重复）。
 * 字节序固定小端（Java DataOutput 是大端，但本实现只在同机读写，统一即可）。
 */
#ifndef FLINK_CORE_MEMORY_DATAOUTPUTVIEW_H
#define FLINK_CORE_MEMORY_DATAOUTPUTVIEW_H

#include <cstdint>
#include <string>

namespace flink {
namespace core {
namespace memory {

class DataOutputView {
 public:
  virtual ~DataOutputView() {}

  virtual void write(const char* bytes, size_t length) = 0;

  virtual void writeBoolean(bool v) = 0;
  virtual void writeInt(int32_t v) = 0;
  virtual void writeLong(int64_t v) = 0;
  virtual void writeDouble(double v) = 0;
  virtual void writeUTF(const std::string& v) = 0;
};

}  // namespace memory
}  // namespace core
}  // namespace flink
#endif  // FLINK_CORE_MEMORY_DATAOUTPUTVIEW_H
