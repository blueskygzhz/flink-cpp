#include "flink/runtime/util/DataInputDeserializer.h"

#include <cstring>
#include <stdexcept>

namespace flink {
namespace runtime {
namespace util {

// 1.0 读越界抛 EOFException
void DataInputDeserializer::readFully(char* target, size_t length) {
  if (position_ + length > buffer_.size()) {
    throw std::runtime_error("DataInputDeserializer: EOF, tried to read " +
                             std::to_string(length) + " bytes, only " +
                             std::to_string(available()) + " available");
  }
  std::memcpy(target, buffer_.data() + position_, length);
  position_ += length;
}

void DataInputDeserializer::skipBytesToRead(size_t numBytes) {
  if (position_ + numBytes > buffer_.size()) {
    throw std::runtime_error("DataInputDeserializer: EOF while skipping");
  }
  position_ += numBytes;
}

bool DataInputDeserializer::readBoolean() {
  char b = 0;
  readFully(&b, 1);
  return b != 0;
}

int32_t DataInputDeserializer::readInt() {
  int32_t v = 0;
  readFully(reinterpret_cast<char*>(&v), sizeof(v));
  return v;
}

int64_t DataInputDeserializer::readLong() {
  int64_t v = 0;
  readFully(reinterpret_cast<char*>(&v), sizeof(v));
  return v;
}

double DataInputDeserializer::readDouble() {
  double v = 0;
  readFully(reinterpret_cast<char*>(&v), sizeof(v));
  return v;
}

std::string DataInputDeserializer::readUTF() {
  const int32_t length = readInt();
  if (length < 0) {
    throw std::runtime_error("DataInputDeserializer: corrupted UTF length");
  }
  std::string s;
  s.resize(static_cast<size_t>(length));
  if (length > 0) readFully(&s[0], static_cast<size_t>(length));
  return s;
}

}  // namespace util
}  // namespace runtime
}  // namespace flink
