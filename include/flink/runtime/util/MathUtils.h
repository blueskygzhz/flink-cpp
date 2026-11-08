/*
 * flink-runtime : org.apache.flink.runtime.util.MathUtils
 *
 * 1.0 MathUtils.java:144-168  murmurHash(int code)
 * HashPartitioner 用它把 key.hashCode() 打散到输出通道。
 * Java 的 int 是 32 位有符号、>>> 是无符号右移，这里用 uint32_t 精确复刻。
 */
#ifndef FLINK_RUNTIME_UTIL_MATHUTILS_H
#define FLINK_RUNTIME_UTIL_MATHUTILS_H

#include <cstdint>

namespace flink {
namespace runtime {
namespace util {

class MathUtils {
 public:
  static int32_t murmurHash(int32_t code);

 private:
  MathUtils() {}
};

}  // namespace util
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_UTIL_MATHUTILS_H
