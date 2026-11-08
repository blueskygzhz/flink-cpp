#include "flink/runtime/util/MathUtils.h"

#include <limits>

namespace flink {
namespace runtime {
namespace util {

namespace {
inline uint32_t rotateLeft(uint32_t value, int distance) {
  return (value << distance) | (value >> (32 - distance));
}
}  // namespace

// 1.0 MathUtils.java:144-168 逐行复刻
int32_t MathUtils::murmurHash(int32_t code) {
  uint32_t c = static_cast<uint32_t>(code);
  c *= 0xcc9e2d51u;
  c = rotateLeft(c, 15);
  c *= 0x1b873593u;

  c = rotateLeft(c, 13);
  c *= 0xe6546b64u;

  c ^= 4u;
  c ^= c >> 16;
  c *= 0x85ebca6bu;
  c ^= c >> 13;
  c *= 0xc2b2ae35u;
  c ^= c >> 16;

  const int32_t result = static_cast<int32_t>(c);
  if (result >= 0) return result;
  if (result != std::numeric_limits<int32_t>::min()) return -result;
  return 0;
}

}  // namespace util
}  // namespace runtime
}  // namespace flink
