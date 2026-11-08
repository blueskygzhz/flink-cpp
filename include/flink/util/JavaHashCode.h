/*
 * 【与 Flink 1.0 的偏差】
 * 1.0 HashPartitioner.selectChannels 调的是 Java 的 key.hashCode()，
 * 分区结果取决于 JDK 各类型 hashCode 的具体实现。要让 C++ 版本的分区结果
 * 与 Java 版一致，就必须复刻 JDK 的 hashCode，而不能用实现相关的 std::hash。
 *
 * 对应 Java: java.lang.{String,Integer,Long,Double,Boolean}.hashCode()
 */
#ifndef FLINK_UTIL_JAVAHASHCODE_H
#define FLINK_UTIL_JAVAHASHCODE_H

#include <cstdint>
#include <functional>
#include <string>

namespace flink {
namespace util {

// 未特化的类型退化为 std::hash：分区仍然正确（同 key 必到同通道），
// 只是具体通道号与 Java 版可能不同。
template <class K>
struct JavaHashCode {
  static int32_t apply(const K& key) {
    return static_cast<int32_t>(std::hash<K>()(key));
  }
};

// java.lang.String.hashCode():  h = 31 * h + c
template <>
struct JavaHashCode<std::string> {
  static int32_t apply(const std::string& s) {
    uint32_t h = 0u;
    for (size_t i = 0; i < s.size(); ++i) {
      h = h * 31u + static_cast<uint32_t>(static_cast<unsigned char>(s[i]));
    }
    return static_cast<int32_t>(h);
  }
};

// java.lang.Integer.hashCode(): 值本身
template <>
struct JavaHashCode<int32_t> {
  static int32_t apply(const int32_t& v) { return v; }
};

// java.lang.Long.hashCode(): (int)(value ^ (value >>> 32))
template <>
struct JavaHashCode<int64_t> {
  static int32_t apply(const int64_t& v) {
    const uint64_t u = static_cast<uint64_t>(v);
    return static_cast<int32_t>(static_cast<uint32_t>(u ^ (u >> 32)));
  }
};

// java.lang.Boolean.hashCode(): true -> 1231, false -> 1237
template <>
struct JavaHashCode<bool> {
  static int32_t apply(const bool& v) { return v ? 1231 : 1237; }
};

}  // namespace util
}  // namespace flink
#endif  // FLINK_UTIL_JAVAHASHCODE_H
