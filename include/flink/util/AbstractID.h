/*
 * flink-core : org.apache.flink.util.AbstractID
 *
 * 1.0 class AbstractID implements IOReadableWritable, Comparable<AbstractID>,
 *                                 Serializable（AbstractID.java:33）
 *
 * 1.0 类注释：「A statistically unique identification number.」
 *
 * 常量与字段（:38-50）：
 *   private static final Random RND = new Random();
 *   private static final int SIZE_OF_LONG = 8;
 *   public  static final int SIZE = 2 * SIZE_OF_LONG;   // 16 字节
 *   protected long upperPart;
 *   protected long lowerPart;
 *   private String toString;        // 懒缓存
 *
 * 四个构造（:55-90）：
 *   AbstractID(byte[] bytes)  —— 长度不是 16 就
 *       IllegalArgumentException("Argument bytes must by an array of 16 bytes")
 *       注意装配顺序：lowerPart 取 bytes[0..8)，upperPart 取 bytes[8..16)
 *   AbstractID(long lowerPart, long upperPart)   —— 参数顺序是 lower 在前
 *   AbstractID(AbstractID id)                    —— null 则 IllegalArgument
 *   AbstractID()                                 —— 两个 RND.nextLong()
 *
 * getBytes()：lowerPart 写前 8 字节、upperPart 写后 8 字节（与构造对称）
 * equals / hashCode / compareTo / toString（十六进制）
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 字节序】1.0 的 byteArrayToLong / longToByteArray 是
 * **大端**（`l |= (b[i] & 0xffL) << ((7 - i) << 3)`）。这里照搬大端，
 * 从而 toString() 的十六进制与 1.0 逐字符一致。
 *
 * 【偏差 2 —— 不实现 IOReadableWritable】read(DataInputView) /
 * write(DataOutputView) 属于序列化层，本仓库没有（见各处同名偏差）。
 *
 * 【偏差 3 —— 随机数】1.0 用共享的 java.util.Random；这里用
 * std::mt19937_64 + random_device 播种。两边都只保证「统计唯一」，没有
 * 可复现性要求。
 */
#ifndef FLINK_UTIL_ABSTRACTID_H
#define FLINK_UTIL_ABSTRACTID_H

#include <cstdint>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace flink {
namespace util {

class AbstractID {
 public:
  // 1.0 SIZE = 2 * SIZE_OF_LONG
  static const int SIZE_OF_LONG = 8;
  static const int SIZE = 2 * SIZE_OF_LONG;

  // 1.0 AbstractID(byte[] bytes)（:55）
  explicit AbstractID(const std::vector<unsigned char>& bytes) {
    if (bytes.size() != static_cast<size_t>(SIZE)) {
      throw std::invalid_argument(
          "Argument bytes must by an array of 16 bytes");
    }
    lowerPart_ = byteArrayToLong(bytes, 0);
    upperPart_ = byteArrayToLong(bytes, SIZE_OF_LONG);
  }

  // 1.0 AbstractID(long lowerPart, long upperPart)（:68）—— lower 在前
  AbstractID(int64_t lowerPart, int64_t upperPart)
      : upperPart_(upperPart), lowerPart_(lowerPart) {}

  // 1.0 AbstractID()（:86）
  AbstractID() {
    static std::mt19937_64 rnd(std::random_device{}());
    lowerPart_ = static_cast<int64_t>(rnd());
    upperPart_ = static_cast<int64_t>(rnd());
  }

  virtual ~AbstractID() {}

  int64_t getLowerPart() const { return lowerPart_; }
  int64_t getUpperPart() const { return upperPart_; }

  // 1.0 getBytes()（:103）
  std::vector<unsigned char> getBytes() const {
    std::vector<unsigned char> bytes(SIZE);
    longToByteArray(lowerPart_, bytes, 0);
    longToByteArray(upperPart_, bytes, SIZE_OF_LONG);
    return bytes;
  }

  bool operator==(const AbstractID& other) const {
    return lowerPart_ == other.lowerPart_ && upperPart_ == other.upperPart_;
  }
  bool operator!=(const AbstractID& other) const { return !(*this == other); }

  // 1.0 compareTo：先比 upperPart 再比 lowerPart（无符号语义）
  bool operator<(const AbstractID& other) const {
    const uint64_t upperA = static_cast<uint64_t>(upperPart_);
    const uint64_t upperB = static_cast<uint64_t>(other.upperPart_);
    if (upperA != upperB) return upperA < upperB;
    return static_cast<uint64_t>(lowerPart_) <
           static_cast<uint64_t>(other.lowerPart_);
  }

  // 1.0 toString()：StringUtils.byteToHexString(getBytes())
  std::string toString() const {
    const std::vector<unsigned char> bytes = getBytes();
    std::string hex;
    hex.reserve(bytes.size() * 2);
    char buffer[3];
    for (unsigned char byte : bytes) {
      std::snprintf(buffer, sizeof(buffer), "%02x", byte);
      hex += buffer;
    }
    return hex;
  }

 protected:
  int64_t upperPart_;
  int64_t lowerPart_;

 private:
  // 1.0 byteArrayToLong：大端（偏差 1）
  static int64_t byteArrayToLong(const std::vector<unsigned char>& bytes,
                                 int offset) {
    int64_t value = 0;
    for (int i = 0; i < SIZE_OF_LONG; ++i) {
      value |= static_cast<int64_t>(bytes[offset + i] & 0xffU)
               << ((SIZE_OF_LONG - 1 - i) << 3);
    }
    return value;
  }

  // 1.0 longToByteArray
  static void longToByteArray(int64_t value,
                              std::vector<unsigned char>& bytes, int offset) {
    for (int i = 0; i < SIZE_OF_LONG; ++i) {
      const int bitShift = (SIZE_OF_LONG - 1 - i) << 3;
      bytes[offset + i] =
          static_cast<unsigned char>((value >> bitShift) & 0xffLL);
    }
  }
};

}  // namespace util
}  // namespace flink
#endif  // FLINK_UTIL_ABSTRACTID_H
