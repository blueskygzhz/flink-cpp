/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.streamrecord.StreamRecord
 *
 * 1.0 StreamRecord.java:28-199
 *   StreamRecord(T value) / StreamRecord(T value, long timestamp)
 *   getValue() / getTimestamp() / hasTimestamp() / setTimestamp(long)
 *   eraseTimestamp() / replace(X) / replace(X, long) / copy(T)
 *
 * 1.0 getTimestamp():73-86 的关键细节：hasTimestamp 为 false 时返回 Long.MIN_VALUE，
 * 窗口 assigner 正是靠这个值识别「无时间戳」并抛错（见 TumblingEventTimeWindows）。
 *
 * 【偏差】1.0 是 StreamRecord<T>，值的类型由泛型带着走；这里值用 Any 承载，
 * 具体类型由算子模板参数在编译期保证（见 flink/types/Any.h）。
 */
#ifndef FLINK_STREAMING_RUNTIME_STREAMRECORD_STREAMRECORD_H
#define FLINK_STREAMING_RUNTIME_STREAMRECORD_STREAMRECORD_H

#include <limits>
#include <sstream>

#include "flink/streaming/runtime/streamrecord/StreamElement.h"
#include "flink/types/Any.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace streamrecord {

class StreamRecord : public StreamElement {
 public:
  // 1.0 的「无时间戳」哨兵值 Long.MIN_VALUE
  static int64_t noTimestamp() { return std::numeric_limits<int64_t>::min(); }

  StreamRecord() : value_(), timestamp_(0), hasTimestamp_(false) {}
  explicit StreamRecord(const flink::types::Any& value)
      : value_(value), timestamp_(0), hasTimestamp_(false) {}
  StreamRecord(const flink::types::Any& value, int64_t timestamp)
      : value_(value), timestamp_(timestamp), hasTimestamp_(true) {}

  bool isRecord() const override { return true; }
  bool isWatermark() const override { return false; }

  const flink::types::Any& getValue() const { return value_; }
  template <class T>
  const T& getValueAs() const {
    return value_.as<T>();
  }

  // 1.0:73-86
  int64_t getTimestamp() const {
    return hasTimestamp_ ? timestamp_ : noTimestamp();
  }
  bool hasTimestamp() const { return hasTimestamp_; }

  void setTimestamp(int64_t timestamp) {
    timestamp_ = timestamp;
    hasTimestamp_ = true;
  }
  void eraseTimestamp() { hasTimestamp_ = false; }

  // 1.0:105-126 replace 保留时间戳 / 同时替换时间戳
  StreamRecord replace(const flink::types::Any& value) const {
    StreamRecord result(value);
    result.timestamp_ = timestamp_;
    result.hasTimestamp_ = hasTimestamp_;
    return result;
  }
  StreamRecord replace(const flink::types::Any& value,
                       int64_t timestamp) const {
    return StreamRecord(value, timestamp);
  }

  std::string toString() const {
    std::ostringstream os;
    os << "Record @ " << (hasTimestamp_ ? std::to_string(timestamp_)
                                        : std::string("(undef)"));
    return os.str();
  }

 private:
  flink::types::Any value_;
  int64_t timestamp_;
  bool hasTimestamp_;
};

}  // namespace streamrecord
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_STREAMRECORD_STREAMRECORD_H
