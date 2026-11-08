/*
 * flink-streaming-java : org.apache.flink.streaming.api.watermark.Watermark
 *
 * 1.0 Watermark.java:
 *   :47  public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE)
 *   Watermark(long timestamp) / getTimestamp()
 * 语义：收到 Watermark(t) 表示「不会再有 timestamp <= t 的记录」。
 */
#ifndef FLINK_STREAMING_API_WATERMARK_WATERMARK_H
#define FLINK_STREAMING_API_WATERMARK_WATERMARK_H

#include <limits>
#include <string>

#include "flink/streaming/runtime/streamrecord/StreamElement.h"

namespace flink {
namespace streaming {
namespace api {
namespace watermark {

class Watermark : public flink::streaming::runtime::streamrecord::StreamElement {
 public:
  // 1.0 Watermark.MAX_WATERMARK（C++ 不能在头里定义非 inline 静态对象，用函数返回）
  static Watermark maxWatermark() {
    return Watermark(std::numeric_limits<int64_t>::max());
  }
  static int64_t maxTimestamp() { return std::numeric_limits<int64_t>::max(); }

  explicit Watermark(int64_t timestamp) : timestamp_(timestamp) {}

  bool isRecord() const override { return false; }
  bool isWatermark() const override { return true; }

  int64_t getTimestamp() const { return timestamp_; }

  bool operator==(const Watermark& other) const {
    return timestamp_ == other.timestamp_;
  }

  std::string toString() const {
    return "Watermark @ " + std::to_string(timestamp_);
  }

 private:
  int64_t timestamp_;
};

}  // namespace watermark
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_WATERMARK_WATERMARK_H
