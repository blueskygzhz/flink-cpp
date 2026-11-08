/*
 * flink-streaming-java : org.apache.flink.streaming.api.operators.TimestampedCollector
 *
 * 1.0 TimestampedCollector.java:
 *   collect(T record)                   -> output.collect(reuse.replace(record))
 *   setTimestamp(StreamRecord<?> base)  -> 继承输入记录的时间戳（StreamFlatMap 用）
 *   setAbsoluteTimestamp(long)          -> 直接设定（WindowOperator.java:249 用
 *                                          window.maxTimestamp()）
 *   eraseTimestamp()
 */
#ifndef FLINK_STREAMING_API_OPERATORS_TIMESTAMPEDCOLLECTOR_H
#define FLINK_STREAMING_API_OPERATORS_TIMESTAMPEDCOLLECTOR_H

#include "flink/streaming/api/operators/Output.h"
#include "flink/types/Any.h"
#include "flink/util/Collector.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

template <class T>
class TimestampedCollector : public flink::util::Collector<T> {
 public:
  explicit TimestampedCollector(Output* output)
      : output_(output), timestamp_(0), hasTimestamp_(false) {}

  void collect(const T& record) override {
    typedef flink::streaming::runtime::streamrecord::StreamRecord StreamRecord;
    if (hasTimestamp_) {
      output_->collect(StreamRecord(flink::types::Any::of(record), timestamp_));
    } else {
      output_->collect(StreamRecord(flink::types::Any::of(record)));
    }
  }

  void setTimestamp(
      const flink::streaming::runtime::streamrecord::StreamRecord& base) {
    if (base.hasTimestamp()) {
      timestamp_ = base.getTimestamp();
      hasTimestamp_ = true;
    } else {
      hasTimestamp_ = false;
    }
  }

  void setAbsoluteTimestamp(int64_t timestamp) {
    timestamp_ = timestamp;
    hasTimestamp_ = true;
  }

  void eraseTimestamp() { hasTimestamp_ = false; }

  void close() override {}

 private:
  Output* output_;
  int64_t timestamp_;
  bool hasTimestamp_;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_TIMESTAMPEDCOLLECTOR_H
