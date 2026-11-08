/*
 * flink-streaming-java : org.apache.flink.streaming.api.TimeCharacteristic
 *
 * 1.0 TimeCharacteristic.java: ProcessingTime / IngestionTime / EventTime
 * 1.0 StreamExecutionEnvironment 的默认值是 ProcessingTime。
 */
#ifndef FLINK_STREAMING_API_TIMECHARACTERISTIC_H
#define FLINK_STREAMING_API_TIMECHARACTERISTIC_H

namespace flink {
namespace streaming {
namespace api {

enum class TimeCharacteristic {
  ProcessingTime,
  IngestionTime,
  EventTime
};

}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TIMECHARACTERISTIC_H
