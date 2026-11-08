/*
 * flink-streaming-java : org.apache.flink.streaming.api.CheckpointingMode
 *
 * 1.0 CheckpointingMode.java:
 *   EXACTLY_ONCE    屏障对齐（BarrierBuffer），对齐期间后到的通道被阻塞缓存
 *   AT_LEAST_ONCE   不对齐（BarrierTracker），屏障一到就直接快照
 * 默认值是 EXACTLY_ONCE（StreamExecutionEnvironment.enableCheckpointing）。
 */
#ifndef FLINK_STREAMING_API_CHECKPOINTINGMODE_H
#define FLINK_STREAMING_API_CHECKPOINTINGMODE_H

namespace flink {
namespace streaming {
namespace api {

enum class CheckpointingMode { EXACTLY_ONCE, AT_LEAST_ONCE };

}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_CHECKPOINTINGMODE_H
