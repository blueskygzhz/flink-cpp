/*
 * flink-streaming-java : org.apache.flink.streaming.api.operators.ChainingStrategy
 *
 * 1.0 ChainingStrategy.java:
 *   ALWAYS      尽可能与上游算子链接（map/filter 等默认值）
 *   NEVER       从不链接（算子独占一个 task）
 *   HEAD        只能作为链头（source 的默认值）
 * 1.0 AbstractStreamOperator.java:68 的默认值是 HEAD。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_CHAININGSTRATEGY_H
#define FLINK_STREAMING_API_OPERATORS_CHAININGSTRATEGY_H

namespace flink {
namespace streaming {
namespace api {
namespace operators {

enum class ChainingStrategy { ALWAYS, NEVER, HEAD };

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_CHAININGSTRATEGY_H
