/*
 * flink-streaming-java : org.apache.flink.streaming.api.checkpoint.Checkpointed
 *
 * 1.0 interface Checkpointed<T extends Serializable>：
 *   T snapshotState(long checkpointId, long checkpointTimestamp) throws Exception;
 *   void restoreState(T state) throws Exception;
 *
 * 由 UDF（典型是 SourceFunction，用来记 offset）实现；
 * AbstractUdfStreamOperator 在 snapshotOperatorState 里检查
 * `userFunction instanceof Checkpointed`，有的话把返回值交给
 * stateBackend.checkpointStateSerializable 存成 StateHandle，
 * 挂到 StreamTaskState.functionState 上。
 *
 * 【偏差】1.0 的 T 是任意 Serializable，靠 Java 序列化落盘。C++ 没有通用序列化，
 * 所以这里直接约定「UDF 自己序列化成字节」：snapshotState 返回 std::string，
 * restoreState 收 std::string —— 与 StateHandle 的载荷类型一致。
 */
#ifndef FLINK_STREAMING_API_CHECKPOINT_CHECKPOINTED_H
#define FLINK_STREAMING_API_CHECKPOINT_CHECKPOINTED_H

#include <cstdint>
#include <string>

namespace flink {
namespace streaming {
namespace api {
namespace checkpoint {

class Checkpointed {
 public:
  virtual ~Checkpointed() {}

  virtual std::string snapshotState(int64_t checkpointId,
                                    int64_t checkpointTimestamp) = 0;
  virtual void restoreState(const std::string& state) = 0;
};

}  // namespace checkpoint
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_CHECKPOINT_CHECKPOINTED_H
