/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.operators.Triggerable
 *
 * 1.0 Triggerable.java:  void trigger(long timestamp) throws Exception
 * 由 StreamTask.registerTimer(time, target) 注册，到点后在**持有
 * checkpointLock 的前提下**回调（见 StreamTask.TriggerTask.run）。
 * WindowOperator 用它驱动 processing-time 窗口触发。
 */
#ifndef FLINK_STREAMING_RUNTIME_OPERATORS_TRIGGERABLE_H
#define FLINK_STREAMING_RUNTIME_OPERATORS_TRIGGERABLE_H

#include <cstdint>

namespace flink {
namespace streaming {
namespace runtime {
namespace operators {

class Triggerable {
 public:
  virtual ~Triggerable() {}
  virtual void trigger(int64_t timestamp) = 0;
};

}  // namespace operators
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_OPERATORS_TRIGGERABLE_H
