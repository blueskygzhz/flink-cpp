/*
 * flink-runtime : org.apache.flink.runtime.state.CheckpointListener
 *
 * 1.0 CheckpointListener.java:
 *   void notifyCheckpointComplete(long checkpointId) throws Exception;
 *
 * AbstractStateBackend.notifyOfCompletedCheckpoint 会遍历所有 KvState，对
 * `instanceof CheckpointListener` 的那些回调（AbstractStateBackend.java:274）。
 * 堆内存状态不需要这个通知，所以 HeapValueState / HeapListState 都不实现它
 * —— 这与 1.0 的 MemValueState / MemListState 一致。
 */
#ifndef FLINK_RUNTIME_STATE_CHECKPOINTLISTENER_H
#define FLINK_RUNTIME_STATE_CHECKPOINTLISTENER_H

#include <cstdint>

namespace flink {
namespace runtime {
namespace state {

class CheckpointListener {
 public:
  virtual ~CheckpointListener() {}
  virtual void notifyCheckpointComplete(int64_t checkpointId) = 0;
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_CHECKPOINTLISTENER_H
