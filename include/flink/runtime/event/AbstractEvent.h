/*
 * flink-runtime : org.apache.flink.runtime.event.AbstractEvent
 *
 * 1.0 AbstractEvent implements IOReadableWritable：所有在数据通道上与数据缓冲
 * 混合传输的「运行时事件」的基类（CheckpointBarrier、EndOfPartitionEvent 等）。
 * read/write(DataInputView/DataOutputView) 序列化方法在本实现里不需要
 * （进程内传输，不走网络），故不保留。
 */
#ifndef FLINK_RUNTIME_EVENT_ABSTRACTEVENT_H
#define FLINK_RUNTIME_EVENT_ABSTRACTEVENT_H

namespace flink {
namespace runtime {
namespace event {

class AbstractEvent {
 public:
  virtual ~AbstractEvent() {}
};

}  // namespace event
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_EVENT_ABSTRACTEVENT_H
