/*
 * flink-runtime : org.apache.flink.runtime.util.event.EventListener
 *
 * 1.0 EventListener.java 全文：
 *   public interface EventListener<T> { void onEvent(T event); }
 *
 * 用处：CheckpointBarrierHandler 攒齐屏障后，通过它回调
 * StreamTask.triggerCheckpoint（见 StreamTask.getCheckpointBarrierListener()，
 * StreamTask.java:673）。
 */
#ifndef FLINK_RUNTIME_UTIL_EVENT_EVENTLISTENER_H
#define FLINK_RUNTIME_UTIL_EVENT_EVENTLISTENER_H

namespace flink {
namespace runtime {
namespace util {
namespace event {

template <class T>
class EventListener {
 public:
  virtual ~EventListener() {}
  virtual void onEvent(const T& event) = 0;
};

}  // namespace event
}  // namespace util
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_UTIL_EVENT_EVENTLISTENER_H
