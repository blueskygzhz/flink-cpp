/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.io.CheckpointBarrierHandler
 *
 * 1.0 interface CheckpointBarrierHandler（CheckpointBarrierHandler.java:33）：
 *   BufferOrEvent getNextNonBlocked() throws IOException, InterruptedException;
 *   void registerCheckpointEventHandler(EventListener<CheckpointBarrier>);
 *   void cleanup() throws IOException;
 *   boolean isEmpty();
 *
 * 1.0 原注释：「The CheckpointBarrierHandler reacts to checkpoint barrier
 * arriving from the input channels. Different implementations may either
 * simply track barriers, or block certain inputs on barriers.」
 * 两个实现：
 *   BarrierBuffer  —— EXACTLY_ONCE，收到屏障就阻塞该通道并把后续数据缓存起来
 *   BarrierTracker —— AT_LEAST_ONCE，只数屏障，不阻塞任何通道
 *
 * 【偏差】getNextNonBlocked() 返回 null 表示流结束；这里改成
 * bool getNextNonBlocked(BufferOrEvent& target)，与 InputGate 的做法一致。
 */
#ifndef FLINK_STREAMING_RUNTIME_IO_CHECKPOINTBARRIERHANDLER_H
#define FLINK_STREAMING_RUNTIME_IO_CHECKPOINTBARRIERHANDLER_H

#include "flink/runtime/io/network/api/CheckpointBarrier.h"
#include "flink/runtime/io/network/partition/consumer/BufferOrEvent.h"
#include "flink/runtime/util/event/EventListener.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

class CheckpointBarrierHandler {
 public:
  typedef flink::runtime::io::network::partition::consumer::BufferOrEvent
      BufferOrEvent;
  typedef flink::runtime::io::network::api::CheckpointBarrier CheckpointBarrier;
  typedef flink::runtime::util::event::EventListener<CheckpointBarrier>
      CheckpointEventListener;

  virtual ~CheckpointBarrierHandler() {}

  virtual bool getNextNonBlocked(BufferOrEvent& target) = 0;
  virtual void registerCheckpointEventHandler(
      CheckpointEventListener* checkpointHandler) = 0;
  virtual void cleanup() = 0;
  virtual bool isEmpty() const = 0;
};

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_IO_CHECKPOINTBARRIERHANDLER_H
