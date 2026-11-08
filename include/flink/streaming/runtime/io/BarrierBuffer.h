/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.io.BarrierBuffer
 *
 * 1.0 BarrierBuffer implements CheckpointBarrierHandler（BarrierBuffer.java:42）
 * —— exactly-once 的核心：收到某通道的屏障后**阻塞**该通道，把它后续的数据
 * 溢写缓存起来，等所有通道的同 id 屏障都到齐，才通知 StreamTask 做快照、
 * 然后解除全部阻塞、优先消费刚缓存的数据。
 *
 * 字段（:48-80）：
 *   private final InputGate inputGate;
 *   private final boolean[] blockedChannels;
 *   private final int totalNumberOfInputChannels;
 *   private final BufferSpiller bufferSpiller;
 *   private final ArrayDeque<SpilledBufferOrEventSequence> queuedBuffered;
 *   private SpilledBufferOrEventSequence currentBuffered;
 *   private EventListener<CheckpointBarrier> checkpointHandler;
 *   private long currentCheckpointId = -1L;
 *   private int numBarriersReceived;
 *   private int numClosedChannels;
 *   private boolean endOfStream;
 *
 * 方法：
 *   BufferOrEvent getNextNonBlocked()           (:103)
 *   private void completeBufferedSequence()     (:153)
 *   private void processBarrier(CheckpointBarrier, int channelIndex)  (:161)
 *   void registerCheckpointEventHandler(...)    (:209)
 *   boolean isEmpty()                           (:219)
 *   void cleanup()                               (:224)
 *   private boolean isBlocked(int)               (:241)
 *   private void onBarrier(int)                  (:250)
 *   private void releaseBlocks()                 (:268)
 *
 * 【偏差】构造参数去掉 IOManager —— BufferSpiller 改成内存队列，不需要临时目录
 * （见 BufferSpiller.h）。
 *
 * 【偏差】1.0 的 getNextNonBlocked 在两处用递归调自己
 * （completeBufferedSequence 之后、releaseBlocks 之后）。这里改成外层 while
 * 循环里 `continue`，语义相同但不会在长流上堆栈增长 —— 1.0 的递归深度受
 * 「缓存序列个数」约束，实际也不深，但 C++ 没有尾调用保证。
 */
#ifndef FLINK_STREAMING_RUNTIME_IO_BARRIERBUFFER_H
#define FLINK_STREAMING_RUNTIME_IO_BARRIERBUFFER_H

#include <cstdint>
#include <deque>
#include <vector>

#include "flink/runtime/io/network/partition/consumer/InputGate.h"
#include "flink/streaming/runtime/io/BufferSpiller.h"
#include "flink/streaming/runtime/io/CheckpointBarrierHandler.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

class BarrierBuffer : public CheckpointBarrierHandler {
 public:
  typedef flink::runtime::io::network::partition::consumer::InputGate InputGate;

  explicit BarrierBuffer(InputGate* inputGate);

  bool getNextNonBlocked(BufferOrEvent& target) override;

  void registerCheckpointEventHandler(
      CheckpointEventListener* checkpointHandler) override;

  // 1.0 isEmpty(): return currentBuffered == null;
  bool isEmpty() const override { return !currentBuffered_; }

  void cleanup() override;

 private:
  // 1.0 private 方法，名字与签名保持一致
  void completeBufferedSequence();
  void processBarrier(const CheckpointBarrier& receivedBarrier,
                      int channelIndex);
  bool isBlocked(int channelIndex) const {
    return blockedChannels_[channelIndex];
  }
  void onBarrier(int channelIndex);
  void releaseBlocks();

  InputGate* inputGate_;
  std::vector<bool> blockedChannels_;
  int totalNumberOfInputChannels_;

  BufferSpiller bufferSpiller_;
  std::deque<BufferSpiller::SequencePtr> queuedBuffered_;
  BufferSpiller::SequencePtr currentBuffered_;

  CheckpointEventListener* checkpointHandler_;

  int64_t currentCheckpointId_;
  int numBarriersReceived_;
  int numClosedChannels_;
  bool endOfStream_;
};

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_IO_BARRIERBUFFER_H
