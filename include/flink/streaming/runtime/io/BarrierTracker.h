/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.io.BarrierTracker
 *
 * 1.0 BarrierTracker implements CheckpointBarrierHandler（BarrierTracker.java:40）
 * —— AT_LEAST_ONCE 模式：**只数屏障，不阻塞任何通道**。所以对齐期间仍有数据
 * 流过算子，快照里可能包含「屏障之后」的记录，故障恢复会重放 → at-least-once。
 *
 * 1.0 注释：「The tracker keeps track of the number of barriers for each
 * checkpoint... Checkpoints are expected to be numbered in increasing order.」
 *
 * 字段（:45-64）：
 *   private static final int MAX_CHECKPOINTS_TO_TRACK = 50;
 *   private final InputGate inputGate;
 *   private final int totalNumberOfInputChannels;
 *   private final ArrayDeque<CheckpointBarrierCount> pendingCheckpoints;
 *   private EventListener<CheckpointBarrier> checkpointHandler;
 *   private long latestPendingCheckpointID = -1;
 *
 * processBarrier 的三条要点（:110）：
 *   1) totalNumberOfInputChannels == 1 时走 fast path，屏障一到直接通知
 *   2) 某 checkpoint 数满 totalNumberOfInputChannels 时，连它**和它之前所有**
 *      pending 的一起出队（被新 checkpoint subsume 掉了），再通知
 *   3) 新 id 必须大于 latestPendingCheckpointID 才入队；队列超过 50 条就丢最老的
 */
#ifndef FLINK_STREAMING_RUNTIME_IO_BARRIERTRACKER_H
#define FLINK_STREAMING_RUNTIME_IO_BARRIERTRACKER_H

#include <cstdint>
#include <deque>

#include "flink/runtime/io/network/partition/consumer/InputGate.h"
#include "flink/streaming/runtime/io/CheckpointBarrierHandler.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

class BarrierTracker : public CheckpointBarrierHandler {
 public:
  typedef flink::runtime::io::network::partition::consumer::InputGate InputGate;

  // 1.0 MAX_CHECKPOINTS_TO_TRACK = 50
  static const int MAX_CHECKPOINTS_TO_TRACK = 50;

  explicit BarrierTracker(InputGate* inputGate);

  bool getNextNonBlocked(BufferOrEvent& target) override;

  void registerCheckpointEventHandler(
      CheckpointEventListener* checkpointHandler) override;

  // 1.0 cleanup(): pendingCheckpoints.clear();
  void cleanup() override { pendingCheckpoints_.clear(); }

  // 1.0 isEmpty(): return pendingCheckpoints.isEmpty();
  bool isEmpty() const override { return pendingCheckpoints_.empty(); }

 private:
  // 1.0 private static final class CheckpointBarrierCount（:170）
  struct CheckpointBarrierCount {
    int64_t checkpointId;
    int barrierCount;  // 1.0 的构造里初始为 1
  };

  void processBarrier(const CheckpointBarrier& receivedBarrier);

  InputGate* inputGate_;
  int totalNumberOfInputChannels_;
  std::deque<CheckpointBarrierCount> pendingCheckpoints_;
  CheckpointEventListener* checkpointHandler_;
  int64_t latestPendingCheckpointID_;
};

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_IO_BARRIERTRACKER_H
