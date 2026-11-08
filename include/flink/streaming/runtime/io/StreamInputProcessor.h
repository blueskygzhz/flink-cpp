/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.io.StreamInputProcessor
 *
 * 1.0 StreamInputProcessor<IN>（StreamInputProcessor.java:62）—— 单输入算子的
 * 「取数据 → 推进水位线 → 调算子」主循环。
 *
 * 字段（:64-82）：
 *   private final RecordDeserializer<...>[] recordDeserializers;
 *   private RecordDeserializer<...> currentRecordDeserializer;
 *   private final CheckpointBarrierHandler barrierHandler;
 *   private int currentChannel = -1;
 *   private boolean isFinished;
 *   private final long[] watermarks;      // 每个输入通道各记一个
 *   private long lastEmittedWatermark;
 *   private final DeserializationDelegate<StreamElement> deserializationDelegate;
 *
 * 构造（:85）：
 *   StreamInputProcessor(InputGate[] inputGates, TypeSerializer<IN>,
 *       EventListener<CheckpointBarrier> checkpointListener,
 *       CheckpointingMode, IOManager, boolean enableWatermarkMultiplexing)
 *   - InputGateUtil.createInputGate(inputGates) 合并多个 gate
 *   - EXACTLY_ONCE  -> new BarrierBuffer(inputGate, ioManager)
 *     AT_LEAST_ONCE -> new BarrierTracker(inputGate)
 *     其它          -> IllegalArgumentException("Unrecognized CheckpointingMode")
 *   - barrierHandler.registerCheckpointEventHandler(checkpointListener)
 *   - watermarks[i] = Long.MIN_VALUE; lastEmittedWatermark = Long.MIN_VALUE;
 *
 * boolean processInput(OneInputStreamOperator<IN,?> op, Object lock)（:130）：
 *   水位线：只有当某通道的水位线**变大**时，才重算所有通道的**最小值**；
 *           最小值大于 lastEmittedWatermark 才真正下发 —— 这就是
 *           「多输入水位线取 min 且单调不减」的全部实现。
 *   记录：  synchronized (lock) { op.setKeyContextElement1(r);
 *                                 op.processElement(r); } 然后 return true
 *   事件：  只接受 EndOfPartitionEvent，其它一律
 *           throw new IOException("Unexpected event: " + event)
 *   结束：  isFinished = true；若 !barrierHandler.isEmpty() 则
 *           throw new IllegalStateException("Trailing data in checkpoint
 *           barrier handler.")
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 没有反序列化层】1.0 的一个 Buffer 是字节页，里面可能有多条记录、
 * 也可能有跨页的半条记录，所以要 per-channel 的
 * SpillingAdaptiveSpanningRecordDeserializer 配合 DeserializationDelegate，
 * 还有 isBufferConsumed() / isFullRecord() / buffer.recycle() 那套状态机。
 * 本实现的一个 Buffer 就是一个 StreamElement（见 BufferOrEvent.h），
 * 所以 recordDeserializers / currentRecordDeserializer /
 * deserializationDelegate / enableWatermarkMultiplexing（它只决定用
 * MultiplexingStreamRecordSerializer 还是 StreamRecordSerializer）
 * 这一整层都不需要。currentChannel 仍然保留——水位线按通道记，必须有它。
 *
 * 【偏差 2】构造参数去掉 TypeSerializer<IN> / IOManager /
 * enableWatermarkMultiplexing，原因同上。
 *
 * 【偏差 3】cleanup() 里 1.0 要回收未读完的 Buffer（buffer.recycle()）再
 * barrierHandler.cleanup()；本实现没有缓冲池，只保留后者。
 */
#ifndef FLINK_STREAMING_RUNTIME_IO_STREAMINPUTPROCESSOR_H
#define FLINK_STREAMING_RUNTIME_IO_STREAMINPUTPROCESSOR_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "flink/runtime/io/network/partition/consumer/InputGate.h"
#include "flink/streaming/api/CheckpointingMode.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"
#include "flink/streaming/runtime/io/CheckpointBarrierHandler.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

class StreamInputProcessor {
 public:
  typedef flink::runtime::io::network::partition::consumer::InputGate InputGate;
  typedef flink::streaming::api::operators::OneInputStreamOperator
      OneInputStreamOperator;
  typedef flink::runtime::io::network::api::CheckpointBarrier CheckpointBarrier;
  typedef flink::runtime::util::event::EventListener<CheckpointBarrier>
      CheckpointEventListener;

  StreamInputProcessor(const std::vector<InputGate*>& inputGates,
                       CheckpointEventListener* checkpointListener,
                       flink::streaming::api::CheckpointingMode checkpointMode);

  // 1.0 processInput(OneInputStreamOperator<IN,?>, Object lock)
  // 返回 false 表示所有输入都结束了
  bool processInput(OneInputStreamOperator* streamOperator, std::mutex& lock);

  // 1.0 cleanup()
  void cleanup();

 private:
  std::shared_ptr<InputGate> inputGate_;
  std::unique_ptr<CheckpointBarrierHandler> barrierHandler_;

  // 1.0: "We need to keep track of the channel from which a buffer came, so
  // that we can appropriately map the watermarks to input channels"
  int currentChannel_;
  bool isFinished_;

  std::vector<int64_t> watermarks_;
  int64_t lastEmittedWatermark_;
};

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_IO_STREAMINPUTPROCESSOR_H
