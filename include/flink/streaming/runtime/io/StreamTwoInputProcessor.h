/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor
 *
 * 1.0 StreamTwoInputProcessor<IN1, IN2>（StreamTwoInputProcessor.java:65）——
 * 两输入算子的「取数据 → 分辨是哪条输入 → 推进该输入的水位线 → 调算子」主循环。
 *
 * 1.0 类注释（:49-63）：
 *   「Input reader for TwoInputStreamTask.
 *     This also keeps track of Watermark events and forwards them to event
 *     subscribers once the Watermark from all inputs advances.
 *     Forwarding elements or watermarks must be protected by synchronizing on
 *     the given lock object. This ensures that we don't call methods on a
 *     TwoInputStreamOperator concurrently with the timer callback or other
 *     things.」
 *
 * 字段（:67-88）：
 *   private final RecordDeserializer<...>[] recordDeserializers;
 *   private RecordDeserializer<...> currentRecordDeserializer;
 *   private int currentChannel = -1;          // 「We need to keep track of the
 *                                             //   channel from which a buffer
 *                                             //   came, so that we can
 *                                             //   appropriately map the
 *                                             //   watermarks to input
 *                                             //   channels」
 *   private boolean isFinished;
 *   private final CheckpointBarrierHandler barrierHandler;
 *   private final long[] watermarks1;  private long lastEmittedWatermark1;
 *   private final long[] watermarks2;  private long lastEmittedWatermark2;
 *   private final int numInputChannels1;
 *   private final DeserializationDelegate<StreamElement> deserializationDelegate1/2;
 *
 * 构造（:91）：
 *   StreamTwoInputProcessor(Collection<InputGate> inputGates1,
 *       Collection<InputGate> inputGates2,
 *       TypeSerializer<IN1>, TypeSerializer<IN2>,
 *       EventListener<CheckpointBarrier> checkpointListener,
 *       CheckpointingMode, IOManager, boolean enableWatermarkMultiplexing)
 *   - InputGateUtil.createInputGate(inputGates1, inputGates2)  (:101)
 *     —— 两组 gate 先拼成一个**扁平通道空间**（见 UnionInputGate.h）
 *   - EXACTLY_ONCE  -> new BarrierBuffer(inputGate, ioManager)
 *     AT_LEAST_ONCE -> new BarrierTracker(inputGate)
 *     其它          -> IllegalArgumentException("Unrecognized CheckpointingMode")
 *   - barrierHandler.registerCheckpointEventHandler(checkpointListener)
 *   - 「determine which unioned channels belong to input 1 and which belong to
 *      input 2」(:144)：numInputChannels1 = Σ gate.getNumberOfInputChannels()
 *      over inputGates1；剩下的就是 input 2。于是运行时只要比较
 *      `currentChannel < numInputChannels1` 就知道这条记录来自哪条输入。
 *   - watermarks1/2 全填 Long.MIN_VALUE，lastEmittedWatermark1/2 = Long.MIN_VALUE
 *
 * boolean processInput(TwoInputStreamOperator<IN1,IN2,?> op, Object lock)（:163）：
 *   input 1（currentChannel < numInputChannels1）：
 *       水位线 -> handleWatermark(...); continue;
 *       记录   -> synchronized (lock) { op.setKeyContextElement1(r);
 *                                       op.processElement1(r); } return true;
 *   input 2：
 *       水位线 -> handleWatermark(...); continue;
 *       记录   -> synchronized (lock) { op.setKeyContextElement2(r);
 *                                       op.processElement2(r); } return true;
 *   事件：  只接受 EndOfPartitionEvent，其它一律
 *           throw new IOException("Unexpected event: " + event)
 *   结束：  isFinished = true；若 !barrierHandler.isEmpty() 则
 *           throw new IllegalStateException("Trailing data in checkpoint
 *           barrier handler.")
 *
 * private void handleWatermark(op, mark, channelIndex, lock)（:241）：
 *   按 channelIndex 落到 watermarks1 还是 watermarks2（后者先减掉
 *   numInputChannels1 做局部下标），然后与单输入完全同构：
 *     只有该通道水位线**变大**才重算该输入**所有通道的最小值**，
 *     最小值超过 lastEmittedWatermarkN 才真正下发 processWatermarkN。
 *   —— 注意这里只做「同一条输入内部多通道取 min」；**两条输入之间**再取 min
 *   是算子的责任（见 CoStreamMap.processWatermark1/2 的 combinedWatermark）。
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 没有反序列化层】同 StreamInputProcessor.h 偏差 1：1.0 的一个
 * Buffer 是字节页，需要 per-channel 的
 * SpillingAdaptiveSpanningRecordDeserializer 配合两个
 * DeserializationDelegate（input 1 和 input 2 的元素类型不同，所以 1.0 要两个
 * delegate，并按 currentChannel 选用哪一个）。本实现的一个 Buffer 就是一个
 * StreamElement（见 BufferOrEvent.h），所以 recordDeserializers /
 * currentRecordDeserializer / deserializationDelegate1 / deserializationDelegate2
 * / enableWatermarkMultiplexing 这一整层都不需要。currentChannel 必须保留 ——
 * 它既定水位线归哪个通道，又定这条记录归哪条输入。
 *
 * 【偏差 2】构造参数去掉 TypeSerializer<IN1> / TypeSerializer<IN2> / IOManager /
 * enableWatermarkMultiplexing，原因同上。
 *
 * 【偏差 3 —— setReporter 不复刻】1.0 的
 * setReporter(AccumulatorRegistry.Reporter)（:276）把 I/O 统计塞给每个
 * deserializer；本实现没有 deserializer，也没有 AccumulatorRegistry。
 *
 * 【偏差 4】cleanup() 里 1.0 要先回收未读完的 Buffer（buffer.recycle()）再
 * barrierHandler.cleanup()（:282）；本实现没有缓冲池，只保留后者。
 *
 * 【偏差 5 —— numInputChannels2 的用途】1.0 用它开 watermarks2 数组；这里
 * watermarks2_ 同样按它定长，但不单独存字段（需要时就是
 * watermarks2_.size()）。
 */
#ifndef FLINK_STREAMING_RUNTIME_IO_STREAMTWOINPUTPROCESSOR_H
#define FLINK_STREAMING_RUNTIME_IO_STREAMTWOINPUTPROCESSOR_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "flink/runtime/io/network/partition/consumer/InputGate.h"
#include "flink/streaming/api/CheckpointingMode.h"
#include "flink/streaming/api/operators/TwoInputStreamOperator.h"
#include "flink/streaming/runtime/io/CheckpointBarrierHandler.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

class StreamTwoInputProcessor {
 public:
  typedef flink::runtime::io::network::partition::consumer::InputGate InputGate;
  typedef flink::streaming::api::operators::TwoInputStreamOperator
      TwoInputStreamOperator;
  typedef flink::runtime::io::network::api::CheckpointBarrier CheckpointBarrier;
  typedef flink::runtime::util::event::EventListener<CheckpointBarrier>
      CheckpointEventListener;
  typedef flink::streaming::api::watermark::Watermark Watermark;

  StreamTwoInputProcessor(
      const std::vector<InputGate*>& inputGates1,
      const std::vector<InputGate*>& inputGates2,
      CheckpointEventListener* checkpointListener,
      flink::streaming::api::CheckpointingMode checkpointMode);

  // 1.0 processInput(TwoInputStreamOperator<IN1,IN2,?>, Object lock)
  // 返回 false 表示两条输入的所有通道都结束了
  bool processInput(TwoInputStreamOperator* streamOperator,
                    std::recursive_mutex& lock);

  // 1.0 cleanup()
  void cleanup();

 private:
  // 1.0 private handleWatermark(op, Watermark, int channelIndex, Object lock)
  void handleWatermark(TwoInputStreamOperator* streamOperator,
                       const Watermark& mark, int channelIndex,
                       std::recursive_mutex& lock);

  std::shared_ptr<InputGate> inputGate_;
  std::unique_ptr<CheckpointBarrierHandler> barrierHandler_;

  // 1.0: "We need to keep track of the channel from which a buffer came, so
  // that we can appropriately map the watermarks to input channels"
  int currentChannel_;
  bool isFinished_;

  // 1.0: 扁平通道空间里 [0, numInputChannels1) 属于 input 1，其余属于 input 2
  int numInputChannels1_;

  std::vector<int64_t> watermarks1_;
  int64_t lastEmittedWatermark1_;

  std::vector<int64_t> watermarks2_;
  int64_t lastEmittedWatermark2_;
};

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_IO_STREAMTWOINPUTPROCESSOR_H
