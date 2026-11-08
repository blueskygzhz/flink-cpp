/*
 * flink-runtime : org.apache.flink.runtime.io.network.api.writer.RecordWriter
 *
 * 1.0 class RecordWriter<T extends IOReadableWritable>（RecordWriter.java:45）
 *
 * 1.0 原注释：「The RecordWriter wraps the runtime's ResultPartitionWriter and
 * takes care of serializing records into buffers. Important: it is necessary
 * to call flush() after all records have been written with emit(). This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).」
 *
 * 字段（:47-54）：
 *   protected final ResultPartitionWriter writer;
 *   private final ChannelSelector<T> channelSelector;
 *   private final int numChannels;
 *   private final RecordSerializer<T>[] serializers;   // 每个输出通道一个
 *
 * 构造（:56-76）：
 *   RecordWriter(ResultPartitionWriter writer)
 *       -> this(writer, new RoundRobinChannelSelector<T>())
 *   RecordWriter(ResultPartitionWriter writer, ChannelSelector<T>)
 *       numChannels = writer.getNumberOfOutputChannels();
 *       serializers[i] = new SpanningRecordSerializer<T>();
 *   1.0 注释：「The runtime exposes a channel abstraction for the produced
 *   results. Every channel has an independent serializer.」
 *
 * 方法：
 *   void emit(T record)                      (:78)
 *   void broadcastEmit(T record)             (:103) 1.0 注释：「This is used to
 *       broadcast Streaming Watermarks in-band with records. This ignores the
 *       ChannelSelector.」
 *   void broadcastEvent(AbstractEvent event) (:124)
 *   void sendEndOfSuperstep()                (:155)
 *   void flush()                             (:175)
 *   void clearBuffers()                      (:192)
 *   void setReporter(AccumulatorRegistry.Reporter)  (:210)
 *   private void writeBuffer(Buffer, int targetChannel, RecordSerializer<T>)
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 没有 serializer 层】这是本类与 1.0 差别最大的地方。1.0 的
 * emit 是：
 *   synchronized (serializer) {
 *       SerializationResult result = serializer.addRecord(record);
 *       while (result.isFullBuffer()) {              // 记录可能跨多个 Buffer
 *           Buffer buffer = serializer.getCurrentBuffer();
 *           if (buffer != null) writeBuffer(buffer, targetChannel, serializer);
 *           buffer = writer.getBufferProvider().requestBufferBlocking();
 *           result = serializer.setNextBuffer(buffer);
 *       }
 *   }
 * 即「把记录写进当前页，写满就推给 ResultPartition 再要一页」。本实现一个
 * Buffer 就是一个 StreamElement（见 BufferOrEvent.h），没有页、没有跨页、
 * 没有缓冲池，所以 emit 直接 writer.writeBuffer(record, targetChannel)。
 * 随之不复刻：serializers[] / SpanningRecordSerializer /
 * SerializationResult / requestBufferBlocking / private writeBuffer。
 * 背压不丢——它落到了下游 SingleInputGate 的有界队列上
 * （见 ResultPartitionWriter.h 的偏差 1）。
 *
 * 【偏差 2 —— flush() / clearBuffers() 成为空动作】它们在 1.0 的职责是把
 * 「滞留在半满 Buffer 里的数据」推出去 / 回收掉。本实现每条记录在 emit 里
 * 已经进了下游队列，不存在半满缓冲，所以无事可做。**方法仍然保留**：
 * StreamTask.invoke() 的 flushOutputs()（1.0 StreamTask.java:265）与
 * releaseOutputs() 的调用点、以及 StreamRecordWriter 的 flushAlways 语义
 * 都按 1.0 原样保留，等将来真要加序列化层时此处即是落点。
 *
 * 【偏差 3 —— synchronized (serializer) 去掉】1.0 对每个通道的 serializer
 * 加锁，是因为 OutputFlusher 线程会并发访问同一个 serializer（见
 * StreamRecordWriter）。本实现没有 serializer 也不起 flusher 线程，
 * 下游队列的 put() 自带锁，故不需要这层同步。
 *
 * 【偏差 4 —— 元素类型】1.0 的 T 是泛型；本实现固定为 RecordPtr
 * （= shared_ptr<const StreamElement>），理由见 ChannelSelector.h 的偏差 1。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_API_WRITER_RECORDWRITER_H
#define FLINK_RUNTIME_IO_NETWORK_API_WRITER_RECORDWRITER_H

#include <memory>

#include "flink/runtime/io/network/api/writer/ChannelSelector.h"
#include "flink/runtime/io/network/api/writer/ResultPartitionWriter.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace api {
namespace writer {

class RecordWriter {
 public:
  typedef ChannelSelector::RecordPtr RecordPtr;
  typedef ResultPartitionWriter::EventPtr EventPtr;

  // 1.0 RecordWriter(ResultPartitionWriter writer)
  //     -> this(writer, new RoundRobinChannelSelector<T>())
  explicit RecordWriter(ResultPartitionWriter* writer);

  // 1.0 RecordWriter(ResultPartitionWriter writer, ChannelSelector<T>)
  RecordWriter(ResultPartitionWriter* writer,
               const std::shared_ptr<ChannelSelector>& channelSelector);

  virtual ~RecordWriter() {}

  // 1.0 emit(T record)（:78）
  virtual void emit(const RecordPtr& record);

  // 1.0 broadcastEmit(T record)（:103）—— 无视 ChannelSelector，发往全部通道
  virtual void broadcastEmit(const RecordPtr& record);

  // 1.0 broadcastEvent(AbstractEvent event)（:124）
  void broadcastEvent(const EventPtr& event);

  // 1.0 flush()（:175）—— 见偏差 2
  void flush();

  // 1.0 clearBuffers()（:192）—— 见偏差 2
  void clearBuffers();

  int getNumChannels() const { return numChannels_; }

 protected:
  // 1.0: protected final ResultPartitionWriter writer;
  ResultPartitionWriter* writer_;

 private:
  std::shared_ptr<ChannelSelector> channelSelector_;
  int numChannels_;
};

}  // namespace writer
}  // namespace api
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_API_WRITER_RECORDWRITER_H
