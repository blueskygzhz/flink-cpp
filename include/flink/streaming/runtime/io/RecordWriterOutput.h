/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.io.RecordWriterOutput
 *
 * 1.0 class RecordWriterOutput<OUT> implements Output<StreamRecord<OUT>>
 * （RecordWriterOutput.java:40）
 *
 * 1.0 原注释：「Implementation of Output that sends data using a RecordWriter.」
 * 这是算子链最末端的输出：链内算子之间用 ChainingOutput 直接调方法，链尾则
 * 通过它把记录交给 RecordWriter 发往下游 task。
 *
 * 字段（:42-44）：
 *   private StreamRecordWriter<SerializationDelegate<StreamElement>> recordWriter;
 *   private SerializationDelegate<StreamElement> serializationDelegate;
 *
 * 构造（:48-71）：
 *   RecordWriterOutput(StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>,
 *                      TypeSerializer<OUT> outSerializer,
 *                      boolean enableWatermarkMultiplexing)
 *   - checkNotNull(recordWriter)
 *   - 1.0 注释：「generic hack: cast the writer to generic Object type so we
 *     can use it with multiplexed records and watermarks」
 *   - enableWatermarkMultiplexing ? MultiplexingStreamRecordSerializer
 *                                 : StreamRecordSerializer
 *
 * 方法：
 *   void collect(StreamRecord<OUT> record)（:74）
 *       serializationDelegate.setInstance(record);
 *       try { recordWriter.emit(serializationDelegate); }
 *       catch (Exception e) { throw new RuntimeException(e.getMessage(), e); }
 *   void emitWatermark(Watermark mark)（:86）
 *       serializationDelegate.setInstance(mark);
 *       recordWriter.broadcastEmit(serializationDelegate);   // 水位线广播
 *   void broadcastEvent(AbstractEvent barrier)（:97）
 *   void flush()（:102）  void close()（:107）  void clearBuffers()（:111）
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 没有 SerializationDelegate】1.0 复用**同一个**
 * SerializationDelegate 实例，setInstance 之后立刻 emit —— 因为 emit 当场就把
 * 记录序列化进字节 Buffer 了，delegate 本身不需要留存。本实现把
 * StreamElement 的 shared_ptr 直接放进下游队列，元素必须活到被消费为止，
 * 所以这里为每条记录**拷一份**到新的 shared_ptr。
 * 随之不复刻：TypeSerializer<OUT> / MultiplexingStreamRecordSerializer /
 * StreamRecordSerializer / enableWatermarkMultiplexing 构造参数。
 * （enableWatermarkMultiplexing 在 1.0 里只决定序列化器带不带「这是记录还是
 *   水位线」的标记位；本实现靠 StreamElement 的虚函数区分，天然多路复用。）
 *
 * 【偏差 2 —— flush() 进了 Output 接口】1.0 的 Output 接口没有 flush()，
 * StreamTask/OperatorChain 直接持 RecordWriterOutput[] 才能调到它。
 * 本实现把 flush() 与 broadcastEvent() 一并放进 Output（理由见 Output.h），
 * 这样 StreamTask.streamOutputs_ 保持 Output* 即可。
 */
#ifndef FLINK_STREAMING_RUNTIME_IO_RECORDWRITEROUTPUT_H
#define FLINK_STREAMING_RUNTIME_IO_RECORDWRITEROUTPUT_H

#include "flink/streaming/api/operators/Output.h"
#include "flink/streaming/runtime/io/StreamRecordWriter.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

class RecordWriterOutput : public flink::streaming::api::operators::Output {
 public:
  typedef flink::streaming::runtime::streamrecord::StreamRecord StreamRecord;
  typedef flink::streaming::api::watermark::Watermark Watermark;
  typedef std::shared_ptr<const flink::runtime::event::AbstractEvent> EventPtr;
  typedef StreamRecordWriter::RecordPtr RecordPtr;

  // 1.0: checkNotNull(recordWriter)
  explicit RecordWriterOutput(StreamRecordWriter* recordWriter);

  // 1.0 collect(StreamRecord<OUT> record)
  void collect(const StreamRecord& record) override;

  // 1.0 emitWatermark(Watermark mark) —— 走 broadcastEmit，发往所有下游通道
  void emitWatermark(const Watermark& mark) override;

  // 1.0 broadcastEvent(AbstractEvent barrier)
  void broadcastEvent(const EventPtr& event) override;

  // 1.0 flush()
  void flush() override;

  // 1.0 close()
  void close() override;

  // 1.0 clearBuffers()
  void clearBuffers();

 private:
  StreamRecordWriter* recordWriter_;
};

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_IO_RECORDWRITEROUTPUT_H
