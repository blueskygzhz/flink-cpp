/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.io.StreamRecordWriter
 *
 * 1.0 class StreamRecordWriter<T extends IOReadableWritable>
 *       extends RecordWriter<T>（StreamRecordWriter.java:37）
 *
 * 1.0 原注释：「This record writer keeps data in buffers at most for a certain
 * timeout. It spawns a separate thread that flushes the outputs in a defined
 * interval, to make sure data does not linger in the buffers for too long.」
 *
 * 字段（:40-50）：
 *   private static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";
 *   private final OutputFlusher outputFlusher;
 *   private final boolean flushAlways;
 *   private Throwable flusherException;
 *
 * 构造（:54-81）：timeout 的三分支语义（checkArgument(timeout >= -1)）
 *   timeout == -1 : flushAlways = false, 不起线程   （攒满一页才发，吞吐优先）
 *   timeout ==  0 : flushAlways = true,  不起线程   （每条记录立即发，延迟优先）
 *   timeout  >  0 : flushAlways = false, 起 OutputFlusher 线程，周期 flush
 *   线程名: taskName == null ? "OutputFlusher" : "Output Timeout Flusher - " + taskName
 *   StreamConfig.getBufferTimeout() 的默认值是 100（见 StreamConfig.h）
 *
 * 方法：
 *   void emit(T)           -> checkErroneous(); super.emit(r);
 *                             if (flushAlways) flush();          (:84)
 *   void broadcastEmit(T)  -> 同上，调 super.broadcastEmit        (:93)
 *   void close()           -> outputFlusher.terminate(); join()   (:104)
 *   private void notifyFlusherException(Throwable)                (:122)
 *   private void checkErroneous()  -> throw new IOException(
 *       "An exception happened while flushing the outputs", flusherException)  (:128)
 *   private class OutputFlusher extends Thread（daemon）           (:141)
 *       run(): while (running) { sleep(timeout); flush(); }
 *              catch (Throwable t) { notifyFlusherException(t); }
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 不起 OutputFlusher 线程】本实现的 flush() 是空动作
 * （见 RecordWriter.h 的偏差 2：没有 serializer 层，不存在「滞留在半满
 * Buffer 里的数据」），所以周期性 flush 线程无事可做，起它只会白耗一条线程。
 * timeout 的三分支**判定仍然原样保留**（含 timeout >= -1 的校验与
 * flushAlways 的取值），线程名也保留在 defaultOutputFlushThreadName()，
 * 这样等将来补上序列化层时，把线程加回 startOutputFlusher() 即可。
 * flusherException / checkErroneous() 一并保留，调用点与 1.0 一致。
 *
 * 【偏差 2 —— 元素类型】同 RecordWriter：T 固定为 RecordPtr。
 */
#ifndef FLINK_STREAMING_RUNTIME_IO_STREAMRECORDWRITER_H
#define FLINK_STREAMING_RUNTIME_IO_STREAMRECORDWRITER_H

#include <cstdint>
#include <memory>
#include <string>

#include "flink/runtime/io/network/api/writer/RecordWriter.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

class StreamRecordWriter
    : public flink::runtime::io::network::api::writer::RecordWriter {
 public:
  typedef flink::runtime::io::network::api::writer::ChannelSelector
      ChannelSelector;
  typedef flink::runtime::io::network::api::writer::ResultPartitionWriter
      ResultPartitionWriter;

  // 1.0 DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher"
  static const char* defaultOutputFlushThreadName() { return "OutputFlusher"; }

  // 1.0 StreamRecordWriter(ResultPartitionWriter, ChannelSelector<T>, long)
  StreamRecordWriter(ResultPartitionWriter* writer,
                     const std::shared_ptr<ChannelSelector>& channelSelector,
                     int64_t timeout);

  // 1.0 StreamRecordWriter(ResultPartitionWriter, ChannelSelector<T>, long, String)
  StreamRecordWriter(ResultPartitionWriter* writer,
                     const std::shared_ptr<ChannelSelector>& channelSelector,
                     int64_t timeout, const std::string& taskName);

  void emit(const RecordPtr& record) override;
  void broadcastEmit(const RecordPtr& record) override;

  // 1.0 close()：「Closes the writer. This stops the flushing thread (if there
  // is one).」
  void close();

  bool isFlushAlways() const { return flushAlways_; }
  const std::string& getFlushThreadName() const { return flushThreadName_; }

 private:
  // 1.0 notifyFlusherException(Throwable t)
  void notifyFlusherException(const std::string& message);
  // 1.0 checkErroneous()
  void checkErroneous() const;

  void init(int64_t timeout, const std::string& taskName);

  bool flushAlways_;
  std::string flushThreadName_;  // 空串 = 不需要 flusher 线程
  std::string flusherException_;
};

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_IO_STREAMRECORDWRITER_H
