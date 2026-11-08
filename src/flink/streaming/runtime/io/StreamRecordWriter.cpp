#include "flink/streaming/runtime/io/StreamRecordWriter.h"

#include <stdexcept>

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

// 1.0 StreamRecordWriter(writer, channelSelector, timeout)
//     -> this(writer, channelSelector, timeout, null)（:54）
StreamRecordWriter::StreamRecordWriter(
    ResultPartitionWriter* writer,
    const std::shared_ptr<ChannelSelector>& channelSelector, int64_t timeout)
    : RecordWriter(writer, channelSelector), flushAlways_(false) {
  init(timeout, std::string());
}

// 1.0 StreamRecordWriter(writer, channelSelector, timeout, taskName)（:58）
StreamRecordWriter::StreamRecordWriter(
    ResultPartitionWriter* writer,
    const std::shared_ptr<ChannelSelector>& channelSelector, int64_t timeout,
    const std::string& taskName)
    : RecordWriter(writer, channelSelector), flushAlways_(false) {
  init(timeout, taskName);
}

// 1.0 构造函数体（:61-81）
void StreamRecordWriter::init(int64_t timeout, const std::string& taskName) {
  // 1.0: checkArgument(timeout >= -1);
  if (timeout < -1) {
    throw std::invalid_argument("StreamRecordWriter: timeout must be >= -1");
  }

  if (timeout == -1) {
    flushAlways_ = false;
    flushThreadName_.clear();  // 1.0: outputFlusher = null
  } else if (timeout == 0) {
    flushAlways_ = true;
    flushThreadName_.clear();  // 1.0: outputFlusher = null
  } else {
    flushAlways_ = false;
    // 1.0: taskName == null ? DEFAULT_OUTPUT_FLUSH_THREAD_NAME
    //                       : "Output Timeout Flusher - " + taskName
    flushThreadName_ = taskName.empty()
                           ? std::string(defaultOutputFlushThreadName())
                           : "Output Timeout Flusher - " + taskName;
    // 【偏差 1】1.0 这里 new OutputFlusher(threadName, timeout).start()
  }
}

// 1.0 emit(T record)（:84）
void StreamRecordWriter::emit(const RecordPtr& record) {
  checkErroneous();
  RecordWriter::emit(record);
  if (flushAlways_) {
    flush();
  }
}

// 1.0 broadcastEmit(T record)（:93）
void StreamRecordWriter::broadcastEmit(const RecordPtr& record) {
  checkErroneous();
  RecordWriter::broadcastEmit(record);
  if (flushAlways_) {
    flush();
  }
}

// 1.0 close()（:104）
void StreamRecordWriter::close() {
  // 1.0: "make sure we terminate the thread in any case"
  //      outputFlusher.terminate(); outputFlusher.join();
  // 【偏差 1】没有 flusher 线程，无需 terminate/join
}

// 1.0 notifyFlusherException(Throwable t)（:122）
void StreamRecordWriter::notifyFlusherException(const std::string& message) {
  // 1.0: 只记第一个异常
  if (flusherException_.empty()) {
    flusherException_ = message;
  }
}

// 1.0 checkErroneous()（:128）
void StreamRecordWriter::checkErroneous() const {
  if (!flusherException_.empty()) {
    throw std::runtime_error(
        "An exception happened while flushing the outputs: " +
        flusherException_);
  }
}

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
