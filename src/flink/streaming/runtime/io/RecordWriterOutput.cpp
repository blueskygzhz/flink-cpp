#include "flink/streaming/runtime/io/RecordWriterOutput.h"

#include <stdexcept>

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

// 1.0 构造（:48）
RecordWriterOutput::RecordWriterOutput(StreamRecordWriter* recordWriter)
    : recordWriter_(recordWriter) {
  // 1.0: checkNotNull(recordWriter)
  if (recordWriter_ == NULL) {
    throw std::invalid_argument("RecordWriterOutput: recordWriter is null");
  }
}

// 1.0 collect(StreamRecord<OUT> record)（:74）
void RecordWriterOutput::collect(const StreamRecord& record) {
  // 1.0: serializationDelegate.setInstance(record);
  // 【偏差 1】1.0 复用同一个 delegate（emit 当场序列化完）；本实现元素要活到
  // 被下游消费，必须拷一份
  RecordWriterOutput::RecordPtr element(new StreamRecord(record));

  try {
    recordWriter_->emit(element);
  } catch (const std::exception& e) {
    // 1.0: throw new RuntimeException(e.getMessage(), e)
    throw std::runtime_error(e.what());
  }
}

// 1.0 emitWatermark(Watermark mark)（:86）
void RecordWriterOutput::emitWatermark(const Watermark& mark) {
  RecordWriterOutput::RecordPtr element(new Watermark(mark));

  try {
    // 1.0: recordWriter.broadcastEmit(serializationDelegate)
    recordWriter_->broadcastEmit(element);
  } catch (const std::exception& e) {
    throw std::runtime_error(e.what());
  }
}

// 1.0 broadcastEvent(AbstractEvent barrier)（:97）
void RecordWriterOutput::broadcastEvent(const EventPtr& event) {
  recordWriter_->broadcastEvent(event);
}

// 1.0 flush()（:102）
void RecordWriterOutput::flush() { recordWriter_->flush(); }

// 1.0 close()（:107）
void RecordWriterOutput::close() { recordWriter_->close(); }

// 1.0 clearBuffers()（:111）
void RecordWriterOutput::clearBuffers() { recordWriter_->clearBuffers(); }

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
