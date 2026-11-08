#include "flink/runtime/io/network/api/writer/RecordWriter.h"

#include "flink/runtime/io/network/api/writer/RoundRobinChannelSelector.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace api {
namespace writer {

// 1.0 RecordWriter(ResultPartitionWriter)（:56）
RecordWriter::RecordWriter(ResultPartitionWriter* writer)
    : writer_(writer),
      channelSelector_(new RoundRobinChannelSelector()),
      numChannels_(writer->getNumberOfOutputChannels()) {}

// 1.0 RecordWriter(ResultPartitionWriter, ChannelSelector<T>)（:61）
RecordWriter::RecordWriter(
    ResultPartitionWriter* writer,
    const std::shared_ptr<ChannelSelector>& channelSelector)
    : writer_(writer),
      channelSelector_(channelSelector),
      // 1.0: numChannels = writer.getNumberOfOutputChannels();
      numChannels_(writer->getNumberOfOutputChannels()) {}

// 1.0 emit(T record)（:78）
void RecordWriter::emit(const RecordPtr& record) {
  // 1.0: for (int targetChannel : channelSelector.selectChannels(record, numChannels))
  const std::vector<int>& targetChannels =
      channelSelector_->selectChannels(record, numChannels_);
  for (int targetChannel : targetChannels) {
    // 【偏差 1】1.0 这里是 serializer.addRecord + 写满页才 writeBuffer
    writer_->writeBuffer(record, targetChannel);
  }
}

// 1.0 broadcastEmit(T record)（:103）
// 1.0 注释：「This is used to broadcast Streaming Watermarks in-band with
// records. This ignores the ChannelSelector.」
void RecordWriter::broadcastEmit(const RecordPtr& record) {
  for (int targetChannel = 0; targetChannel < numChannels_; ++targetChannel) {
    writer_->writeBuffer(record, targetChannel);
  }
}

// 1.0 broadcastEvent(AbstractEvent event)（:124）
// 1.0 在写事件前要先把该通道 serializer 里**已有的数据**推出去，保证
// 「事件不会插到一条记录的中间」。本实现一条记录一个 Buffer，天然不可能被
// 插断，所以直接写事件即可（偏差 1 的直接后果）。
void RecordWriter::broadcastEvent(const EventPtr& event) {
  for (int targetChannel = 0; targetChannel < numChannels_; ++targetChannel) {
    writer_->writeEvent(event, targetChannel);
  }
}

// 1.0 flush()（:175）—— 见头文件偏差 2
void RecordWriter::flush() {}

// 1.0 clearBuffers()（:192）—— 见头文件偏差 2
void RecordWriter::clearBuffers() {}

}  // namespace writer
}  // namespace api
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
