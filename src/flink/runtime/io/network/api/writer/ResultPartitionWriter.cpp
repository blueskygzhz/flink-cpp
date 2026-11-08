#include "flink/runtime/io/network/api/writer/ResultPartitionWriter.h"

#include <stdexcept>

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace api {
namespace writer {

namespace {
typedef flink::runtime::io::network::partition::consumer::BufferOrEvent
    BufferOrEvent;
}  // namespace

// 1.0 writeBuffer(Buffer buffer, int targetChannel)（:70）
void ResultPartitionWriter::writeBuffer(const BufferPtr& buffer,
                                        int targetChannel) {
  if (targetChannel < 0 || targetChannel >= getNumberOfOutputChannels()) {
    throw std::out_of_range("ResultPartitionWriter: no such subpartition");
  }
  const Target& target = subpartitions_[targetChannel];
  // 【偏差 3】channelIndex 由生产端填（1.0 由接收端的 InputChannel 填）
  target.gate->put(BufferOrEvent(buffer, target.channelIndex));
}

// 1.0 writeEvent(AbstractEvent event, int targetChannel)（:74）
void ResultPartitionWriter::writeEvent(const EventPtr& event,
                                       int targetChannel) {
  if (targetChannel < 0 || targetChannel >= getNumberOfOutputChannels()) {
    throw std::out_of_range("ResultPartitionWriter: no such subpartition");
  }
  const Target& target = subpartitions_[targetChannel];
  target.gate->put(BufferOrEvent(event, target.channelIndex));
}

// 1.0 writeEventToAllChannels(AbstractEvent event)（:78）
// 1.0 为每个 subpartition 单独 EventSerializer.toBuffer(event) 一份；本实现
// 事件对象不可变且按 shared_ptr 传递，各通道共用同一份即可。
void ResultPartitionWriter::writeEventToAllChannels(const EventPtr& event) {
  for (size_t i = 0; i < subpartitions_.size(); ++i) {
    writeEvent(event, static_cast<int>(i));
  }
}

}  // namespace writer
}  // namespace api
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
