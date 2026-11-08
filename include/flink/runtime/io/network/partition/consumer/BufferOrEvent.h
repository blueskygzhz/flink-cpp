/*
 * flink-runtime : org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent
 *
 * 1.0 BufferOrEvent.java: InputGate 取出的东西要么是数据缓冲(Buffer)，
 * 要么是运行时事件(AbstractEvent)，并且**一定**带 channelIndex ——
 * barrier 对齐和 per-channel 水印推进都依赖通道号。
 *
 * 【偏差】1.0 的 Buffer 是序列化后的字节缓冲（一个 Buffer 里可能有多条记录）；
 * 本实现进程内传输，不做序列化，所以「Buffer」直接就是一个 StreamElement
 * （StreamRecord 或 Watermark）。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_BUFFERORVENT_H
#define FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_BUFFERORVENT_H

#include <memory>

#include "flink/runtime/event/AbstractEvent.h"
#include "flink/streaming/runtime/streamrecord/StreamElement.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace partition {
namespace consumer {

class BufferOrEvent {
 public:
  typedef flink::streaming::runtime::streamrecord::StreamElement StreamElement;
  typedef flink::runtime::event::AbstractEvent AbstractEvent;

  BufferOrEvent() : channelIndex_(0) {}
  BufferOrEvent(std::shared_ptr<const StreamElement> buffer, int channelIndex)
      : buffer_(buffer), channelIndex_(channelIndex) {}
  BufferOrEvent(std::shared_ptr<const AbstractEvent> event, int channelIndex)
      : event_(event), channelIndex_(channelIndex) {}

  bool isBuffer() const { return static_cast<bool>(buffer_); }
  bool isEvent() const { return static_cast<bool>(event_); }

  const std::shared_ptr<const StreamElement>& getBuffer() const {
    return buffer_;
  }
  const std::shared_ptr<const AbstractEvent>& getEvent() const {
    return event_;
  }

  int getChannelIndex() const { return channelIndex_; }
  void setChannelIndex(int channelIndex) { channelIndex_ = channelIndex; }

 private:
  std::shared_ptr<const StreamElement> buffer_;
  std::shared_ptr<const AbstractEvent> event_;
  int channelIndex_;
};

}  // namespace consumer
}  // namespace partition
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_BUFFERORVENT_H
