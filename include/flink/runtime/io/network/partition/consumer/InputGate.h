/*
 * flink-runtime : org.apache.flink.runtime.io.network.partition.consumer.InputGate
 *
 * 1.0 InputGate.java:
 *   int getNumberOfInputChannels()
 *   boolean isFinished()
 *   void requestPartitions()
 *   BufferOrEvent getNextBufferOrEvent()   // 阻塞直到有数据或结束
 *   void sendTaskEvent(TaskEvent) / registerListener(...)
 *
 * getNextBufferOrEvent 这里改成 bool getNextBufferOrEvent(BufferOrEvent& target)：
 * 返回 false 表示 gate 结束（1.0 是返回 null）。
 * sendTaskEvent/registerListener 服务于迭代作业与 netty 事件，本实现没有。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_INPUTGATE_H
#define FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_INPUTGATE_H

#include "flink/runtime/io/network/partition/consumer/BufferOrEvent.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace partition {
namespace consumer {

class InputGate {
 public:
  virtual ~InputGate() {}

  virtual int getNumberOfInputChannels() const = 0;
  virtual bool isFinished() const = 0;
  virtual void requestPartitions() = 0;
  virtual bool getNextBufferOrEvent(BufferOrEvent& target) = 0;
};

}  // namespace consumer
}  // namespace partition
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_INPUTGATE_H
