/*
 * flink-runtime :
 *   org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter
 *
 * 1.0 final class ResultPartitionWriter implements EventListener<TaskEvent>
 * （ResultPartitionWriter.java:40）
 *
 * 1.0 原注释：「A buffer-oriented runtime result writer. The
 * ResultPartitionWriter is the runtime API for producing results. It supports
 * two kinds of data to be sent: buffers and events.」
 *
 * 字段（:42-44）：
 *   private final ResultPartition partition;
 *   private final TaskEventHandler taskEventHandler = new TaskEventHandler();
 *
 * 方法：
 *   ResultPartitionID getPartitionId()                         (:54)
 *   BufferProvider getBufferProvider()                         (:58)
 *   int getNumberOfOutputChannels()  -> partition.getNumberOfSubpartitions()  (:62)
 *   void writeBuffer(Buffer, int targetChannel)
 *        -> partition.add(buffer, targetChannel)                (:70)
 *   void writeEvent(AbstractEvent, int targetChannel)
 *        -> partition.add(EventSerializer.toBuffer(event), targetChannel)  (:74)
 *   void writeEventToAllChannels(AbstractEvent)                 (:78)
 *   void writeEndOfSuperstep()                                  (:85)
 *   void subscribeToEvent(EventListener<TaskEvent>, Class<? extends TaskEvent>) (:96)
 *   void onEvent(TaskEvent) -> taskEventHandler.publish(event)  (:100)
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 没有 ResultPartition / BufferProvider / netty】
 * 1.0 的下行链路是
 *   ResultPartitionWriter -> ResultPartition.add(buffer, subpartitionIndex)
 *   -> ResultSubpartition -> LocalInputChannel 或 netty RemoteInputChannel
 *   -> 下游 SingleInputGate
 * 本实现已经把「ResultPartition + 各 InputChannel + 缓冲池」压进了下游的
 * SingleInputGate（见 SingleInputGate.h 的偏差），所以这里的「第 i 个
 * subpartition」直接就是一个 Target：下游的哪个 gate、以及本生产者在那个
 * gate 上占的通道号。writeBuffer/writeEvent 变成往该 gate 的有界队列 put()，
 * 队列满时阻塞 —— 等价于 1.0 的 requestBufferBlocking 背压。
 * 因此 getBufferProvider() / getPartitionId() 不复刻。
 *
 * 【偏差 2 —— TaskEvent 不复刻】subscribeToEvent / onEvent / TaskEventHandler
 * 服务于迭代作业（BlockingBackChannel 等）和 netty 的反向事件通道，
 * 本实现没有迭代支持，也没有网络栈，整块省掉。writeEndOfSuperstep() 同理
 * （EndOfSuperstepEvent 只用于 iteration 的 superstep 同步）。
 *
 * 【偏差 3 —— channelIndex 的来源】1.0 下游 BufferOrEvent 的 channelIndex 是
 * 由接收端的 SingleInputGate 按 InputChannel 填的；本实现没有 InputChannel
 * 对象，所以由生产端在 Target 里带上，put() 前写进 BufferOrEvent。
 * 这个号必须与下游 BarrierBuffer / StreamInputProcessor 开数组时用的扁平
 * 通道空间一致（见 UnionInputGate.h）。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_API_WRITER_RESULTPARTITIONWRITER_H
#define FLINK_RUNTIME_IO_NETWORK_API_WRITER_RESULTPARTITIONWRITER_H

#include <memory>
#include <vector>

#include "flink/runtime/event/AbstractEvent.h"
#include "flink/runtime/io/network/partition/consumer/SingleInputGate.h"
#include "flink/streaming/runtime/streamrecord/StreamElement.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace api {
namespace writer {

class ResultPartitionWriter {
 public:
  typedef flink::runtime::io::network::partition::consumer::SingleInputGate
      SingleInputGate;
  typedef std::shared_ptr<const flink::streaming::runtime::streamrecord::
                              StreamElement> BufferPtr;
  typedef std::shared_ptr<const flink::runtime::event::AbstractEvent> EventPtr;

  // 【偏差 1】1.0 是 ResultPartitionWriter(ResultPartition)；这里一个
  // subpartition = 「下游的某个 gate + 本生产者在该 gate 上的通道号」
  struct Target {
    SingleInputGate* gate;
    int channelIndex;

    Target() : gate(NULL), channelIndex(0) {}
    Target(SingleInputGate* g, int c) : gate(g), channelIndex(c) {}
  };

  explicit ResultPartitionWriter(const std::vector<Target>& subpartitions)
      : subpartitions_(subpartitions) {}

  // 1.0 getNumberOfOutputChannels(): partition.getNumberOfSubpartitions()
  int getNumberOfOutputChannels() const {
    return static_cast<int>(subpartitions_.size());
  }

  // 1.0 writeBuffer(Buffer, int): partition.add(buffer, targetChannel)
  void writeBuffer(const BufferPtr& buffer, int targetChannel);

  // 1.0 writeEvent(AbstractEvent, int):
  //     partition.add(EventSerializer.toBuffer(event), targetChannel)
  void writeEvent(const EventPtr& event, int targetChannel);

  // 1.0 writeEventToAllChannels(AbstractEvent)
  void writeEventToAllChannels(const EventPtr& event);

 private:
  std::vector<Target> subpartitions_;
};

}  // namespace writer
}  // namespace api
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_API_WRITER_RESULTPARTITIONWRITER_H
