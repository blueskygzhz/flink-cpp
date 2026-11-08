/*
 * flink-runtime : org.apache.flink.runtime.io.network.api.writer.ChannelSelector
 *
 * 1.0 ChannelSelector.java 全文（除注释）：
 *   public interface ChannelSelector<T extends IOReadableWritable> {
 *       int[] selectChannels(T record, int numChannels);
 *   }
 *
 * 1.0 原注释：「The ChannelSelector determines to which logical channels a
 * record should be written to.」返回值是「a (possibly empty) array of integer
 * numbers which indicate the indices of the output channels through which the
 * record shall be forwarded」—— 可以为空（丢弃），也可以多于一个（广播）。
 *
 * 实现方：RoundRobinChannelSelector（runtime 默认）与 streaming 的
 * StreamPartitioner 家族（Forward / Rebalance / Hash / Broadcast / Global /
 * Shuffle / Rescale / CustomPartitionerWrapper）。
 *
 * 【偏差 1 —— 元素类型】1.0 的 T 在流作业里恒为
 * SerializationDelegate<StreamRecord<T>>（见 StreamPartitioner.java:26），
 * 即「即将被序列化的那条记录」。本实现不做序列化，通道里流动的就是
 * StreamElement 的 shared_ptr（见 BufferOrEvent.h），所以这里把 T 固定成
 * RecordPtr，不再是模板参数。各 partitioner 取值时写
 * `record->as<StreamRecord>().getValue()`，对应 1.0 的
 * `record.getInstance().getValue()`。
 *
 * 【偏差 2 —— 返回值】1.0 返回 int[]，而且各实现都返回自己的一个**复用字段**
 * （见 RoundRobinChannelSelector 的 nextChannelToSendTo、HashPartitioner 的
 * returnArray）——即调用方不得持有该数组。这里返回 const std::vector<int>&
 * 指向实现内部的同一个 vector，把「复用」这条约定在类型上表达出来。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_API_WRITER_CHANNELSELECTOR_H
#define FLINK_RUNTIME_IO_NETWORK_API_WRITER_CHANNELSELECTOR_H

#include <memory>
#include <vector>

#include "flink/streaming/runtime/streamrecord/StreamElement.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace api {
namespace writer {

class ChannelSelector {
 public:
  typedef std::shared_ptr<const flink::streaming::runtime::streamrecord::
                              StreamElement> RecordPtr;

  virtual ~ChannelSelector() {}

  // 1.0 int[] selectChannels(T record, int numChannels)
  virtual const std::vector<int>& selectChannels(const RecordPtr& record,
                                                 int numChannels) = 0;
};

}  // namespace writer
}  // namespace api
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_API_WRITER_CHANNELSELECTOR_H
