/*
 * flink-runtime :
 *   org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector
 *
 * 1.0 RoundRobinChannelSelector.java:
 *   private final int[] nextChannelToSendTo = new int[1];   // 长度恒为 1
 *   RoundRobinChannelSelector() { nextChannelToSendTo[0] = 0; }
 *   int[] selectChannels(T record, int numberOfOutputChannels) {
 *       nextChannelToSendTo[0] =
 *           (nextChannelToSendTo[0] + 1) % numberOfOutputChannels;
 *       return nextChannelToSendTo;
 *   }
 *
 * 1.0 原注释：「regardless of the record every attached exactly one output
 * channel is selected at a time」。
 *
 * 注意 1.0 的一个细节并照搬：**先自增再取模**，所以第一条记录去的是通道 1
 * （numChannels == 1 时是通道 0），不是通道 0。RecordWriter 的默认选择器就是
 * 它（RecordWriter.java:57），改掉会与 1.0 的分发顺序不一致。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_API_WRITER_ROUNDROBINCHANNELSELECTOR_H
#define FLINK_RUNTIME_IO_NETWORK_API_WRITER_ROUNDROBINCHANNELSELECTOR_H

#include "flink/runtime/io/network/api/writer/ChannelSelector.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace api {
namespace writer {

class RoundRobinChannelSelector : public ChannelSelector {
 public:
  // 1.0: nextChannelToSendTo = new int[1]; nextChannelToSendTo[0] = 0;
  RoundRobinChannelSelector() : nextChannelToSendTo_(1, 0) {}

  const std::vector<int>& selectChannels(const RecordPtr& /* record */,
                                         int numberOfOutputChannels) override {
    nextChannelToSendTo_[0] =
        (nextChannelToSendTo_[0] + 1) % numberOfOutputChannels;
    return nextChannelToSendTo_;
  }

 private:
  std::vector<int> nextChannelToSendTo_;
};

}  // namespace writer
}  // namespace api
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_API_WRITER_ROUNDROBINCHANNELSELECTOR_H
