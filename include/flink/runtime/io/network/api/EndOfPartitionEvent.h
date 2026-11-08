/*
 * flink-runtime : org.apache.flink.runtime.io.network.api.EndOfPartitionEvent
 *
 * 1.0 EndOfPartitionEvent.java: 单例事件（INSTANCE），上游 subtask 产出结束时
 * 发往每个下游通道。下游据此知道「这个通道不会再来数据了」：
 *   - StreamInputProcessor 用它统计关闭的通道数，全关则退出主循环；
 *   - BarrierBuffer.processEndOfPartition() 把它计入 numClosedChannels，
 *     避免永远等一个已经结束的通道的 barrier。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_API_ENDOFPARTITIONEVENT_H
#define FLINK_RUNTIME_IO_NETWORK_API_ENDOFPARTITIONEVENT_H

#include "flink/runtime/event/AbstractEvent.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace api {

class EndOfPartitionEvent : public flink::runtime::event::AbstractEvent {
 public:
  EndOfPartitionEvent() {}
};

}  // namespace api
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_API_ENDOFPARTITIONEVENT_H
