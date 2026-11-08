/*
 * flink-runtime : org.apache.flink.runtime.io.network.api.CheckpointBarrier
 *
 * 1.0 CheckpointBarrier.java: id + timestamp，extends RuntimeEvent。
 * 由 source task 注入数据流，随数据一起流到下游；下游 task 在 BarrierBuffer 里
 * 对齐所有输入通道的同 id barrier 后触发自己的快照（exactly-once 的核心机制）。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_API_CHECKPOINTBARRIER_H
#define FLINK_RUNTIME_IO_NETWORK_API_CHECKPOINTBARRIER_H

#include <cstdint>
#include <string>

#include "flink/runtime/event/AbstractEvent.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace api {

class CheckpointBarrier : public flink::runtime::event::AbstractEvent {
 public:
  CheckpointBarrier() : id_(0), timestamp_(0) {}
  CheckpointBarrier(int64_t id, int64_t timestamp)
      : id_(id), timestamp_(timestamp) {}

  int64_t getId() const { return id_; }
  int64_t getTimestamp() const { return timestamp_; }

  std::string toString() const {
    return "CheckpointBarrier " + std::to_string(id_) + " @ " +
           std::to_string(timestamp_);
  }

 private:
  int64_t id_;
  int64_t timestamp_;
};

}  // namespace api
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_API_CHECKPOINTBARRIER_H
