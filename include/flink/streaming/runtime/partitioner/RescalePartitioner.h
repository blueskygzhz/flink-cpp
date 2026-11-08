/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.partitioner.RescalePartitioner
 *
 * 1.0 RescalePartitioner.java 的 selectChannels 与 RebalancePartitioner
 * **逐字相同**（returnArray 初值 -1 的轮转），区别完全不在本类里：
 *
 * 1.0 原注释：「This distributes only to a subset of downstream nodes because
 * StreamingJobGraphGenerator instantiates a DistributionPattern.POINTWISE
 * distribution pattern when encountering SemiRebalancePartitioner.」
 * 即 REBALANCE 走 ALL_TO_ALL、RESCALE 走 POINTWISE，于是本 subtask 的
 * numberOfOutputChannels 只覆盖下游的一个子集，轮转范围自然变小。
 *
 * 「上游并行度 2 / 下游 4 时，每个上游各向两个下游分发；反之下游 2 / 上游 4 时，
 * 每两个上游分发给同一个下游」—— 这条语义由 JobGraph 的连接模式决定，
 * 到 StreamingJobGraphGenerator 落地时再对齐。
 */
#ifndef FLINK_STREAMING_RUNTIME_PARTITIONER_RESCALEPARTITIONER_H
#define FLINK_STREAMING_RUNTIME_PARTITIONER_RESCALEPARTITIONER_H

#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace partitioner {

class RescalePartitioner : public StreamPartitioner {
 public:
  // 1.0: returnArray = new int[] {-1}
  RescalePartitioner() : returnArray_(1, -1) {}

  const std::vector<int>& selectChannels(
      const RecordPtr& /* record */, int numberOfOutputChannels) override {
    returnArray_[0] = (returnArray_[0] + 1) % numberOfOutputChannels;
    return returnArray_;
  }

  // 1.0: return this
  std::shared_ptr<StreamPartitioner> copy() override {
    return shared_from_this();
  }

  std::string toString() const override { return "RESCALE"; }

 private:
  std::vector<int> returnArray_;
};

}  // namespace partitioner
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_PARTITIONER_RESCALEPARTITIONER_H
