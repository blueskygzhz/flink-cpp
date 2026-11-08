/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner
 *
 * 1.0 ForwardPartitioner.java：
 *   private int[] returnArray = new int[] {0};
 *   int[] selectChannels(...) { return returnArray; }   // 恒为通道 0
 *   StreamPartitioner<T> copy() { return this; }
 *   String toString() { return "FORWARD"; }
 *
 * 1.0 原注释：「Partitioner that forwards elements only to the locally running
 * downstream operation.」
 *
 * 它恒返回 {0} 之所以正确，是因为 JobGraph 侧对 FORWARD 边用
 * DistributionPattern.POINTWISE 且要求上下游并行度相同（见
 * StreamingJobGraphGenerator / StreamGraph.addEdge 对 ForwardPartitioner 的
 * 校验），于是每个上游 subtask 的输出只有一个 subpartition。
 */
#ifndef FLINK_STREAMING_RUNTIME_PARTITIONER_FORWARDPARTITIONER_H
#define FLINK_STREAMING_RUNTIME_PARTITIONER_FORWARDPARTITIONER_H

#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace partitioner {

class ForwardPartitioner : public StreamPartitioner {
 public:
  // 1.0: returnArray = new int[] {0}
  ForwardPartitioner() : returnArray_(1, 0) {}

  const std::vector<int>& selectChannels(
      const RecordPtr& /* record */,
      int /* numberOfOutputChannels */) override {
    return returnArray_;
  }

  // 1.0: return this
  std::shared_ptr<StreamPartitioner> copy() override {
    return shared_from_this();
  }

  std::string toString() const override { return "FORWARD"; }

 private:
  std::vector<int> returnArray_;
};

}  // namespace partitioner
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_PARTITIONER_FORWARDPARTITIONER_H
