/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner
 *
 * 1.0 RebalancePartitioner.java：
 *   private int[] returnArray = new int[] {-1};          // 注意初值 -1
 *   int[] selectChannels(record, n) {
 *       returnArray[0] = (returnArray[0] + 1) % n;
 *       return returnArray;
 *   }
 *   StreamPartitioner<T> copy() { return this; }
 *   String toString() { return "REBALANCE"; }
 *
 * 1.0 原注释：「Partitioner that distributes the data equally by cycling
 * through the output channels.」
 *
 * 初值 -1 使**第一条记录去通道 0**——与 runtime 的
 * RoundRobinChannelSelector（初值 0，第一条去通道 1）刚好差一位，
 * 这点差异照原样保留。
 */
#ifndef FLINK_STREAMING_RUNTIME_PARTITIONER_REBALANCEPARTITIONER_H
#define FLINK_STREAMING_RUNTIME_PARTITIONER_REBALANCEPARTITIONER_H

#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace partitioner {

class RebalancePartitioner : public StreamPartitioner {
 public:
  // 1.0: returnArray = new int[] {-1}
  RebalancePartitioner() : returnArray_(1, -1) {}

  const std::vector<int>& selectChannels(
      const RecordPtr& /* record */, int numberOfOutputChannels) override {
    returnArray_[0] = (returnArray_[0] + 1) % numberOfOutputChannels;
    return returnArray_;
  }

  // 1.0: return this
  std::shared_ptr<StreamPartitioner> copy() override {
    return shared_from_this();
  }

  std::string toString() const override { return "REBALANCE"; }

 private:
  std::vector<int> returnArray_;
};

}  // namespace partitioner
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_PARTITIONER_REBALANCEPARTITIONER_H
