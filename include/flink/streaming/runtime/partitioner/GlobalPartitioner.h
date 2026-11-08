/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner
 *
 * 1.0 GlobalPartitioner.java：
 *   private int[] returnArray = new int[] { 0 };
 *   int[] selectChannels(...) { return returnArray; }
 *   StreamPartitioner<T> copy() { return this; }
 *   String toString() { return "GLOBAL"; }
 *
 * 1.0 原注释：「Partitioner that sends all elements to the downstream operator
 * with subtask ID=0」。
 *
 * 与 ForwardPartitioner 的 {0} 看着一样，含义不同：FORWARD 的 0 是
 * 「本地那唯一一个 subpartition」（POINTWISE + 同并行度），
 * GLOBAL 的 0 是「全局第 0 个 subtask」（ALL_TO_ALL，下游可以有很多 subtask，
 * 但只有 0 号收到数据），因此 GLOBAL 下游实际只有一个 subtask 在干活。
 */
#ifndef FLINK_STREAMING_RUNTIME_PARTITIONER_GLOBALPARTITIONER_H
#define FLINK_STREAMING_RUNTIME_PARTITIONER_GLOBALPARTITIONER_H

#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace partitioner {

class GlobalPartitioner : public StreamPartitioner {
 public:
  // 1.0: returnArray = new int[] { 0 }
  GlobalPartitioner() : returnArray_(1, 0) {}

  const std::vector<int>& selectChannels(
      const RecordPtr& /* record */,
      int /* numberOfOutputChannels */) override {
    return returnArray_;
  }

  // 1.0: return this
  std::shared_ptr<StreamPartitioner> copy() override {
    return shared_from_this();
  }

  std::string toString() const override { return "GLOBAL"; }

 private:
  std::vector<int> returnArray_;
};

}  // namespace partitioner
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_PARTITIONER_GLOBALPARTITIONER_H
