/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner
 *
 * 1.0 BroadcastPartitioner.java：
 *   int[] returnArray;
 *   boolean set;
 *   int setNumber;
 *   int[] selectChannels(record, n) {
 *       if (set && setNumber == n) {
 *           return returnArray;                 // 缓存命中
 *       } else {
 *           returnArray = new int[n];
 *           for (int i = 0; i < n; i++) returnArray[i] = i;
 *           set = true; setNumber = n;
 *           return returnArray;
 *       }
 *   }
 *   StreamPartitioner<T> copy() { return this; }
 *   String toString() { return "BROADCAST"; }
 *
 * 1.0 原注释：「Partitioner that selects all the output channels.」
 *
 * set / setNumber 这对字段是 1.0 的一个小优化：通道数在一个 task 生命周期内
 * 不变，所以 {0..n-1} 只需构造一次。这里把这段逻辑原样保留（而不是简化成
 * 「构造时就按 n 填好」），因为 selectChannels 的 n 是调用方传进来的，
 * 构造时还不知道。
 */
#ifndef FLINK_STREAMING_RUNTIME_PARTITIONER_BROADCASTPARTITIONER_H
#define FLINK_STREAMING_RUNTIME_PARTITIONER_BROADCASTPARTITIONER_H

#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace partitioner {

class BroadcastPartitioner : public StreamPartitioner {
 public:
  BroadcastPartitioner() : set_(false), setNumber_(0) {}

  const std::vector<int>& selectChannels(
      const RecordPtr& /* record */, int numberOfOutputChannels) override {
    if (set_ && setNumber_ == numberOfOutputChannels) {
      return returnArray_;
    }
    returnArray_.resize(numberOfOutputChannels);
    for (int i = 0; i < numberOfOutputChannels; ++i) {
      returnArray_[i] = i;
    }
    set_ = true;
    setNumber_ = numberOfOutputChannels;
    return returnArray_;
  }

  // 1.0: return this
  std::shared_ptr<StreamPartitioner> copy() override {
    return shared_from_this();
  }

  std::string toString() const override { return "BROADCAST"; }

 private:
  std::vector<int> returnArray_;
  bool set_;
  int setNumber_;
};

}  // namespace partitioner
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_PARTITIONER_BROADCASTPARTITIONER_H
