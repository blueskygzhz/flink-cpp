/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner
 *
 * 1.0 ShufflePartitioner.java：
 *   private Random random = new Random();
 *   private int[] returnArray = new int[1];
 *   int[] selectChannels(record, n) {
 *       returnArray[0] = random.nextInt(n);
 *       return returnArray;
 *   }
 *   StreamPartitioner<T> copy() { return new ShufflePartitioner<T>(); }
 *   String toString() { return "SHUFFLE"; }
 *
 * 1.0 原注释：「Partitioner that distributes the data equally by selecting one
 * output channel randomly.」
 *
 * 注意它是八个实现里**唯一** copy() 返回新实例的——因为持有 Random，
 * 两条边共享同一个 Random 会让随机序列相互纠缠。这点原样保留。
 *
 * 【偏差】1.0 用 java.util.Random（线性同余 + nextInt(bound) 的拒绝采样）。
 * 这里用 std::mt19937 + uniform_int_distribution：随机数列与 1.0 不同，
 * 但分布语义（在 [0, n) 上均匀）一致。shuffle 本身不要求可重现序列，
 * 1.0 也没有给 Random 设种子，故不构成行为差异。
 */
#ifndef FLINK_STREAMING_RUNTIME_PARTITIONER_SHUFFLEPARTITIONER_H
#define FLINK_STREAMING_RUNTIME_PARTITIONER_SHUFFLEPARTITIONER_H

#include <random>

#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace partitioner {

class ShufflePartitioner : public StreamPartitioner {
 public:
  // 1.0: random = new Random(); returnArray = new int[1];
  ShufflePartitioner() : random_(std::random_device()()), returnArray_(1, 0) {}

  const std::vector<int>& selectChannels(
      const RecordPtr& /* record */, int numberOfOutputChannels) override {
    // 1.0: returnArray[0] = random.nextInt(numberOfOutputChannels)
    std::uniform_int_distribution<int> dist(0, numberOfOutputChannels - 1);
    returnArray_[0] = dist(random_);
    return returnArray_;
  }

  // 1.0: return new ShufflePartitioner<T>()  —— 唯一不共享实例的实现
  std::shared_ptr<StreamPartitioner> copy() override {
    return std::make_shared<ShufflePartitioner>();
  }

  std::string toString() const override { return "SHUFFLE"; }

 private:
  std::mt19937 random_;
  std::vector<int> returnArray_;
};

}  // namespace partitioner
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_PARTITIONER_SHUFFLEPARTITIONER_H
