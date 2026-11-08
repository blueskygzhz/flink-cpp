/*
 * flink-runtime :
 *   org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate
 *
 * 1.0 UnionInputGate implements InputGate（UnionInputGate.java:48）：把多个
 * InputGate 合成一个，并**重新编号通道**——这是关键点：
 *   「The union input gate maps input channels in a flat index space」
 * 第 0 个 gate 的通道占 [0, n0)，第 1 个占 [n0, n0+n1)，以此类推；
 * 取出的 BufferOrEvent 的 channelIndex 会被加上该 gate 的偏移
 * （1.0 UnionInputGate.java:148 的
 *  `bufferOrEvent.setChannelIndex(channelIndexOffset + bufferOrEvent.getChannelIndex())`）。
 * BarrierBuffer / StreamInputProcessor 都按这个扁平空间开数组，所以偏移必须对。
 *
 * 1.0 用 inputGatesWithData 这个 LinkedBlockingQueue + registerListener 做
 * 「哪个 gate 有数据就取哪个」的事件驱动调度；本实现的 SingleInputGate 是
 * 阻塞队列，没有 listener 机制，所以这里退化成**轮询**：依次问每个未结束的
 * gate，直到全部结束。
 *
 * 【偏差】轮询会在某个 gate 空、另一个 gate 有数据时阻塞在前者上。1.0 不会。
 * 对本实现（进程内、gate 背后就是阻塞队列）影响只是公平性，不影响正确性：
 * 任何一个 gate 迟早会有数据或结束。两输入算子（TwoInputStreamOperator）
 * 落地时若需要真正的「谁有数据取谁」，再补 listener 机制。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_UNIONINPUTGATE_H
#define FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_UNIONINPUTGATE_H

#include <vector>

#include "flink/runtime/io/network/partition/consumer/InputGate.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace partition {
namespace consumer {

class UnionInputGate : public InputGate {
 public:
  explicit UnionInputGate(const std::vector<InputGate*>& inputGates)
      : inputGates_(inputGates),
        totalNumberOfInputChannels_(0),
        nextGate_(0) {
    channelIndexOffsets_.reserve(inputGates_.size());
    for (InputGate* gate : inputGates_) {
      channelIndexOffsets_.push_back(totalNumberOfInputChannels_);
      totalNumberOfInputChannels_ += gate->getNumberOfInputChannels();
    }
  }

  int getNumberOfInputChannels() const override {
    return totalNumberOfInputChannels_;
  }

  // 1.0 isFinished(): 所有 gate 都结束才算结束
  bool isFinished() const override {
    for (InputGate* gate : inputGates_) {
      if (!gate->isFinished()) return false;
    }
    return true;
  }

  void requestPartitions() override {
    for (InputGate* gate : inputGates_) {
      gate->requestPartitions();
    }
  }

  bool getNextBufferOrEvent(BufferOrEvent& target) override {
    const size_t numGates = inputGates_.size();
    for (size_t attempt = 0; attempt < numGates; ++attempt) {
      const size_t index = (nextGate_ + attempt) % numGates;
      if (inputGates_[index]->isFinished()) continue;

      if (inputGates_[index]->getNextBufferOrEvent(target)) {
        // 1.0 UnionInputGate.java:148：映射到扁平通道空间
        target.setChannelIndex(channelIndexOffsets_[index] +
                               target.getChannelIndex());
        nextGate_ = (index + 1) % numGates;  // 轮转，避免总是先问同一个
        return true;
      }
    }
    return false;
  }

 private:
  std::vector<InputGate*> inputGates_;
  std::vector<int> channelIndexOffsets_;
  int totalNumberOfInputChannels_;
  size_t nextGate_;
};

}  // namespace consumer
}  // namespace partition
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_UNIONINPUTGATE_H
