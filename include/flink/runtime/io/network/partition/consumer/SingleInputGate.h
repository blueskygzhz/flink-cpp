/*
 * flink-runtime : org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate
 *
 * 【偏差 —— 本实现用进程内有界队列替代整个网络栈】
 * 1.0 的链路是：
 *   RecordWriter -> ResultPartition -> (LocalInputChannel / netty RemoteInputChannel)
 *                -> SingleInputGate.getNextBufferOrEvent()
 * 其中背压来自 LocalBufferPool：缓冲池耗尽时 requestBufferBlocking() 阻塞上游。
 *
 * 本实现把「ResultPartition + 各 InputChannel + 缓冲池」压成 SingleInputGate 内部
 * 的一个有界阻塞队列：
 *   - queue 容量 = 缓冲池大小，队列满时 put() 阻塞 => 与 1.0 等价的背压语义；
 *   - 每个 BufferOrEvent 自带 channelIndex，故一个队列即可服务多个逻辑输入通道；
 *   - numberOfInputChannels 仍如实记录，BarrierBuffer 的对齐逻辑完全照 1.0 跑。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_SINGLEINPUTGATE_H
#define FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_SINGLEINPUTGATE_H

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>

#include "flink/runtime/io/network/partition/consumer/InputGate.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace partition {
namespace consumer {

class SingleInputGate : public InputGate {
 public:
  SingleInputGate(int numberOfInputChannels, size_t bufferPoolSize);

  int getNumberOfInputChannels() const override;
  bool isFinished() const override;
  void requestPartitions() override;
  bool getNextBufferOrEvent(BufferOrEvent& target) override;

  // 生产侧（对应 ResultPartition.add -> LocalInputChannel）：
  // 队列满时阻塞，等价于 1.0 LocalBufferPool.requestBufferBlocking 的背压。
  void put(const BufferOrEvent& bufferOrEvent);

  // 作业失败时唤醒所有等待者，避免线程挂死（1.0 由 TaskManager cancel 链路负责）
  void releaseAllResources();

  void setNumberOfInputChannels(int numberOfInputChannels);

 private:
  mutable std::mutex mutex_;
  std::condition_variable producerCondition_;
  std::condition_variable consumerCondition_;
  std::deque<BufferOrEvent> queue_;
  int numberOfInputChannels_;
  size_t bufferPoolSize_;
  bool released_;
};

}  // namespace consumer
}  // namespace partition
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_PARTITION_CONSUMER_SINGLEINPUTGATE_H
