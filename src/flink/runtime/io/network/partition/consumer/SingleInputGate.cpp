#include "flink/runtime/io/network/partition/consumer/SingleInputGate.h"

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace partition {
namespace consumer {

SingleInputGate::SingleInputGate(int numberOfInputChannels, size_t bufferPoolSize)
    : numberOfInputChannels_(numberOfInputChannels > 0 ? numberOfInputChannels
                                                       : 1),
      bufferPoolSize_(bufferPoolSize > 0 ? bufferPoolSize : 1),
      released_(false) {}

int SingleInputGate::getNumberOfInputChannels() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return numberOfInputChannels_;
}

void SingleInputGate::setNumberOfInputChannels(int numberOfInputChannels) {
  std::lock_guard<std::mutex> lock(mutex_);
  numberOfInputChannels_ =
      numberOfInputChannels > 0 ? numberOfInputChannels : 1;
}

// 1.0 的 isFinished() 表示所有输入通道都已结束。本实现里 gate 的结束由
// EndOfPartitionEvent 的计数在 StreamInputProcessor / BarrierBuffer 侧判定，
// 这里只报告「已释放且队列排空」。
bool SingleInputGate::isFinished() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return released_ && queue_.empty();
}

// 1.0 SingleInputGate.requestPartitions() 向上游 ResultPartition 发起订阅。
// 进程内实现没有订阅动作（上游直接 put），留空以保持调用点与 1.0 一致。
void SingleInputGate::requestPartitions() {}

bool SingleInputGate::getNextBufferOrEvent(BufferOrEvent& target) {
  std::unique_lock<std::mutex> lock(mutex_);
  consumerCondition_.wait(lock,
                          [this] { return !queue_.empty() || released_; });
  if (queue_.empty()) {
    return false;  // 1.0 返回 null：gate 已结束
  }
  target = queue_.front();
  queue_.pop_front();
  producerCondition_.notify_one();
  return true;
}

// 队列满时阻塞 —— 等价于 1.0 LocalBufferPool.requestBufferBlocking 的背压：
// 下游消费不过来时，上游 RecordWriter 的写入被挂住。
void SingleInputGate::put(const BufferOrEvent& bufferOrEvent) {
  std::unique_lock<std::mutex> lock(mutex_);
  producerCondition_.wait(
      lock, [this] { return queue_.size() < bufferPoolSize_ || released_; });
  if (released_) {
    return;  // 已释放：丢弃，避免生产者永久阻塞
  }
  queue_.push_back(bufferOrEvent);
  consumerCondition_.notify_one();
}

void SingleInputGate::releaseAllResources() {
  std::lock_guard<std::mutex> lock(mutex_);
  released_ = true;
  consumerCondition_.notify_all();
  producerCondition_.notify_all();
}

}  // namespace consumer
}  // namespace partition
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
