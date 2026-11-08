#include "flink/streaming/runtime/io/StreamTwoInputProcessor.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <typeinfo>

#include "flink/runtime/io/network/api/EndOfPartitionEvent.h"
#include "flink/streaming/runtime/io/BarrierBuffer.h"
#include "flink/streaming/runtime/io/BarrierTracker.h"
#include "flink/streaming/runtime/io/InputGateUtil.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

namespace {
typedef flink::runtime::io::network::api::EndOfPartitionEvent
    EndOfPartitionEvent;
typedef flink::runtime::io::network::partition::consumer::BufferOrEvent
    BufferOrEvent;
typedef flink::streaming::runtime::streamrecord::StreamRecord StreamRecord;

const int64_t LONG_MIN_VALUE = std::numeric_limits<int64_t>::min();
const int64_t LONG_MAX_VALUE = std::numeric_limits<int64_t>::max();
}  // namespace

// 1.0 的构造函数（StreamTwoInputProcessor.java:91）
StreamTwoInputProcessor::StreamTwoInputProcessor(
    const std::vector<InputGate*>& inputGates1,
    const std::vector<InputGate*>& inputGates2,
    CheckpointEventListener* checkpointListener,
    flink::streaming::api::CheckpointingMode checkpointMode)
    : currentChannel_(-1), isFinished_(false), numInputChannels1_(0) {
  // 1.0:101 两组 gate 拼成一个扁平通道空间
  inputGate_ = InputGateUtil::createInputGate(inputGates1, inputGates2);

  if (checkpointMode == flink::streaming::api::CheckpointingMode::EXACTLY_ONCE) {
    barrierHandler_.reset(new BarrierBuffer(inputGate_.get()));
  } else if (checkpointMode ==
             flink::streaming::api::CheckpointingMode::AT_LEAST_ONCE) {
    barrierHandler_.reset(new BarrierTracker(inputGate_.get()));
  } else {
    throw std::invalid_argument("Unrecognized CheckpointingMode");
  }

  if (checkpointListener != NULL) {
    barrierHandler_->registerCheckpointEventHandler(checkpointListener);
  }

  // 1.0:144 "determine which unioned channels belong to input 1 and which
  //          belong to input 2"
  for (InputGate* gate : inputGates1) {
    numInputChannels1_ += gate->getNumberOfInputChannels();
  }
  const int numInputChannels2 =
      inputGate_->getNumberOfInputChannels() - numInputChannels1_;

  // 1.0:153-159
  watermarks1_.assign(numInputChannels1_ > 0 ? numInputChannels1_ : 0,
                      LONG_MIN_VALUE);
  lastEmittedWatermark1_ = LONG_MIN_VALUE;

  watermarks2_.assign(numInputChannels2 > 0 ? numInputChannels2 : 0,
                      LONG_MIN_VALUE);
  lastEmittedWatermark2_ = LONG_MIN_VALUE;
}

// 1.0 processInput(TwoInputStreamOperator<IN1,IN2,?>, Object lock)（:163）
bool StreamTwoInputProcessor::processInput(
    TwoInputStreamOperator* streamOperator, std::recursive_mutex& lock) {
  if (isFinished_) {
    return false;
  }

  while (true) {
    // 【偏差 1】1.0 这里先按 currentChannel 选 deserializationDelegate1/2 走
    // 反序列化状态机；本实现一个 Buffer 就是一个 StreamElement，直接拿整条
    BufferOrEvent bufferOrEvent;
    if (barrierHandler_->getNextNonBlocked(bufferOrEvent)) {
      if (bufferOrEvent.isBuffer()) {
        currentChannel_ = bufferOrEvent.getChannelIndex();
        const auto& element = bufferOrEvent.getBuffer();

        if (element->isWatermark()) {
          handleWatermark(streamOperator,
                          static_cast<const Watermark&>(*element),
                          currentChannel_, lock);
          continue;
        }

        const StreamRecord& record = static_cast<const StreamRecord&>(*element);
        std::lock_guard<std::recursive_mutex> guard(lock);
        // 1.0: currentChannel < numInputChannels1 ? input 1 : input 2
        if (currentChannel_ < numInputChannels1_) {
          streamOperator->setKeyContextElement1(record);
          streamOperator->processElement1(record);
        } else {
          streamOperator->setKeyContextElement2(record);
          streamOperator->processElement2(record);
        }
        return true;
      } else {
        // 1.0: "Event received"
        if (typeid(*bufferOrEvent.getEvent()) != typeid(EndOfPartitionEvent)) {
          throw std::runtime_error("Unexpected event");
        }
      }
    } else {
      isFinished_ = true;
      if (!barrierHandler_->isEmpty()) {
        throw std::logic_error("Trailing data in checkpoint barrier handler.");
      }
      return false;
    }
  }
}

// 1.0 private handleWatermark(...)（:241）
void StreamTwoInputProcessor::handleWatermark(
    TwoInputStreamOperator* streamOperator, const Watermark& mark,
    int channelIndex, std::recursive_mutex& lock) {
  if (channelIndex < numInputChannels1_) {
    const int64_t watermarkMillis = mark.getTimestamp();
    // 1.0: 只有该通道水位线变大才需要重算
    if (watermarkMillis > watermarks1_[channelIndex]) {
      watermarks1_[channelIndex] = watermarkMillis;
      int64_t newMinWatermark = LONG_MAX_VALUE;
      for (int64_t watermark : watermarks1_) {
        newMinWatermark = std::min(watermark, newMinWatermark);
      }
      // 1.0: input 1 全通道最小值超过已下发值，才真正下发（保证单调不减）
      if (newMinWatermark > lastEmittedWatermark1_) {
        lastEmittedWatermark1_ = newMinWatermark;
        std::lock_guard<std::recursive_mutex> guard(lock);
        streamOperator->processWatermark1(Watermark(lastEmittedWatermark1_));
      }
    }
  } else {
    // 1.0: channelIndex = channelIndex - numInputChannels1
    channelIndex = channelIndex - numInputChannels1_;
    const int64_t watermarkMillis = mark.getTimestamp();
    if (watermarkMillis > watermarks2_[channelIndex]) {
      watermarks2_[channelIndex] = watermarkMillis;
      int64_t newMinWatermark = LONG_MAX_VALUE;
      for (int64_t watermark : watermarks2_) {
        newMinWatermark = std::min(watermark, newMinWatermark);
      }
      if (newMinWatermark > lastEmittedWatermark2_) {
        lastEmittedWatermark2_ = newMinWatermark;
        std::lock_guard<std::recursive_mutex> guard(lock);
        streamOperator->processWatermark2(Watermark(lastEmittedWatermark2_));
      }
    }
  }
}

// 1.0 cleanup()（:282）
void StreamTwoInputProcessor::cleanup() {
  // 【偏差 4】1.0 先回收未读完的 Buffer，再 cleanup barrier handler
  barrierHandler_->cleanup();
}

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
