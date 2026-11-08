#include "flink/streaming/runtime/io/StreamInputProcessor.h"

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
typedef flink::streaming::api::watermark::Watermark Watermark;

const int64_t LONG_MIN_VALUE = std::numeric_limits<int64_t>::min();
const int64_t LONG_MAX_VALUE = std::numeric_limits<int64_t>::max();
}  // namespace

// 1.0 的构造函数（StreamInputProcessor.java:85）
StreamInputProcessor::StreamInputProcessor(
    const std::vector<InputGate*>& inputGates,
    CheckpointEventListener* checkpointListener,
    flink::streaming::api::CheckpointingMode checkpointMode)
    : currentChannel_(-1), isFinished_(false) {
  inputGate_ = InputGateUtil::createInputGate(inputGates);

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

  // 1.0: watermarks[i] = Long.MIN_VALUE; lastEmittedWatermark = Long.MIN_VALUE;
  watermarks_.assign(inputGate_->getNumberOfInputChannels(), LONG_MIN_VALUE);
  lastEmittedWatermark_ = LONG_MIN_VALUE;
}

// 1.0 processInput(OneInputStreamOperator<IN,?>, Object lock)（:130）
bool StreamInputProcessor::processInput(OneInputStreamOperator* streamOperator,
                                       std::mutex& lock) {
  if (isFinished_) {
    return false;
  }

  while (true) {
    // 【偏差 1】1.0 这里先走反序列化状态机（currentRecordDeserializer），
    // 本实现一个 Buffer 就是一个 StreamElement，直接拿到的就是整条记录
    BufferOrEvent bufferOrEvent;
    if (barrierHandler_->getNextNonBlocked(bufferOrEvent)) {
      if (bufferOrEvent.isBuffer()) {
        currentChannel_ = bufferOrEvent.getChannelIndex();
        const auto& element = bufferOrEvent.getBuffer();

        if (element->isWatermark()) {
          const int64_t watermarkMillis =
              static_cast<const Watermark&>(*element).getTimestamp();
          // 1.0: 只有该通道水位线变大才需要重算
          if (watermarkMillis > watermarks_[currentChannel_]) {
            watermarks_[currentChannel_] = watermarkMillis;
            int64_t newMinWatermark = LONG_MAX_VALUE;
            for (int64_t watermark : watermarks_) {
              newMinWatermark = std::min(watermark, newMinWatermark);
            }
            // 1.0: 全通道最小值超过已下发值，才真正下发（保证单调不减）
            if (newMinWatermark > lastEmittedWatermark_) {
              lastEmittedWatermark_ = newMinWatermark;
              std::lock_guard<std::mutex> guard(lock);
              streamOperator->processWatermark(Watermark(lastEmittedWatermark_));
            }
          }
          continue;
        } else {
          // 1.0: "now we can do the actual processing"
          const StreamRecord& record =
              static_cast<const StreamRecord&>(*element);
          std::lock_guard<std::mutex> guard(lock);
          streamOperator->setKeyContextElement1(record);
          streamOperator->processElement(record);
          return true;
        }
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

// 1.0 cleanup()（:215）
void StreamInputProcessor::cleanup() {
  // 【偏差 3】1.0 先回收未读完的 Buffer，再 cleanup barrier handler
  barrierHandler_->cleanup();
}

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
