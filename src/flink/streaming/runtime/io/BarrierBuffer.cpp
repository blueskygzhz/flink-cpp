#include "flink/streaming/runtime/io/BarrierBuffer.h"

#include <stdexcept>
#include <typeinfo>

#include "flink/runtime/io/network/api/EndOfPartitionEvent.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

namespace {
typedef flink::runtime::io::network::api::CheckpointBarrier CheckpointBarrier;
typedef flink::runtime::io::network::api::EndOfPartitionEvent
    EndOfPartitionEvent;
}  // namespace

BarrierBuffer::BarrierBuffer(InputGate* inputGate)
    : inputGate_(inputGate),
      blockedChannels_(inputGate->getNumberOfInputChannels(), false),
      totalNumberOfInputChannels_(inputGate->getNumberOfInputChannels()),
      checkpointHandler_(NULL),
      // 1.0: currentCheckpointId = -1L
      currentCheckpointId_(-1),
      numBarriersReceived_(0),
      numClosedChannels_(0),
      endOfStream_(false) {}

// 1.0 getNextNonBlocked()（BarrierBuffer.java:103）
// 1.0 用递归表达「处理完缓存序列/解除阻塞后重新取一次」，这里用外层循环（见头文件偏差）
bool BarrierBuffer::getNextNonBlocked(BufferOrEvent& target) {
  while (true) {
    // 1.0: "process buffered BufferOrEvents before grabbing new ones"
    BufferOrEvent next;
    bool hasNext;
    if (!currentBuffered_) {
      hasNext = inputGate_->getNextBufferOrEvent(next);
    } else {
      hasNext = currentBuffered_->getNext(next);
      if (!hasNext) {
        completeBufferedSequence();
        continue;  // 1.0: return getNextNonBlocked();
      }
    }

    if (hasNext) {
      if (isBlocked(next.getChannelIndex())) {
        // 1.0: "if the channel is blocked we, we just store the BufferOrEvent"
        bufferSpiller_.add(next);
      } else if (next.isBuffer()) {
        target = next;
        return true;
      } else if (typeid(*next.getEvent()) == typeid(CheckpointBarrier)) {
        if (!endOfStream_) {
          // 1.0: "process barriers only if there is a chance of the
          // checkpoint completing"
          processBarrier(
              static_cast<const CheckpointBarrier&>(*next.getEvent()),
              next.getChannelIndex());
        }
      } else {
        if (typeid(*next.getEvent()) == typeid(EndOfPartitionEvent)) {
          numClosedChannels_++;
          // 1.0: "no chance to complete this checkpoint"
          releaseBlocks();
        }
        target = next;
        return true;
      }
    } else if (!endOfStream_) {
      // 1.0: "end of stream. we feed the data that is still buffered"
      endOfStream_ = true;
      releaseBlocks();
      continue;  // 1.0: return getNextNonBlocked();
    } else {
      return false;  // 1.0: return null;
    }
  }
}

// 1.0 completeBufferedSequence()（:153）
void BarrierBuffer::completeBufferedSequence() {
  currentBuffered_->cleanup();
  if (queuedBuffered_.empty()) {
    currentBuffered_.reset();
  } else {
    currentBuffered_ = queuedBuffered_.front();
    queuedBuffered_.pop_front();
    currentBuffered_->open();
  }
}

// 1.0 processBarrier(CheckpointBarrier, int)（:161）
void BarrierBuffer::processBarrier(const CheckpointBarrier& receivedBarrier,
                                   int channelIndex) {
  const int64_t barrierId = receivedBarrier.getId();

  if (numBarriersReceived_ > 0) {
    // subsequent barrier of a checkpoint.
    if (barrierId == currentCheckpointId_) {
      // regular case
      onBarrier(channelIndex);
    } else if (barrierId > currentCheckpointId_) {
      // 1.0: "Received checkpoint barrier for checkpoint {} before completing
      // current checkpoint {}. Skipping current checkpoint."
      releaseBlocks();
      currentCheckpointId_ = barrierId;
      onBarrier(channelIndex);
    } else {
      // ignore trailing barrier from aborted checkpoint
      return;
    }
  } else if (barrierId > currentCheckpointId_) {
    // first barrier of a new checkpoint
    currentCheckpointId_ = barrierId;
    onBarrier(channelIndex);
  } else {
    // trailing barrier from previous (skipped) checkpoint
    return;
  }

  // check if we have all barriers
  if (numBarriersReceived_ + numClosedChannels_ ==
      totalNumberOfInputChannels_) {
    // 1.0: "Received all barrier, triggering checkpoint {} at {}"
    if (checkpointHandler_ != NULL) {
      checkpointHandler_->onEvent(receivedBarrier);
    }
    releaseBlocks();
  }
}

// 1.0 registerCheckpointEventHandler(...)（:209）
void BarrierBuffer::registerCheckpointEventHandler(
    CheckpointEventListener* checkpointHandler) {
  if (checkpointHandler_ == NULL) {
    checkpointHandler_ = checkpointHandler;
  } else {
    throw std::logic_error(
        "BarrierBuffer already has a registered checkpoint handler");
  }
}

// 1.0 cleanup()（:224）
void BarrierBuffer::cleanup() {
  bufferSpiller_.close();
  if (currentBuffered_) {
    currentBuffered_->cleanup();
  }
  for (auto& sequence : queuedBuffered_) {
    sequence->cleanup();
  }
  queuedBuffered_.clear();
  currentBuffered_.reset();
}

// 1.0 onBarrier(int)（:250）
void BarrierBuffer::onBarrier(int channelIndex) {
  if (!blockedChannels_[channelIndex]) {
    blockedChannels_[channelIndex] = true;
    numBarriersReceived_++;
  } else {
    // 1.0 抛 IOException
    throw std::runtime_error(
        "Stream corrupt: Repeated barrier for same checkpoint and input "
        "stream");
  }
}

// 1.0 releaseBlocks()（:268）
// 原注释：「Releases the blocks on all channels. Makes sure the just written
// data is the next to be consumed.」
void BarrierBuffer::releaseBlocks() {
  for (size_t i = 0; i < blockedChannels_.size(); ++i) {
    blockedChannels_[i] = false;
  }
  numBarriersReceived_ = 0;

  if (!currentBuffered_) {
    // 1.0: "common case: no more buffered data"
    currentBuffered_ = bufferSpiller_.rollOver();
    if (currentBuffered_) {
      currentBuffered_->open();
    }
  } else {
    // 1.0: "uncommon case: buffered data pending. push back the pending data"
    BufferSpiller::SequencePtr bufferedNow =
        bufferSpiller_.rollOverWithNewBuffer();
    if (bufferedNow) {
      bufferedNow->open();
      queuedBuffered_.push_front(currentBuffered_);
      currentBuffered_ = bufferedNow;
    }
  }
}

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
