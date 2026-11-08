#include "flink/streaming/runtime/io/BarrierTracker.h"

#include <stdexcept>
#include <typeinfo>

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

namespace {
typedef flink::runtime::io::network::api::CheckpointBarrier CheckpointBarrier;
}  // namespace

BarrierTracker::BarrierTracker(InputGate* inputGate)
    : inputGate_(inputGate),
      totalNumberOfInputChannels_(inputGate->getNumberOfInputChannels()),
      checkpointHandler_(NULL),
      // 1.0: latestPendingCheckpointID = -1
      latestPendingCheckpointID_(-1) {}

// 1.0 getNextNonBlocked()（BarrierTracker.java:73）
bool BarrierTracker::getNextNonBlocked(BufferOrEvent& target) {
  while (true) {
    BufferOrEvent next;
    if (!inputGate_->getNextBufferOrEvent(next)) {
      return false;  // 1.0: return null;
    }
    if (next.isBuffer() ||
        typeid(*next.getEvent()) != typeid(CheckpointBarrier)) {
      target = next;
      return true;
    }
    processBarrier(static_cast<const CheckpointBarrier&>(*next.getEvent()));
  }
}

// 1.0 registerCheckpointEventHandler(...)（:88）
void BarrierTracker::registerCheckpointEventHandler(
    CheckpointEventListener* checkpointHandler) {
  if (checkpointHandler_ == NULL) {
    checkpointHandler_ = checkpointHandler;
  } else {
    throw std::logic_error(
        "BarrierTracker already has a registered checkpoint handler");
  }
}

// 1.0 processBarrier(CheckpointBarrier)（:110）
void BarrierTracker::processBarrier(const CheckpointBarrier& receivedBarrier) {
  // 1.0: "fast path for single channel trackers"
  if (totalNumberOfInputChannels_ == 1) {
    if (checkpointHandler_ != NULL) {
      checkpointHandler_->onEvent(receivedBarrier);
    }
    return;
  }

  // 1.0: "general path for multiple input channels"
  const int64_t barrierId = receivedBarrier.getId();

  // 1.0: "find the checkpoint barrier in the queue of pending barriers"
  CheckpointBarrierCount* cbc = NULL;
  size_t pos = 0;
  for (size_t i = 0; i < pendingCheckpoints_.size(); ++i) {
    if (pendingCheckpoints_[i].checkpointId == barrierId) {
      cbc = &pendingCheckpoints_[i];
      pos = i;
      break;
    }
  }

  if (cbc != NULL) {
    // 1.0: "add one to the count to that barrier and check for completion"
    const int numBarriersNew = ++cbc->barrierCount;
    if (numBarriersNew == totalNumberOfInputChannels_) {
      // 1.0: "first, remove this checkpoint and all prior pending
      // checkpoints (which are now subsumed)"
      for (size_t i = 0; i <= pos; ++i) {
        pendingCheckpoints_.pop_front();
      }
      // 1.0: "notify the listener"
      if (checkpointHandler_ != NULL) {
        checkpointHandler_->onEvent(receivedBarrier);
      }
    }
  } else {
    // 1.0: "first barrier for that checkpoint ID. add it only if it is newer
    // than the latest checkpoint. if it is not newer than the latest
    // checkpoint ID, then there cannot be a successful checkpoint for that ID
    // anyways"
    if (barrierId > latestPendingCheckpointID_) {
      latestPendingCheckpointID_ = barrierId;
      pendingCheckpoints_.push_back(CheckpointBarrierCount{barrierId, 1});

      // 1.0: "make sure we do not track too many checkpoints"
      if (static_cast<int>(pendingCheckpoints_.size()) >
          MAX_CHECKPOINTS_TO_TRACK) {
        pendingCheckpoints_.pop_front();
      }
    }
  }
}

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
