#include "flink/streaming/runtime/tasks/OneInputStreamTask.h"

#include <stdexcept>
#include <vector>

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

// 1.0 init()（OneInputStreamTask.java:37）
void OneInputStreamTask::init() {
  StreamConfig& configuration = getConfiguration();

  // 1.0: TypeSerializer<IN> inSerializer =
  //        configuration.getTypeSerializerIn1(getUserCodeClassLoader());
  // —— 【偏差 1】没有反序列化层，不取

  const int numberOfInputs = configuration.getNumberOfInputs();

  if (numberOfInputs > 0) {
    std::vector<StreamInputProcessor::InputGate*> inputGates =
        getEnvironment()->getAllInputGates();

    inputProcessor_.reset(new StreamInputProcessor(
        inputGates, getCheckpointBarrierListener(),
        configuration.getCheckpointMode()));

    // 1.0: "make sure that stream tasks report their I/O statistics"
    //      inputProcessor.setReporter(reporter)  —— 【偏差 3】
  }
}

// 1.0 run()（:59）
void OneInputStreamTask::run() {
  // 1.0: "cache some references on the stack, to make the code more JIT
  // friendly"
  OneInputStreamOperator* streamOperator =
      dynamic_cast<OneInputStreamOperator*>(headOperator_);
  StreamInputProcessor* inputProcessor = inputProcessor_.get();
  std::recursive_mutex& lock = getCheckpointLock();

  // 【偏差 2】1.0 在这里直接 NPE
  if (inputProcessor == NULL) {
    throw std::logic_error(
        "OneInputStreamTask has no input processor; the vertex is configured "
        "with zero inputs");
  }
  // 【偏差 4】1.0 由泛型上界保证
  if (streamOperator == NULL) {
    throw std::logic_error(
        "The head operator of a OneInputStreamTask must be a "
        "OneInputStreamOperator");
  }

  while (running_ && inputProcessor->processInput(streamOperator, lock)) {
    checkTimerException();
  }
}

// 1.0 cleanup()（:71）
void OneInputStreamTask::cleanup() {
  if (inputProcessor_) {
    inputProcessor_->cleanup();
  }
}

// 1.0 cancelTask()（:76）
void OneInputStreamTask::cancelTask() { running_ = false; }

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
