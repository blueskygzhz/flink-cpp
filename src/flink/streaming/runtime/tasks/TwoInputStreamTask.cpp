#include "flink/streaming/runtime/tasks/TwoInputStreamTask.h"

#include <stdexcept>
#include <string>
#include <vector>

#include "flink/streaming/api/graph/StreamEdge.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

// 1.0 init()（TwoInputStreamTask.java:40）
void TwoInputStreamTask::init() {
  StreamConfig& configuration = getConfiguration();

  // 1.0: TypeSerializer<IN1> inputDeserializer1 =
  //        configuration.getTypeSerializerIn1(userClassLoader);
  //      TypeSerializer<IN2> inputDeserializer2 = ...
  // —— 【偏差 1】没有反序列化层，不取

  const int numberOfInputs = configuration.getNumberOfInputs();

  std::vector<StreamTwoInputProcessor::InputGate*> inputList1;
  std::vector<StreamTwoInputProcessor::InputGate*> inputList2;

  const StreamConfig::StreamEdgeList& inEdges =
      configuration.getInPhysicalEdges();

  // 1.0:54-67
  for (int i = 0; i < numberOfInputs; ++i) {
    // 【偏差 2】1.0 直接 inEdges.get(i) / getInputGate(i)，配错就越界 / NPE
    if (i >= static_cast<int>(inEdges.size()) || !inEdges[i]) {
      throw std::logic_error(
          "TwoInputStreamTask: numberOfInputs is " +
          std::to_string(numberOfInputs) +
          " but inPhysicalEdges has only " + std::to_string(inEdges.size()) +
          " entries");
    }
    const int inputType = inEdges[i]->getTypeNumber();
    StreamTwoInputProcessor::InputGate* reader = getEnvironment()->getInputGate(i);
    if (reader == NULL) {
      throw std::logic_error("TwoInputStreamTask: missing input gate " +
                             std::to_string(i));
    }
    switch (inputType) {
      case 1:
        inputList1.push_back(reader);
        break;
      case 2:
        inputList2.push_back(reader);
        break;
      default:
        // 1.0: throw new RuntimeException("Invalid input type number: " + ...)
        throw std::runtime_error("Invalid input type number: " +
                                 std::to_string(inputType));
    }
  }

  // 1.0:69
  inputProcessor_.reset(new StreamTwoInputProcessor(
      inputList1, inputList2, getCheckpointBarrierListener(),
      configuration.getCheckpointMode()));

  // 1.0: "make sure that stream tasks report their I/O statistics"
  //      inputProcessor.setReporter(reporter)  —— 【偏差 3】
}

// 1.0 run()（:83）
void TwoInputStreamTask::run() {
  // 1.0: "cache some references on the stack, to make the code more JIT
  // friendly"
  TwoInputStreamOperator* streamOperator =
      dynamic_cast<TwoInputStreamOperator*>(headOperator_);
  StreamTwoInputProcessor* inputProcessor = inputProcessor_.get();
  std::recursive_mutex& lock = getCheckpointLock();

  // 【偏差 4】1.0 由泛型上界保证
  if (streamOperator == NULL) {
    throw std::logic_error(
        "The head operator of a TwoInputStreamTask must be a "
        "TwoInputStreamOperator");
  }

  while (running_ && inputProcessor->processInput(streamOperator, lock)) {
    checkTimerException();
  }
}

// 1.0 cleanup()（:95）
void TwoInputStreamTask::cleanup() {
  if (inputProcessor_) {
    inputProcessor_->cleanup();
  }
}

// 1.0 cancelTask()（:100）
void TwoInputStreamTask::cancelTask() { running_ = false; }

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
