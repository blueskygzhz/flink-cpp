#include "flink/streaming/runtime/tasks/SourceStreamTask.h"

#include <stdexcept>

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

// 【偏差 1】1.0 的 headOperator 字段本身就是 StreamSource
SourceStreamTask::SourceOperator* SourceStreamTask::sourceOperator() const {
  return dynamic_cast<SourceOperator*>(headOperator_);
}

// 1.0 run()：headOperator.run(getCheckpointLock(), getHeadOutput())（:56）
void SourceStreamTask::run() {
  SourceOperator* source = sourceOperator();
  if (source == NULL) {
    throw std::logic_error(
        "The head operator of a SourceStreamTask must be a StreamSource");
  }
  source->run(getCheckpointLock(), getHeadOutput());
}

// 1.0 cancelTask()：headOperator.cancel()（:61）
void SourceStreamTask::cancelTask() {
  SourceOperator* source = sourceOperator();
  // 【偏差 2】1.0 这时 headOperator 一定已就位
  if (source != NULL) {
    source->cancel();
  }
}

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
