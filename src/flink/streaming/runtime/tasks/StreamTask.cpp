#include "flink/streaming/runtime/tasks/StreamTask.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>

#include "flink/runtime/io/network/api/CheckpointBarrier.h"
#include "flink/streaming/runtime/tasks/StreamTaskState.h"
#include "flink/streaming/runtime/tasks/TimerException.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

namespace {
int64_t currentTimeMillis() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
      .count();
}
}  // namespace

StreamTask::StreamTask(Environment* environment,
                       const StreamConfig& configuration)
    : headOperator_(NULL),
      environment_(environment),
      configuration_(configuration),
      chainEntryPoint_(NULL),
      recoveryTimestamp_(0),
      isRunning_(false),
      canceled_(false),
      timerServiceShutdown_(false) {}

StreamTask::~StreamTask() { shutdownTimerService(); }

std::string StreamTask::getName() const {
  return environment_ != NULL ? environment_->getTaskNameWithSubtasks()
                              : std::string("StreamTask");
}

std::vector<StreamTask::StreamOperator*> StreamTask::getAllOperators() const {
  // 【偏差 1】OperatorChain 到位后这里要返回「链头 + 链上其余算子」
  std::vector<StreamOperator*> operators;
  if (headOperator_ != NULL) {
    operators.push_back(headOperator_);
  }
  return operators;
}

// 1.0 StreamTask.invoke()（StreamTask.java:175）：结构、锁范围、finally 里的
// 清理顺序都照原样保留
void StreamTask::invoke() {
  bool disposed = false;
  try {
    // -------- Initialize ---------
    // 1.0 这里从配置里取出 headOperator 并建 OperatorChain；本实现的
    // headOperator 由 configuration.getStreamOperator() 提供（见偏差 1）
    if (configuration_.getStreamOperator()) {
      headOperator_ = configuration_.getStreamOperator().get();
    }
    if (headOperator_ != NULL) {
      headOperator_->setup(this, &configuration_, chainEntryPoint_);
    }

    startTimerService();

    // task specific initialization
    init();

    // 1.0: "save the work of reloading state, etc, if the task is already
    // canceled"
    if (canceled_) {
      throw std::runtime_error("CancelTaskException");
    }

    // -------- Invoke --------

    // 1.0: "first order of business is to give operators back their state"
    restoreState();

    // 1.0: "we need to make sure that any triggers scheduled in open() cannot
    // be executed before all operators are opened"
    {
      std::lock_guard<std::mutex> guard(lock_);
      openAllOperators();
    }

    if (canceled_) {
      throw std::runtime_error("CancelTaskException");
    }

    // let the task do its work
    isRunning_ = true;
    run();
    isRunning_ = false;

    // 1.0: "make sure no further checkpoint and notification actions happen
    // ... we also need to make sure that no triggers fire concurrently with
    // the close logic"
    {
      std::lock_guard<std::mutex> guard(lock_);
      closeAllOperators();
    }

    // make sure all buffered data is flushed
    flushOutputs();

    // 1.0: "make an attempt to dispose the operators such that failures in
    // the dispose call still let the computation fail"
    tryDisposeAllOperators();
    disposed = true;
  } catch (...) {
    // 对应 1.0 的 finally 块（C++ 没有 finally，用 catch + 重抛）
    isRunning_ = false;
    shutdownTimerService();
    releaseOutputs();
    try {
      cleanup();
    } catch (...) {
      // 1.0: "catch and log the exception to not replace the original
      // exception"
    }
    if (!disposed) {
      disposeAllOperators();
    }
    throw;
  }

  // 正常路径上的 finally
  isRunning_ = false;
  shutdownTimerService();
  releaseOutputs();
  try {
    cleanup();
  } catch (...) {
  }
  if (!disposed) {
    disposeAllOperators();
  }
}

// 1.0 cancel()（:297）
void StreamTask::cancel() {
  isRunning_ = false;
  canceled_ = true;
  cancelTask();
}

// 1.0 openAllOperators()（:311）：从链尾往链头开，这样上游 open 时下游已就绪
void StreamTask::openAllOperators() {
  std::vector<StreamOperator*> operators = getAllOperators();
  for (auto it = operators.rbegin(); it != operators.rend(); ++it) {
    if (*it != NULL) (*it)->open();
  }
}

// 1.0 closeAllOperators()（:319）：从链头往链尾关，让数据能一路 flush 出去
void StreamTask::closeAllOperators() {
  std::vector<StreamOperator*> operators = getAllOperators();
  for (StreamOperator* op : operators) {
    if (op != NULL) op->close();
  }
}

// 1.0 tryDisposeAllOperators()（:331）：异常会往外抛，让作业失败
void StreamTask::tryDisposeAllOperators() {
  for (StreamOperator* op : getAllOperators()) {
    if (op != NULL) op->dispose();
  }
}

// 1.0 disposeAllOperators()（:339）：兜底版，吞掉异常只记日志
void StreamTask::disposeAllOperators() {
  for (StreamOperator* op : getAllOperators()) {
    if (op == NULL) continue;
    try {
      op->dispose();
    } catch (...) {
      // 1.0: "LOG.error("Error during disposal of stream operator.", t)"
    }
  }
}

// 1.0 private restoreState()（:429）：按链上下标把状态派回各算子
void StreamTask::restoreState() {
  if (!lazyRestoreState_) return;

  const std::vector<StreamTaskStateList::StreamTaskStatePtr>& states =
      lazyRestoreState_->getState();
  std::vector<StreamOperator*> allOperators = getAllOperators();

  for (size_t i = 0; i < states.size() && i < allOperators.size(); ++i) {
    if (states[i] && allOperators[i] != NULL) {
      allOperators[i]->restoreState(*states[i], recoveryTimestamp_);
    }
  }
  // 1.0: "be GC friendly"
  lazyRestoreState_.reset();
}

void StreamTask::flushOutputs() {
  // 1.0 operatorChain.flushOutputs() -> RecordWriterOutput.flush()。
  // 本实现的 Output 还没有缓冲层（RecordWriter 待落地），暂无动作。
}

void StreamTask::releaseOutputs() {
  // 1.0 operatorChain.releaseOutputs() -> RecordWriterOutput.close()
  for (Output* output : streamOutputs_) {
    if (output != NULL) output->close();
  }
}

// 1.0 setInitialState(StreamTaskStateList, long)（:424）
void StreamTask::setInitialState(const StreamTaskStateList& stateHandle,
                                 int64_t recoveryTimestamp) {
  lazyRestoreState_ = std::make_shared<StreamTaskStateList>(stateHandle);
  recoveryTimestamp_ = recoveryTimestamp;
}

// 1.0 triggerCheckpoint(long, long)（:460）
bool StreamTask::triggerCheckpoint(int64_t checkpointId, int64_t timestamp) {
  std::lock_guard<std::mutex> guard(lock_);
  if (!isRunning_) {
    return false;
  }

  // 1.0: "since both state checkpointing and downstream barrier emission
  // occurs in this lock scope, they are an atomic operation regardless of the
  // order in which they occur. we immediately emit the checkpoint barriers,
  // so the downstream operators can start their checkpoint work as soon as
  // possible"
  broadcastCheckpointBarrier(checkpointId, timestamp);

  // now draw the state snapshot
  std::vector<StreamOperator*> allOperators = getAllOperators();
  std::vector<StreamTaskStateList::StreamTaskStatePtr> states(
      allOperators.size());

  for (size_t i = 0; i < allOperators.size(); ++i) {
    if (allOperators[i] == NULL) continue;
    StreamTaskState state =
        allOperators[i]->snapshotOperatorState(checkpointId, timestamp);
    // 1.0: states[i] = state.isEmpty() ? null : state;
    if (!state.isEmpty()) {
      states[i] = std::make_shared<StreamTaskState>(state);
    }
  }

  if (!isRunning_) {
    // 1.0: "Rethrow the cancel exception because some state backends could
    // swallow exceptions and seem to exit cleanly."
    throw std::runtime_error("CancelTaskException");
  }

  StreamTaskStateList allStates(states);

  // 1.0 的三分支里只保留同步两支（见偏差 3）
  if (environment_ != NULL) {
    if (allStates.isEmpty()) {
      environment_->acknowledgeCheckpoint(checkpointId);
    } else {
      environment_->acknowledgeCheckpoint(checkpointId, allStates);
    }
  }
  return true;
}

// 1.0 notifyCheckpointComplete(long)（:572）
void StreamTask::notifyCheckpointComplete(int64_t checkpointId) {
  std::lock_guard<std::mutex> guard(lock_);
  if (!isRunning_) {
    // 1.0: "Ignoring notification of complete checkpoint for not-running task"
    return;
  }
  for (StreamOperator* op : getAllOperators()) {
    if (op != NULL) op->notifyOfCompletedCheckpoint(checkpointId);
  }
}

// 1.0 operatorChain.broadcastCheckpointBarrier(checkpointId, timestamp)
void StreamTask::broadcastCheckpointBarrier(int64_t checkpointId,
                                            int64_t timestamp) {
  auto barrier =
      std::make_shared<const flink::runtime::io::network::api::CheckpointBarrier>(
          checkpointId, timestamp);
  for (Output* output : streamOutputs_) {
    if (output != NULL) output->broadcastEvent(barrier);
  }
}

// 1.0 createStateBackend(String, TypeSerializer<?>)（:593）
StreamTask::StateBackendPtr StreamTask::createStateBackend(
    const std::string& operatorIdentifier, SerializerPtr keySerializer) {
  StateBackendPtr stateBackend = configuration_.getStateBackend();
  if (!stateBackend) {
    // 1.0 这里会退回 StateBackendFactory 按 flink-conf 里的配置建一个；
    // 本实现只有 MemoryStateBackend，由搭建代码写进 StreamConfig，缺了就是
    // 配置错误
    throw std::runtime_error(
        "No state backend configured for operator " + operatorIdentifier);
  }
  stateBackend->initializeForJob(operatorIdentifier, keySerializer);
  return stateBackend;
}

// 1.0 isSerializingTimestamps()（:377）
bool StreamTask::isSerializingTimestamps() const {
  flink::streaming::api::TimeCharacteristic tc =
      configuration_.getTimeCharacteristic();
  return tc == flink::streaming::api::TimeCharacteristic::EventTime ||
         tc == flink::streaming::api::TimeCharacteristic::IngestionTime;
}

// ------------------------------------------------------------------------
//  定时器服务（见偏差 2）
// ------------------------------------------------------------------------

void StreamTask::startTimerService() {
  if (timerThread_.joinable()) return;
  timerServiceShutdown_ = false;
  // 1.0: newSingleThreadScheduledExecutor，名字是 "Time Trigger for " + getName()
  timerThread_ = std::thread(&StreamTask::timerServiceLoop, this);
}

void StreamTask::shutdownTimerService() {
  if (!timerThread_.joinable()) return;
  {
    std::lock_guard<std::mutex> guard(timerMutex_);
    timerServiceShutdown_ = true;
  }
  timerCondition_.notify_all();
  // 1.0 是 timerService.shutdownNow()：不等待排队中的任务
  timerThread_.join();
}

// 1.0 registerTimer(long, Triggerable)（:649）
void StreamTask::registerTimer(int64_t timestamp, Triggerable* target) {
  {
    std::lock_guard<std::mutex> guard(timerMutex_);
    // 1.0: long delay = Math.max(timestamp - System.currentTimeMillis(), 0);
    // 这里存绝对时间，由循环自己算等待时长，效果等价
    timerQueue_.push(TriggerTask{timestamp, target});
  }
  timerCondition_.notify_all();
}

// 1.0 TriggerTask.run()（:707）
void StreamTask::timerServiceLoop() {
  std::unique_lock<std::mutex> timerGuard(timerMutex_);
  while (!timerServiceShutdown_) {
    if (timerQueue_.empty()) {
      timerCondition_.wait(timerGuard);
      continue;
    }

    const TriggerTask next = timerQueue_.top();
    const int64_t delay = next.timestamp - currentTimeMillis();
    if (delay > 0) {
      timerCondition_.wait_for(timerGuard, std::chrono::milliseconds(delay));
      continue;  // 重新检查：期间可能插入了更早的定时器，或者被 shutdown
    }
    timerQueue_.pop();

    // 回调期间放开 timerMutex_，但**必须持有 checkpointLock**
    timerGuard.unlock();
    {
      std::lock_guard<std::mutex> checkpointGuard(lock_);
      try {
        next.target->trigger(next.timestamp);
      } catch (const std::exception& e) {
        // 1.0: 异常不崩定时器线程，记进 asyncException 等主线程重抛
        if (asyncException_.empty()) {
          asyncException_ = e.what();
        }
      } catch (...) {
        if (asyncException_.empty()) {
          asyncException_ = "unknown exception in timer callback";
        }
      }
    }
    timerGuard.lock();
  }
}

// 1.0 checkTimerException()（:658）
void StreamTask::checkTimerException() const {
  std::lock_guard<std::mutex> guard(timerMutex_);
  if (!asyncException_.empty()) {
    throw TimerException(asyncException_);
  }
}

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
