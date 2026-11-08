/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.tasks.StreamTask
 *
 * 1.0 abstract class StreamTask<OUT, Operator extends StreamOperator<OUT>>
 *       extends AbstractInvokable
 *       implements StatefulTask<StreamTaskStateList>   (StreamTask.java:101)
 *
 * 字段（StreamTask.java:117-156）：
 *   private final Object lock = new Object();   // 著名的 checkpointLock
 *   protected Operator headOperator;
 *   private OperatorChain<OUT> operatorChain;
 *   private StreamConfig configuration;
 *   private ScheduledExecutorService timerService;
 *   private StreamTaskStateList lazyRestoreState;
 *   private volatile AsynchronousException asyncException;
 *   private volatile boolean isRunning;
 *   private volatile boolean canceled;
 *   private long recoveryTimestamp;
 *
 * 模板方法（子类实现，StreamTask.java:162-168）：
 *   protected abstract void init();
 *   protected abstract void run();
 *   protected abstract void cleanup();
 *   protected abstract void cancelTask();
 *
 * 公共方法：
 *   public final void invoke()                  生命周期总入口（:175）
 *   public final void cancel()                  (:297)
 *   public Object getCheckpointLock()           (:399)
 *   public void setInitialState(StreamTaskStateList, long)   (:424)
 *   public boolean triggerCheckpoint(long, long)             (:460)
 *   public void notifyCheckpointComplete(long)               (:572)
 *   public AbstractStateBackend createStateBackend(String, TypeSerializer<?>)  (:593)
 *   public void registerTimer(long, Triggerable)             (:649)
 *   public void checkTimerException()                        (:658)
 *   protected boolean isSerializingTimestamps()              (:377)
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— reporter 参数】1.0 的 invoke() 第一件事是
 * `operatorChain = new OperatorChain<>(this, headOperator,
 *   getEnvironment().getAccumulatorRegistry().getReadWriteReporter())`。
 * 本实现已按 1.0 接上 OperatorChain，只是构造参数里的
 * AccumulatorRegistry.Reporter 省掉了（见 OperatorChain.h 偏差 3）。
 * getAllOperators() / flushOutputs() / releaseOutputs() /
 * broadcastCheckpointBarrier() 全部转给 operatorChain_，与 1.0 一致。
 *
 * 【偏差 2 —— 定时器服务】1.0 用
 * `Executors.newSingleThreadScheduledExecutor(DispatcherThreadFactory(
 *   TRIGGER_THREAD_GROUP, "Time Trigger for " + getName()))`。C++ 标准库没有
 * ScheduledExecutorService，这里自己起一条线程 + 最小堆，语义对齐 1.0 的
 * TriggerTask.run（StreamTask.java:707）：**先拿 checkpointLock 再回调
 * trigger(timestamp)**，异常不崩线程而是记到 asyncException 里，由主线程在
 * checkTimerException() 重抛。单线程这点也保留——1.0 靠它保证定时器之间不并发。
 *
 * 【偏差 3 —— 异步快照】1.0 的 triggerCheckpoint 会检查
 * AsynchronousStateHandle / AsynchronousKvStateSnapshot，有的话另起线程做
 * materialize 再 acknowledge。本实现只有内存后端（同步），所以这条分支不复刻，
 * hasAsyncStates 恒为 false。
 */
#ifndef FLINK_STREAMING_RUNTIME_TASKS_STREAMTASK_H
#define FLINK_STREAMING_RUNTIME_TASKS_STREAMTASK_H

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "flink/runtime/execution/Environment.h"
#include "flink/runtime/jobgraph/tasks/StatefulTask.h"
#include "flink/runtime/state/AbstractStateBackend.h"
#include "flink/streaming/api/graph/StreamConfig.h"
#include "flink/streaming/api/operators/Output.h"
#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/runtime/operators/Triggerable.h"
#include "flink/streaming/runtime/tasks/OperatorChain.h"
#include "flink/streaming/runtime/tasks/StreamTaskStateList.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

class StreamTask : public flink::runtime::jobgraph::tasks::StatefulTask {
 public:
  typedef flink::streaming::api::graph::StreamConfig StreamConfig;
  typedef flink::streaming::api::operators::Output Output;
  typedef flink::streaming::api::operators::StreamOperator StreamOperator;
  typedef flink::streaming::runtime::operators::Triggerable Triggerable;
  typedef flink::streaming::runtime::io::RecordWriterOutput RecordWriterOutput;
  typedef flink::runtime::execution::Environment Environment;
  typedef std::shared_ptr<flink::runtime::state::AbstractStateBackend>
      StateBackendPtr;
  typedef flink::runtime::state::KvState::SerializerPtr SerializerPtr;

  // 【偏差】1.0 由 TaskManager 反序列化出 invokable 再注入 Environment 与
  // TaskConfiguration；本实现进程内直接构造注入
  StreamTask(Environment* environment, const StreamConfig& configuration);
  ~StreamTask() override;

  // 1.0 AbstractInvokable.invoke() 的实现（StreamTask.java:175）
  void invoke();

  // 1.0 cancel()（:297）
  void cancel();

  bool isRunning() const { return isRunning_; }
  bool isCanceled() const { return canceled_; }

  // 1.0 getName()：实际走 getEnvironment().getTaskNameWithSubtasks()
  std::string getName() const;

  // 1.0 getCheckpointLock()：返回那个 `private final Object lock`
  std::mutex& getCheckpointLock() { return lock_; }

  StreamConfig& getConfiguration() { return configuration_; }
  Environment* getEnvironment() const { return environment_; }

  // 1.0 getHeadOutput() -> operatorChain.getChainEntryPoint()
  // 1.0 getStreamOutputs() -> operatorChain.getStreamOutputs()
  Output* getHeadOutput() const;
  const std::vector<RecordWriterOutput*>& getStreamOutputs() const;

  // 1.0 operatorChain.getAllOperators()
  std::vector<StreamOperator*> getAllOperators() const;

  // ---------------------------- StatefulTask ----------------------------

  void setInitialState(const StreamTaskStateList& stateHandle,
                       int64_t recoveryTimestamp) override;
  bool triggerCheckpoint(int64_t checkpointId, int64_t timestamp) override;
  void notifyCheckpointComplete(int64_t checkpointId) override;

  // --------------------------- state backend ---------------------------

  // 1.0 createStateBackend(String operatorIdentifier, TypeSerializer<?>)
  StateBackendPtr createStateBackend(const std::string& operatorIdentifier,
                                     SerializerPtr keySerializer);

  // ------------------------------ timers -------------------------------

  // 1.0 registerTimer(long timestamp, Triggerable target)
  void registerTimer(int64_t timestamp, Triggerable* target);

  // 1.0 checkTimerException()：主线程重抛定时器线程里攒下的异常
  void checkTimerException() const;

 protected:
  // 1.0 的四个模板方法
  virtual void init() = 0;
  virtual void run() = 0;
  virtual void cleanup() = 0;
  virtual void cancelTask() = 0;

  // 1.0 isSerializingTimestamps()（:377）：EventTime / IngestionTime 下
  // StreamRecord 的时间戳要随记录一起序列化
  bool isSerializingTimestamps() const;

  // 1.0 operatorChain.broadcastCheckpointBarrier(checkpointId, timestamp)
  void broadcastCheckpointBarrier(int64_t checkpointId, int64_t timestamp);

  // 1.0 的 headOperator 字段
  StreamOperator* headOperator_;

 private:
  // 1.0 openAllOperators / closeAllOperators / tryDisposeAllOperators /
  //     disposeAllOperators（:311-339）
  void openAllOperators();
  void closeAllOperators();
  void tryDisposeAllOperators();
  void disposeAllOperators();

  // 1.0 的 private restoreState()（:429）—— 注意与 StreamOperator.restoreState
  // 同名但不同层：这里是把 lazyRestoreState 按下标派回各算子
  void restoreState();

  // 1.0 operatorChain.flushOutputs() / releaseOutputs()
  void flushOutputs();
  void releaseOutputs();

  // —— 定时器线程（见偏差 2）——
  struct TriggerTask {
    int64_t timestamp;
    Triggerable* target;
    // std::priority_queue 是大顶堆，反着比以得到最早到期优先
    bool operator<(const TriggerTask& other) const {
      return timestamp > other.timestamp;
    }
  };
  void startTimerService();
  void shutdownTimerService();
  void timerServiceLoop();

  Environment* environment_;
  StreamConfig configuration_;

  // 1.0 `private final Object lock`：算子方法、快照、定时器回调三者互斥
  mutable std::mutex lock_;

  // 1.0 `private OperatorChain<OUT> operatorChain`
  std::unique_ptr<OperatorChain> operatorChain_;

  std::shared_ptr<StreamTaskStateList> lazyRestoreState_;
  int64_t recoveryTimestamp_;

  volatile bool isRunning_;
  volatile bool canceled_;

  std::thread timerThread_;
  mutable std::mutex timerMutex_;
  std::condition_variable timerCondition_;
  std::priority_queue<TriggerTask> timerQueue_;
  bool timerServiceShutdown_;
  mutable std::string asyncException_;
};

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_TASKS_STREAMTASK_H
