/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.operators.AbstractStreamOperator
 *
 * 1.0 abstract class AbstractStreamOperator<OUT> implements StreamOperator<OUT>
 *     （AbstractStreamOperator.java:57）
 *
 * 字段（:64-91）：
 *   protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;
 *   private boolean inputCopyDisabled = false;
 *   private transient StreamTask<?, ?> container;
 *   private transient StreamConfig config;
 *   protected transient Output<StreamRecord<OUT>> output;
 *   private transient StreamingRuntimeContext runtimeContext;
 *   private transient KeySelector<?, ?> stateKeySelector1, stateKeySelector2;
 *   private AbstractStateBackend stateBackend = null;
 *
 * 方法：
 *   setup(StreamTask, StreamConfig, Output)      (:99)
 *   open() / close()                             默认空实现
 *   dispose()                                    关掉并释放 state backend
 *   snapshotOperatorState(long, long)            只管 keyed state
 *   restoreState(StreamTaskState, long)          注入快照（真正恢复是惰性的）
 *   notifyOfCompletedCheckpoint(long)
 *   getExecutionConfig() / getOperatorConfig() / getContainingTask()
 *   getRuntimeContext() / getStateBackend()
 *   protected registerTimer(long time, Triggerable target)        (:249)
 *   protected <S extends State> S getPartitionedState(StateDescriptor)   (:259)
 *   protected <S extends State, N> S getPartitionedState(N, TypeSerializer<N>,
 *                                                       StateDescriptor) (:270)
 *   setKeyContextElement1/2(StreamRecord) / setKeyContext(Object)
 *   final setChainingStrategy / final getChainingStrategy
 *   isInputCopyingDisabled() / disableInputCopy()
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— operatorIdentifier 的算子名】1.0 用
 *   getClass().getSimpleName() + "_" + config.getVertexID() + "_" +
 *   runtimeContext.getIndexOfThisSubtask()
 * 作为状态分组标识。C++ 没有反射拿类名，所以抽出虚方法 getSimpleName()，
 * 各算子自己返回与 1.0 同名的字符串（"StreamMap" / "WindowOperator" ...）。
 * 这个字符串会进状态快照的归档路径，所以必须与 1.0 对齐。
 *
 * 【偏差 2 —— StreamingRuntimeContext 尚未落地】1.0 在 setup() 里建
 * StreamingRuntimeContext（给 UDF 提供 accumulator / 并行度 / keyed state）。
 * 它依赖 Environment 的 AccumulatorRegistry 与 BroadcastVariableManager，
 * 本实现还没有这两样，所以 subtask 下标直接从 Environment 取，
 * getRuntimeContext() 暂缺。getExecutionConfig() 同理（1.0 走
 * container.getExecutionConfig()）。
 *
 * 【偏差 3 —— getPartitionedState 是模板而非泛型方法】见
 * AbstractStateBackend.h 偏差 1：靠重载决议代替 StateDescriptor.bind。
 *
 * 【偏差 4 —— isInputCopyingDisabled 无实际作用】见 StreamOperator.h。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_ABSTRACTSTREAMOPERATOR_H
#define FLINK_STREAMING_API_OPERATORS_ABSTRACTSTREAMOPERATOR_H

#include <stdexcept>
#include <string>

#include "flink/api/common/typeutils/base/VoidSerializer.h"
#include "flink/api/java/functions/AnyKeySelector.h"
#include "flink/runtime/state/AbstractStateBackend.h"
#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/runtime/operators/Triggerable.h"
#include "flink/streaming/runtime/tasks/StreamTask.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

class AbstractStreamOperator : public virtual StreamOperator {
 public:
  typedef flink::runtime::state::AbstractStateBackend AbstractStateBackend;
  typedef std::shared_ptr<AbstractStateBackend> StateBackendPtr;
  typedef flink::streaming::runtime::operators::Triggerable Triggerable;
  typedef std::shared_ptr<flink::api::java::functions::AnyKeySelector>
      KeySelectorPtr;

  AbstractStreamOperator()
      // 1.0 AbstractStreamOperator.java:68 "A sane default for most operators"
      : chainingStrategy_(ChainingStrategy::HEAD),
        output_(NULL),
        inputCopyDisabled_(false),
        container_(NULL),
        config_(NULL) {}

  // ---------------------------- life cycle ----------------------------

  // 1.0 setup(StreamTask<?,?>, StreamConfig, Output<StreamRecord<OUT>>)（:99）
  void setup(StreamTask* containingTask, StreamConfig* config,
             Output* output) override {
    container_ = containingTask;
    config_ = config;
    output_ = output;

    stateKeySelector1_ = config->getStatePartitioner(0);
    stateKeySelector2_ = config->getStatePartitioner(1);

    // 1.0: "if the keySerializer is null we still need to create the state
    // backend for the non-partitioned state features it provides, such as the
    // state output streams"
    AbstractStateBackend::SerializerPtr keySerializer =
        config->getStateKeySerializer();

    int subtaskIndex = container_ != NULL && container_->getEnvironment() != NULL
                           ? container_->getEnvironment()->getIndexInSubtaskGroup()
                           : 0;
    std::string operatorIdentifier = getSimpleName() + "_" +
                                     std::to_string(config->getVertexID()) +
                                     "_" + std::to_string(subtaskIndex);
    stateBackend_ =
        container_->createStateBackend(operatorIdentifier, keySerializer);
  }

  // 1.0 的默认实现就是空的
  void open() override {}
  void close() override {}

  // 1.0 dispose()：关掉并释放 state backend，失败则包成 RuntimeException
  void dispose() override {
    if (stateBackend_) {
      try {
        stateBackend_->close();
        stateBackend_->dispose();
      } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Error while closing/disposing state backend. ") +
            e.what());
      }
    }
  }

  // -------------------------- Checkpointing --------------------------

  // 1.0 snapshotOperatorState(long, long)：这里只处理 keyed state 快照，
  // UDF 的状态由 AbstractUdfStreamOperator 覆写后追加
  StreamTaskState snapshotOperatorState(int64_t checkpointId,
                                        int64_t timestamp) override {
    StreamTaskState state;
    if (stateBackend_) {
      AbstractStateBackend::StateHandleMap partitionedSnapshots =
          stateBackend_->snapshotPartitionedState(checkpointId, timestamp);
      if (!partitionedSnapshots.empty()) {
        state.setKvStates(partitionedSnapshots);
      }
    }
    return state;
  }

  // 1.0 restoreState(StreamTaskState, long)
  // 原注释：「the actual restore happens lazily, when the function requests
  // the state again, because the restore method needs information provided by
  // the user function」—— 本实现的延迟恢复见 AbstractStateBackend.h 偏差 3
  void restoreState(const StreamTaskState& state,
                    int64_t recoveryTimestamp) override {
    if (stateBackend_) {
      stateBackend_->injectKeyValueStateSnapshots(state.getKvStates(),
                                                  recoveryTimestamp);
    }
  }

  void notifyOfCompletedCheckpoint(int64_t checkpointId) override {
    if (stateBackend_) {
      stateBackend_->notifyOfCompletedCheckpoint(checkpointId);
    }
  }

  // --------------------- Properties and Services ---------------------

  StreamConfig* getOperatorConfig() const { return config_; }
  StreamTask* getContainingTask() const { return container_; }
  const StateBackendPtr& getStateBackend() const { return stateBackend_; }

  // 【偏差 1】1.0 用 getClass().getSimpleName()
  virtual std::string getSimpleName() const = 0;

  // ------------------------- key context -------------------------

  // 1.0 setKeyContextElement1(StreamRecord)
  void setKeyContextElement1(const StreamRecord& record) override {
    if (stateKeySelector1_) {
      stateBackend_->setCurrentKey(stateKeySelector1_->getKey(record.getValue()));
    }
  }

  void setKeyContextElement2(const StreamRecord& record) override {
    if (stateKeySelector2_) {
      stateBackend_->setCurrentKey(stateKeySelector2_->getKey(record.getValue()));
    }
  }

  // 1.0 setKeyContext(Object key)：WindowOperator 处理定时器触发时用它
  // 把 state backend 切到某个 key 上
  void setKeyContext(const flink::types::Any& key) {
    if (stateKeySelector1_) {
      stateBackend_->setCurrentKey(key);
    }
  }

  // ----------------- Context and chaining properties -----------------

  // 1.0 这两个是 final
  void setChainingStrategy(ChainingStrategy strategy) final {
    chainingStrategy_ = strategy;
  }
  ChainingStrategy getChainingStrategy() const final {
    return chainingStrategy_;
  }

  bool isInputCopyingDisabled() const override { return inputCopyDisabled_; }

  // 1.0 disableInputCopy()
  void disableInputCopy() { inputCopyDisabled_ = true; }

 protected:
  // 1.0 registerTimer(long time, Triggerable target)（:249）
  // 原注释：「This call is guaranteed to not happen concurrently with method
  // calls on the operator.」—— 由 StreamTask 的 checkpointLock 保证
  void registerTimer(int64_t time, Triggerable* target) {
    container_->registerTimer(time, target);
  }

  // 1.0 protected <S extends State> S getPartitionedState(StateDescriptor)（:259）
  //   -> getStateBackend().getPartitionedState(null, VoidSerializer.INSTANCE,
  //                                            stateDescriptor)
  // 返回类型取 descriptor 的第一个泛型参数 S（见 StateDescriptor.h）
  template <class SD>
  std::shared_ptr<typename SD::StateType> getPartitionedState(
      const SD& stateDescriptor) {
    return stateBackend_->getPartitionedState(
        flink::types::Any(),  // 1.0 传的是 null namespace
        flink::api::common::typeutils::base::VoidSerializer::INSTANCE(),
        stateDescriptor);
  }

  // 1.0 protected <S extends State, N> S getPartitionedState(
  //         N namespace, TypeSerializer<N>, StateDescriptor)（:270）
  template <class SD>
  std::shared_ptr<typename SD::StateType> getPartitionedState(
      const flink::types::Any& ns,
      AbstractStateBackend::SerializerPtr namespaceSerializer,
      const SD& stateDescriptor) {
    return stateBackend_->getPartitionedState(ns, namespaceSerializer,
                                              stateDescriptor);
  }

  // 1.0 的 protected 字段
  ChainingStrategy chainingStrategy_;
  Output* output_;

 private:
  bool inputCopyDisabled_;
  StreamTask* container_;
  StreamConfig* config_;
  KeySelectorPtr stateKeySelector1_;
  KeySelectorPtr stateKeySelector2_;
  StateBackendPtr stateBackend_;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_ABSTRACTSTREAMOPERATOR_H
