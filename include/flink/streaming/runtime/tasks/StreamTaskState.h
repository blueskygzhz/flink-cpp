/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.tasks.StreamTaskState
 *
 * 1.0 StreamTaskState implements Serializable（StreamTaskState.java:40）：
 *   private StateHandle<?> operatorState;              // 算子自身的状态
 *   private StateHandle<Serializable> functionState;   // UDF 的 Checkpointed 状态
 *   private HashMap<String, KvStateSnapshot<?,?,?,?,?>> kvStates;  // keyed state
 *   get/set 三件套 + boolean isEmpty() + void discardState()
 *
 * 一个算子一份；StreamTask 把算子链上每个算子的 StreamTaskState 汇总成
 * StreamTaskStateList 交给 CheckpointCoordinator。
 *
 * 【偏差】kvStates 的值类型由 KvStateSnapshot 换成 StateHandle（见
 * AbstractStateBackend.h 偏差 3）。Java 里 null 表示「没有这部分状态」，C++ 这边
 * 用 shared_ptr 的空指针表达。
 */
#ifndef FLINK_STREAMING_RUNTIME_TASKS_STREAMTASKSTATE_H
#define FLINK_STREAMING_RUNTIME_TASKS_STREAMTASKSTATE_H

#include <memory>
#include <string>
#include <unordered_map>

#include "flink/runtime/state/StateHandle.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

class StreamTaskState {
 public:
  typedef std::shared_ptr<flink::runtime::state::StateHandle> StateHandlePtr;
  typedef std::unordered_map<std::string, flink::runtime::state::StateHandle>
      KvStateMap;

  // 1.0 getOperatorState() / setOperatorState(StateHandle<?>)
  const StateHandlePtr& getOperatorState() const { return operatorState_; }
  void setOperatorState(StateHandlePtr operatorState) {
    operatorState_ = operatorState;
  }

  // 1.0 getFunctionState() / setFunctionState(StateHandle<Serializable>)
  const StateHandlePtr& getFunctionState() const { return functionState_; }
  void setFunctionState(StateHandlePtr functionState) {
    functionState_ = functionState;
  }

  // 1.0 getKvStates() / setKvStates(HashMap<String, KvStateSnapshot<...>>)
  const KvStateMap& getKvStates() const { return kvStates_; }
  void setKvStates(const KvStateMap& kvStates) { kvStates_ = kvStates; }

  // 1.0 isEmpty()：三部分都没有才算空
  bool isEmpty() const {
    return !operatorState_ && !functionState_ && kvStates_.empty();
  }

  // 1.0 discardState()：逐个丢弃，然后把引用清掉
  void discardState() {
    if (operatorState_) operatorState_->discardState();
    if (functionState_) functionState_->discardState();
    for (auto& entry : kvStates_) {
      entry.second.discardState();
    }
    operatorState_.reset();
    functionState_.reset();
    kvStates_.clear();
  }

 private:
  StateHandlePtr operatorState_;
  StateHandlePtr functionState_;
  KvStateMap kvStates_;
};

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_TASKS_STREAMTASKSTATE_H
