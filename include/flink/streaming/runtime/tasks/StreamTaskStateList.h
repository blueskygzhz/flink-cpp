/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.tasks.StreamTaskStateList
 *
 * 1.0 StreamTaskStateList implements StateHandle<StreamTaskState[]>：
 *   StreamTaskStateList(StreamTaskState[] states)
 *   StreamTaskState[] getState(ClassLoader)
 *   void discardState()
 *   long getStateSize()
 *   boolean isEmpty()     // 数组里全是 null 才算空
 *
 * 一个算子链上每个算子一份 StreamTaskState，按链上顺序排。StreamTask 把它整体
 * acknowledge 给 JobManager，恢复时再按同样的下标派回各算子
 * （StreamTask.restoreState()，StreamTask.java:429）。
 *
 * 【偏差】1.0 里数组元素可以是 null（表示该算子无状态），C++ 用
 * shared_ptr<StreamTaskState> 的空指针表达，保持「下标与算子一一对应」这个
 * 关键性质不变。
 */
#ifndef FLINK_STREAMING_RUNTIME_TASKS_STREAMTASKSTATELIST_H
#define FLINK_STREAMING_RUNTIME_TASKS_STREAMTASKSTATELIST_H

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "flink/streaming/runtime/tasks/StreamTaskState.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

class StreamTaskStateList {
 public:
  typedef std::shared_ptr<StreamTaskState> StreamTaskStatePtr;

  StreamTaskStateList() {}
  explicit StreamTaskStateList(std::vector<StreamTaskStatePtr> states)
      : states_(std::move(states)) {}

  // 1.0 getState(ClassLoader)：C++ 没有类加载器，去掉参数
  const std::vector<StreamTaskStatePtr>& getState() const { return states_; }

  // 1.0 isEmpty()：只要有一个非 null 元素就不算空
  bool isEmpty() const {
    for (const auto& state : states_) {
      if (state) return false;
    }
    return true;
  }

  // 1.0 getStateSize()：累加各算子状态的字节数
  size_t getStateSize() const {
    size_t size = 0;
    for (const auto& state : states_) {
      if (!state) continue;
      if (state->getOperatorState()) {
        size += state->getOperatorState()->getStateSize();
      }
      if (state->getFunctionState()) {
        size += state->getFunctionState()->getStateSize();
      }
      for (const auto& kv : state->getKvStates()) {
        size += kv.second.getStateSize();
      }
    }
    return size;
  }

  // 1.0 discardState()
  void discardState() {
    for (auto& state : states_) {
      if (state) state->discardState();
    }
    states_.clear();
  }

 private:
  std::vector<StreamTaskStatePtr> states_;
};

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_TASKS_STREAMTASKSTATELIST_H
