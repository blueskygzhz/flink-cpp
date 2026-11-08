/*
 * flink-runtime : org.apache.flink.runtime.state.StateHandle
 *
 * 1.0 StateHandle<T>:  T getState(ClassLoader) / discardState()
 * 1.0 里句柄可以指向堆内存、文件系统或分布式存储；本实现只做内存态
 * （对应 1.0 的 ByteStreamStateHandle / LocalStateHandle 一族），
 * 载荷是 KvStateSnapshot 序列化后的字节。
 */
#ifndef FLINK_RUNTIME_STATE_STATEHANDLE_H
#define FLINK_RUNTIME_STATE_STATEHANDLE_H

#include <string>
#include <utility>

namespace flink {
namespace runtime {
namespace state {

class StateHandle {
 public:
  StateHandle() {}
  explicit StateHandle(std::string state) : state_(std::move(state)) {}

  // 1.0 getState(ClassLoader userCodeClassLoader)：C++ 没有类加载器，去掉参数
  const std::string& getState() const { return state_; }
  size_t getStateSize() const { return state_.size(); }

  // 1.0 discardState()：丢弃该句柄占用的存储
  void discardState() { state_.clear(); }

 private:
  std::string state_;
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_STATEHANDLE_H
