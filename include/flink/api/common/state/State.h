/*
 * flink-core : org.apache.flink.api.common.state.State
 * 1.0 State.java:  void clear()
 *   清空「当前 key」（keyed state 的作用域）下的状态。
 */
#ifndef FLINK_API_COMMON_STATE_STATE_H
#define FLINK_API_COMMON_STATE_STATE_H

namespace flink {
namespace api {
namespace common {
namespace state {

class State {
 public:
  virtual ~State() {}
  virtual void clear() = 0;
};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_STATE_H
