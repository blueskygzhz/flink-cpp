/*
 * flink-core : org.apache.flink.api.common.state.MergingState
 * 1.0 MergingState<IN, OUT> extends State:
 *   OUT get()
 *   void add(IN value)
 */
#ifndef FLINK_API_COMMON_STATE_MERGINGSTATE_H
#define FLINK_API_COMMON_STATE_MERGINGSTATE_H

#include "flink/api/common/state/State.h"

namespace flink {
namespace api {
namespace common {
namespace state {

template <class IN, class OUT>
class MergingState : public State {
 public:
  virtual OUT get() = 0;
  virtual void add(const IN& value) = 0;
};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_MERGINGSTATE_H
