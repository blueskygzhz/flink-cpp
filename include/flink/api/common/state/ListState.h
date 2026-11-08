/*
 * flink-core : org.apache.flink.api.common.state.ListState
 * 1.0 ListState.java:36  interface ListState<T> extends MergingState<T, Iterable<T>>
 *   Iterable<T> 在 C++ 里用 std::vector<T> 表达。
 */
#ifndef FLINK_API_COMMON_STATE_LISTSTATE_H
#define FLINK_API_COMMON_STATE_LISTSTATE_H

#include <vector>

#include "flink/api/common/state/MergingState.h"

namespace flink {
namespace api {
namespace common {
namespace state {

template <class T>
class ListState : public MergingState<T, std::vector<T> > {};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_LISTSTATE_H
