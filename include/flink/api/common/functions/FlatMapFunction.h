/*
 * flink-core : org.apache.flink.api.common.functions.FlatMapFunction
 * 1.0 FlatMapFunction.java:  void flatMap(IN value, Collector<OUT> out)
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_FLATMAPFUNCTION_H
#define FLINK_API_COMMON_FUNCTIONS_FLATMAPFUNCTION_H

#include "flink/api/common/functions/Function.h"
#include "flink/util/Collector.h"

namespace flink {
namespace api {
namespace common {
namespace functions {

template <class T_IN, class T_OUT>
class FlatMapFunction : public Function {
 public:
  typedef T_IN IN;
  typedef T_OUT OUT;

  virtual void flatMap(const T_IN& value,
                       flink::util::Collector<T_OUT>& out) = 0;
};

}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_FLATMAPFUNCTION_H
