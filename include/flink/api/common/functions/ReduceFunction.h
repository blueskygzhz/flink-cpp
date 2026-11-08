/*
 * flink-core : org.apache.flink.api.common.functions.ReduceFunction
 * 1.0 ReduceFunction.java:  T reduce(T value1, T value2)
 *   输入输出同类型；1.0 要求实现满足结合律，否则并行结果不确定。
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_REDUCEFUNCTION_H
#define FLINK_API_COMMON_FUNCTIONS_REDUCEFUNCTION_H

#include "flink/api/common/functions/Function.h"

namespace flink {
namespace api {
namespace common {
namespace functions {

template <class T>
class ReduceFunction : public Function {
 public:
  typedef T IN;
  typedef T OUT;

  virtual T reduce(const T& value1, const T& value2) = 0;
};

}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_REDUCEFUNCTION_H
