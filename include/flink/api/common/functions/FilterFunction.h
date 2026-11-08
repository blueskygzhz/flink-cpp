/*
 * flink-core : org.apache.flink.api.common.functions.FilterFunction
 * 1.0 FilterFunction.java:  boolean filter(T value)
 *   true = 保留该记录，false = 丢弃
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_FILTERFUNCTION_H
#define FLINK_API_COMMON_FUNCTIONS_FILTERFUNCTION_H

#include "flink/api/common/functions/Function.h"

namespace flink {
namespace api {
namespace common {
namespace functions {

template <class T>
class FilterFunction : public Function {
 public:
  typedef T IN;
  typedef T OUT;

  virtual bool filter(const T& value) = 0;
};

}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_FILTERFUNCTION_H
