/*
 * flink-core : org.apache.flink.api.common.functions.FoldFunction
 * 1.0 FoldFunction<O, T>.java:  T fold(T accumulator, O value)
 *   O = 输入元素类型，T = 累加器/输出类型。
 * 这里模板参数写成 <T_IN, T_OUT>（= <O, T>），顺序与 1.0 一致，只是换了名字，
 * 以便和其它 Function 的 IN/OUT 约定统一。
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_FOLDFUNCTION_H
#define FLINK_API_COMMON_FUNCTIONS_FOLDFUNCTION_H

#include "flink/api/common/functions/Function.h"

namespace flink {
namespace api {
namespace common {
namespace functions {

template <class T_IN, class T_OUT>
class FoldFunction : public Function {
 public:
  typedef T_IN IN;
  typedef T_OUT OUT;

  virtual T_OUT fold(const T_OUT& accumulator, const T_IN& value) = 0;
};

}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_FOLDFUNCTION_H
