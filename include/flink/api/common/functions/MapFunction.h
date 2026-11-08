/*
 * flink-core : org.apache.flink.api.common.functions.MapFunction
 * 1.0 MapFunction.java:  OUT map(IN value) throws Exception
 *
 * IN / OUT 两个 typedef 是 C++ 侧的必需品：1.0 由 TypeExtractor 从泛型签名反射出
 * 输入输出类型，这里改由算子/DataStream 在编译期读 F::IN、F::OUT。
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_MAPFUNCTION_H
#define FLINK_API_COMMON_FUNCTIONS_MAPFUNCTION_H

#include "flink/api/common/functions/Function.h"

namespace flink {
namespace api {
namespace common {
namespace functions {

template <class T_IN, class T_OUT>
class MapFunction : public Function {
 public:
  typedef T_IN IN;
  typedef T_OUT OUT;

  virtual T_OUT map(const T_IN& value) = 0;
};

}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_MAPFUNCTION_H
