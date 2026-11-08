/*
 * flink-core : org.apache.flink.api.java.functions.KeySelector
 * 1.0 KeySelector.java:  KEY getKey(IN value)
 *   1.0 要求 KEY 实现 hashCode/equals（keyBy 的分区与状态分桶都依赖它）；
 *   C++ 这边对应要求 KEY 可用于 JavaHashCode<KEY> 与 std::hash<KEY>。
 */
#ifndef FLINK_API_JAVA_FUNCTIONS_KEYSELECTOR_H
#define FLINK_API_JAVA_FUNCTIONS_KEYSELECTOR_H

#include "flink/api/common/functions/Function.h"

namespace flink {
namespace api {
namespace java {
namespace functions {

template <class T_IN, class T_KEY>
class KeySelector : public flink::api::common::functions::Function {
 public:
  typedef T_IN IN;
  typedef T_KEY KEY;

  virtual T_KEY getKey(const T_IN& value) = 0;
};

}  // namespace functions
}  // namespace java
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_JAVA_FUNCTIONS_KEYSELECTOR_H
