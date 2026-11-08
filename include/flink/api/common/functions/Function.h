/*
 * flink-core : org.apache.flink.api.common.functions.Function
 *
 * 1.0 Function.java 是空的标记接口（extends java.io.Serializable）：
 * 所有用户函数的根类型，FunctionUtils 靠它 + instanceof 判断是否 RichFunction。
 * C++ 这边保留它作为公共基类，才能对任意 UDF 做 dynamic_cast。
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_FUNCTION_H
#define FLINK_API_COMMON_FUNCTIONS_FUNCTION_H

namespace flink {
namespace api {
namespace common {
namespace functions {

class Function {
 public:
  virtual ~Function() {}
};

}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_FUNCTION_H
