/*
 * flink-core : org.apache.flink.api.common.functions.RichFunction
 *
 * 1.0 RichFunction.java:
 *   void open(Configuration parameters)
 *   void close()
 *   RuntimeContext getRuntimeContext()
 *   IterationRuntimeContext getIterationRuntimeContext()
 *   void setRuntimeContext(RuntimeContext t)
 *
 * open() 的 Configuration 参数去掉了：1.0 AbstractUdfStreamOperator.java:91
 * 传进去的永远是 `new Configuration()`（空的），流式 API 根本没有途径往里塞东西。
 * getIterationRuntimeContext() 属于 DataSet 迭代，流式用不到。
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_RICHFUNCTION_H
#define FLINK_API_COMMON_FUNCTIONS_RICHFUNCTION_H

#include "flink/api/common/functions/Function.h"
#include "flink/api/common/functions/RuntimeContext.h"

namespace flink {
namespace api {
namespace common {
namespace functions {

class RichFunction : public Function {
 public:
  virtual void open() = 0;
  virtual void close() = 0;
  virtual void setRuntimeContext(const RuntimeContext* context) = 0;
  virtual const RuntimeContext& getRuntimeContext() const = 0;
};

}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_RICHFUNCTION_H
