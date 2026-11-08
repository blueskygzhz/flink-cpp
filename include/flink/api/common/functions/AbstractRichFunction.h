/*
 * flink-core : org.apache.flink.api.common.functions.AbstractRichFunction
 *
 * 1.0 AbstractRichFunction.java:
 *   持有 transient RuntimeContext runtimeContext；
 *   open()/close() 默认空实现；
 *   getRuntimeContext() 在未 setup 时抛 IllegalStateException
 *     ("The runtime context has not been initialized.")
 *
 * runtimeContext 用裸指针持有：它的实际对象是算子内的 StreamingRuntimeContext，
 * 生命周期覆盖 UDF（1.0 里同样是算子持有、UDF 只拿引用）。
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_ABSTRACTRICHFUNCTION_H
#define FLINK_API_COMMON_FUNCTIONS_ABSTRACTRICHFUNCTION_H

#include <stdexcept>

#include "flink/api/common/functions/RichFunction.h"

namespace flink {
namespace api {
namespace common {
namespace functions {

class AbstractRichFunction : public RichFunction {
 public:
  AbstractRichFunction() : runtimeContext_(NULL) {}

  void open() override {}
  void close() override {}

  void setRuntimeContext(const RuntimeContext* context) override {
    runtimeContext_ = context;
  }

  const RuntimeContext& getRuntimeContext() const override {
    if (runtimeContext_ == NULL) {
      throw std::logic_error("The runtime context has not been initialized.");
    }
    return *runtimeContext_;
  }

 private:
  const RuntimeContext* runtimeContext_;
};

}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_ABSTRACTRICHFUNCTION_H
