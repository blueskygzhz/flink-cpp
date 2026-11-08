/*
 * flink-core : org.apache.flink.api.common.functions.util.FunctionUtils
 *
 * 1.0 FunctionUtils.java:
 *   openFunction(Function, Configuration)     -> if (f instanceof RichFunction) open
 *   closeFunction(Function)                   -> if (f instanceof RichFunction) close
 *   setFunctionRuntimeContext(Function, RuntimeContext)
 * C++ 用 dynamic_cast 复刻 instanceof 分支：UDF 没实现 RichFunction 就什么都不做。
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_UTIL_FUNCTIONUTILS_H
#define FLINK_API_COMMON_FUNCTIONS_UTIL_FUNCTIONUTILS_H

#include <memory>

#include "flink/api/common/functions/Function.h"
#include "flink/api/common/functions/RichFunction.h"
#include "flink/api/common/functions/RuntimeContext.h"

namespace flink {
namespace api {
namespace common {
namespace functions {
namespace util {

class FunctionUtils {
 public:
  template <class F>
  static void openFunction(const std::shared_ptr<F>& function) {
    RichFunction* rich = dynamic_cast<RichFunction*>(function.get());
    if (rich != NULL) rich->open();
  }

  template <class F>
  static void closeFunction(const std::shared_ptr<F>& function) {
    RichFunction* rich = dynamic_cast<RichFunction*>(function.get());
    if (rich != NULL) rich->close();
  }

  template <class F>
  static void setFunctionRuntimeContext(const std::shared_ptr<F>& function,
                                        const RuntimeContext* context) {
    RichFunction* rich = dynamic_cast<RichFunction*>(function.get());
    if (rich != NULL) rich->setRuntimeContext(context);
  }

 private:
  FunctionUtils() {}
};

}  // namespace util
}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_UTIL_FUNCTIONUTILS_H
