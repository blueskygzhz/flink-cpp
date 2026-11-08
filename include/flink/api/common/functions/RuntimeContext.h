/*
 * flink-core : org.apache.flink.api.common.functions.RuntimeContext
 *
 * 1.0 RuntimeContext.java 共 20+ 个方法：任务信息、累加器、广播变量、
 * distributed cache、keyed state 等。这里保留运行时真正能提供的部分：
 *   String getTaskName()
 *   int getNumberOfParallelSubtasks()
 *   int getIndexOfThisSubtask()
 *   ExecutionConfig getExecutionConfig()
 * 省略项（accumulator / 广播变量 / distributed cache）都依赖 JobManager 侧设施，
 * 本实现没有，故不提供空壳。
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_RUNTIMECONTEXT_H
#define FLINK_API_COMMON_FUNCTIONS_RUNTIMECONTEXT_H

#include <string>

#include "flink/api/common/ExecutionConfig.h"

namespace flink {
namespace api {
namespace common {
namespace functions {

class RuntimeContext {
 public:
  virtual ~RuntimeContext() {}

  virtual std::string getTaskName() const = 0;
  virtual int getNumberOfParallelSubtasks() const = 0;
  virtual int getIndexOfThisSubtask() const = 0;
  virtual const ExecutionConfig& getExecutionConfig() const = 0;
};

}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_RUNTIMECONTEXT_H
