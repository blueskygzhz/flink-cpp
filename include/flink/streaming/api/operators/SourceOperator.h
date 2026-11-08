/*
 * 【与 Flink 1.0 的偏差】不带模板参数的「源算子」接口。
 *
 * 1.0 的 SourceStreamTask 签名是
 *   class SourceStreamTask<OUT, SRC extends SourceFunction<OUT>,
 *                          OP extends StreamSource<OUT, SRC>>
 *       extends StreamTask<OUT, OP>
 * （SourceStreamTask.java:40）—— 靠**泛型上界** OP extends StreamSource<...>
 * 让 run()/cancelTask() 能直接写 `headOperator.run(...)` /
 * `headOperator.cancel()`。
 *
 * C++ 的模板参数没有「上界」这回事，而 StreamTask::headOperator_ 是
 * StreamOperator*（见 StreamOperator.h 的类型擦除偏差），所以把 1.0 在
 * StreamSource 上的这两个方法抽成一个不带模板参数的接口：
 *   StreamSource<OUT, SRC> 实现它，SourceStreamTask 用 dynamic_cast 取到它。
 * 这与 AnySerializer / AnyKeySelector / AnyOutputSelector 是同一手法，
 * 只是这里擦掉的是「算子的元素类型」而不是「值的类型」。
 *
 * 对应 Java：StreamSource.run(Object, Output) / StreamSource.cancel()
 * （StreamSource.java:55 / :92）。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_SOURCEOPERATOR_H
#define FLINK_STREAMING_API_OPERATORS_SOURCEOPERATOR_H

#include <mutex>

#include "flink/streaming/api/operators/Output.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

class SourceOperator {
 public:
  virtual ~SourceOperator() {}

  // 1.0 StreamSource.run(final Object lockingObject,
  //                      final Output<StreamRecord<OUT>> collector)
  virtual void run(std::recursive_mutex& lockingObject, Output* collector) = 0;

  // 1.0 StreamSource.cancel()
  virtual void cancel() = 0;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_SOURCEOPERATOR_H
