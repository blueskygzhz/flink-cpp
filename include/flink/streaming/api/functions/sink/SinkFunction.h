/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.functions.sink.SinkFunction
 *
 * 1.0 interface SinkFunction<IN> extends Function, Serializable
 * （SinkFunction.java:29）：
 *   「Interface for implementing user defined sink functionality.」
 *   void invoke(IN value) throws Exception;   // "Function for standard sink
 *                                             //  behaviour. This function is
 *                                             //  called for every record."
 *
 * 1.0 另有 RichSinkFunction extends AbstractRichFunction implements
 * SinkFunction（RichSinkFunction.java:28），只是把两者拼起来不加方法 ——
 * C++ 直接多继承即可，不单独建类（同 CoMapFunction.h 的说明）。
 */
#ifndef FLINK_STREAMING_API_FUNCTIONS_SINK_SINKFUNCTION_H
#define FLINK_STREAMING_API_FUNCTIONS_SINK_SINKFUNCTION_H

#include "flink/api/common/functions/Function.h"

namespace flink {
namespace streaming {
namespace api {
namespace functions {
namespace sink {

template <class IN>
class SinkFunction : public flink::api::common::functions::Function {
 public:
  // 1.0: "Function for standard sink behaviour. This function is called for
  // every record."
  virtual void invoke(const IN& value) = 0;
};

}  // namespace sink
}  // namespace functions
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_FUNCTIONS_SINK_SINKFUNCTION_H
