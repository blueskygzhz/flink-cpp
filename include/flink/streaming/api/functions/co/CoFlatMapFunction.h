/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
 *
 * 1.0 interface CoFlatMapFunction<IN1, IN2, OUT> extends Function, Serializable
 * （CoFlatMapFunction.java:35）：
 *   void flatMap1(IN1 value, Collector<OUT> out) throws Exception;
 *   void flatMap2(IN2 value, Collector<OUT> out) throws Exception;
 *
 * 1.0 类注释：「A CoFlatMapFunction implements a flat-map transformation over
 * two connected streams. The same instance of the transformation function is
 * used to transform both of the connected streams. That way the stream
 * transformations can share state.」
 *
 * 1.0 另有 RichCoFlatMapFunction（RichCoFlatMapFunction.java:33），同
 * CoMapFunction.h 的说明：C++ 直接多继承，不单独建类。
 */
#ifndef FLINK_STREAMING_API_FUNCTIONS_CO_COFLATMAPFUNCTION_H
#define FLINK_STREAMING_API_FUNCTIONS_CO_COFLATMAPFUNCTION_H

#include "flink/api/common/functions/Function.h"
#include "flink/util/Collector.h"

namespace flink {
namespace streaming {
namespace api {
namespace functions {
namespace co {

template <class T_IN1, class T_IN2, class T_OUT>
class CoFlatMapFunction : public flink::api::common::functions::Function {
 public:
  typedef T_IN1 IN1;
  typedef T_IN2 IN2;
  typedef T_OUT OUT;

  // 1.0: "This method is called for each element in the first of the connected
  // streams."
  virtual void flatMap1(const T_IN1& value,
                        flink::util::Collector<T_OUT>& out) = 0;

  // 1.0: "This method is called for each element in the second of the
  // connected streams."
  virtual void flatMap2(const T_IN2& value,
                        flink::util::Collector<T_OUT>& out) = 0;
};

}  // namespace co
}  // namespace functions
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_FUNCTIONS_CO_COFLATMAPFUNCTION_H
