/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.functions.co.CoMapFunction
 *
 * 1.0 interface CoMapFunction<IN1, IN2, OUT> extends Function, Serializable
 * （CoMapFunction.java:33）：
 *   OUT map1(IN1 value) throws Exception;    // "the first input"
 *   OUT map2(IN2 value) throws Exception;    // "the second input"
 *
 * 1.0 类注释：「A CoFlatMapFunction implements a map() transformation over two
 * connected streams. The same instance of the transformation function is used
 * to transform both of the connected streams. That way the stream
 * transformations can share state.」
 *
 * 1.0 另有 RichCoMapFunction extends AbstractRichFunction implements
 * CoMapFunction（RichCoMapFunction.java:33），它只是把 AbstractRichFunction 和
 * 本接口拼起来，不加任何方法 —— C++ 直接多继承 AbstractRichFunction +
 * CoMapFunction 即可，所以不单独建类（同 MapFunction/RichMapFunction 的处理）。
 */
#ifndef FLINK_STREAMING_API_FUNCTIONS_CO_COMAPFUNCTION_H
#define FLINK_STREAMING_API_FUNCTIONS_CO_COMAPFUNCTION_H

#include "flink/api/common/functions/Function.h"

namespace flink {
namespace streaming {
namespace api {
namespace functions {
namespace co {

template <class T_IN1, class T_IN2, class T_OUT>
class CoMapFunction : public flink::api::common::functions::Function {
 public:
  typedef T_IN1 IN1;
  typedef T_IN2 IN2;
  typedef T_OUT OUT;

  // 1.0: "This method is called for each element in the first of the connected
  // streams."
  virtual T_OUT map1(const T_IN1& value) = 0;

  // 1.0: "This method is called for each element in the second of the connected
  // streams."
  virtual T_OUT map2(const T_IN2& value) = 0;
};

}  // namespace co
}  // namespace functions
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_FUNCTIONS_CO_COMAPFUNCTION_H
