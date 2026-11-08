/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.operators.OneInputStreamOperator
 *
 * 1.0 interface OneInputStreamOperator<IN, OUT> extends StreamOperator<OUT>:
 *   void processElement(StreamRecord<IN> element) throws Exception;
 *   void processWatermark(Watermark mark) throws Exception;
 *
 * 两个方法都带 1.0 的同一条保证：
 *   「guaranteed to not be called concurrently with other methods of the
 *     operator」—— 由 StreamTask 的单线程 + checkpointLock 提供。
 *
 * 【偏差】1.0 的 <IN, OUT> 在本实现里不出现在接口上：StreamRecord 的值是 Any
 * （见 StreamRecord.h），IN/OUT 由具体算子自己的模板参数在编译期保证。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_ONEINPUTSTREAMOPERATOR_H
#define FLINK_STREAMING_API_OPERATORS_ONEINPUTSTREAMOPERATOR_H

#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/api/watermark/Watermark.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

// 1.0 是 interface OneInputStreamOperator<IN,OUT> extends StreamOperator<OUT>，
// 具体算子同时 extends AbstractUdfStreamOperator 并 implements 本接口。C++ 这边
// 两条继承线都要经过 StreamOperator，所以用虚继承让 Java「类 + 接口」的菱形
// 收敛成一个基类子对象。
class OneInputStreamOperator : public virtual StreamOperator {
 public:
  virtual void processElement(const StreamRecord& element) = 0;
  virtual void processWatermark(
      const flink::streaming::api::watermark::Watermark& mark) = 0;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_ONEINPUTSTREAMOPERATOR_H
