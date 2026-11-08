/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.operators.TwoInputStreamOperator
 *
 * 1.0 interface TwoInputStreamOperator<IN1, IN2, OUT> extends StreamOperator<OUT>
 * （TwoInputStreamOperator.java:35）：
 *   void processElement1(StreamRecord<IN1> element) throws Exception;   (:41)
 *   void processElement2(StreamRecord<IN2> element) throws Exception;   (:47)
 *   void processWatermark1(Watermark mark) throws Exception;            (:55)
 *   void processWatermark2(Watermark mark) throws Exception;            (:63)
 *
 * 四个方法都带 1.0 的同一条保证（:39/:45/:51/:59）：
 *   「This method is guaranteed to not be called concurrently with other
 *     methods of the operator」—— 由 StreamTask 的单线程 + checkpointLock 提供。
 *
 * 注意 1.0 这里是**两条独立的水位线**：StreamTwoInputProcessor 只负责把各自
 * 输入的多个通道取 min（StreamTwoInputProcessor.java:241-274），两条输入之间
 * 再取 min 是**算子的责任**（见 CoStreamMap.processWatermark1/2 里的
 * combinedWatermark）。这与单输入的 OneInputStreamOperator.processWatermark
 * 只有一条线不同。
 *
 * 【偏差】1.0 的 <IN1, IN2, OUT> 在本实现里不出现在接口上：StreamRecord 的值
 * 是 Any（见 StreamRecord.h），IN1/IN2/OUT 由具体算子自己的模板参数在编译期
 * 保证 —— 与 OneInputStreamOperator.h 同一手法。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_TWOINPUTSTREAMOPERATOR_H
#define FLINK_STREAMING_API_OPERATORS_TWOINPUTSTREAMOPERATOR_H

#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/api/watermark/Watermark.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

// 1.0 是 interface TwoInputStreamOperator<IN1,IN2,OUT> extends
// StreamOperator<OUT>，具体算子同时 extends AbstractUdfStreamOperator 并
// implements 本接口。C++ 这边两条继承线都要经过 StreamOperator，所以用虚继承
// 让 Java「类 + 接口」的菱形收敛成一个基类子对象（同 OneInputStreamOperator）。
class TwoInputStreamOperator : public virtual StreamOperator {
 public:
  typedef flink::streaming::api::watermark::Watermark Watermark;

  virtual void processElement1(const StreamRecord& element) = 0;
  virtual void processElement2(const StreamRecord& element) = 0;
  virtual void processWatermark1(const Watermark& mark) = 0;
  virtual void processWatermark2(const Watermark& mark) = 0;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_TWOINPUTSTREAMOPERATOR_H
