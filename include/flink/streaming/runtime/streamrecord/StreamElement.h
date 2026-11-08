/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.streamrecord.StreamElement
 *
 * 1.0 StreamElement.java:28-61 —— 数据通道里流动的元素的抽象基类，只有两种子类：
 *   StreamRecord<T>（带值、可带时间戳）与 Watermark。
 *   isRecord() / isWatermark() / asRecord() / asWatermark()
 *
 * 注意：CheckpointBarrier 与 EndOfPartitionEvent **不是** StreamElement，
 * 它们是 AbstractEvent，与数据缓冲并行地在同一条通道上传输
 * （见 runtime/io/network/partition/consumer/BufferOrEvent.h）。
 *
 * asRecord()/asWatermark() 需要派生类的完整定义，C++ 里无法在基类中声明，
 * 故统一提供模板 as<T>()（dynamic_cast 版本），语义与 1.0 的两个方法一致。
 */
#ifndef FLINK_STREAMING_RUNTIME_STREAMRECORD_STREAMELEMENT_H
#define FLINK_STREAMING_RUNTIME_STREAMRECORD_STREAMELEMENT_H

#include <stdexcept>
#include <string>
#include <typeinfo>

namespace flink {
namespace streaming {
namespace runtime {
namespace streamrecord {

class StreamElement {
 public:
  virtual ~StreamElement() {}

  virtual bool isRecord() const = 0;
  virtual bool isWatermark() const = 0;

  // 对应 1.0 的 asRecord() / asWatermark()，类型不符时抛异常（Java 是 ClassCastException）
  template <class E>
  const E& as() const {
    const E* casted = dynamic_cast<const E*>(this);
    if (casted == NULL) {
      throw std::runtime_error(std::string("StreamElement is not a ") +
                               typeid(E).name());
    }
    return *casted;
  }
};

}  // namespace streamrecord
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_STREAMRECORD_STREAMELEMENT_H
