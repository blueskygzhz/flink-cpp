/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.partitioner.HashPartitioner
 *
 * 1.0 HashPartitioner.java：
 *   private int[] returnArray = new int[1];
 *   KeySelector<T, ?> keySelector;
 *   HashPartitioner(KeySelector<T, ?> keySelector) { ... }
 *   int[] selectChannels(record, n) {
 *       Object key;
 *       try {
 *           key = keySelector.getKey(record.getInstance().getValue());
 *       } catch (Exception e) {
 *           throw new RuntimeException(
 *               "Could not extract key from " + record.getInstance().getValue(), e);
 *       }
 *       returnArray[0] = MathUtils.murmurHash(key.hashCode()) % n;
 *       return returnArray;
 *   }
 *   StreamPartitioner<T> copy() { return this; }
 *   String toString() { return "HASH"; }
 *
 * 1.0 原注释：「Partitioner selects the target channel based on the hash value
 * of a key from a KeySelector.」keyBy() 生成的就是它。
 *
 * MathUtils.murmurHash 保证返回非负（见 MathUtils.cpp:32-35 对 1.0
 * MathUtils.java:164-167 的复刻），所以 `% n` 不会出负数。
 * 注意这与 keyed state 的分桶（KvState 按 key 的 hashCode 查表）是两件事：
 * 这里决定「记录发给下游哪个 subtask」，那里决定「在 subtask 内查哪个状态」。
 * 两边都用同一个 Any::hashCode()，才保证同 key 落到同 subtask 的同一份状态上。
 *
 * 【偏差】KeySelector<T,?> -> AnyKeySelector（类型擦除版，见 AnyKeySelector.h）。
 * 1.0 的 `record.getInstance().getValue()` 在这里是
 * `record->as<StreamRecord>().getValue()`。
 */
#ifndef FLINK_STREAMING_RUNTIME_PARTITIONER_HASHPARTITIONER_H
#define FLINK_STREAMING_RUNTIME_PARTITIONER_HASHPARTITIONER_H

#include <memory>
#include <stdexcept>

#include "flink/api/java/functions/AnyKeySelector.h"
#include "flink/runtime/util/MathUtils.h"
#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"
#include "flink/streaming/runtime/streamrecord/StreamRecord.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace partitioner {

class HashPartitioner : public StreamPartitioner {
 public:
  typedef std::shared_ptr<flink::api::java::functions::AnyKeySelector>
      KeySelectorPtr;

  explicit HashPartitioner(const KeySelectorPtr& keySelector)
      : returnArray_(1, 0), keySelector_(keySelector) {}

  const std::vector<int>& selectChannels(
      const RecordPtr& record, int numberOfOutputChannels) override {
    flink::types::Any key;
    try {
      key = keySelector_->getKey(
          record->as<flink::streaming::runtime::streamrecord::StreamRecord>()
              .getValue());
    } catch (const std::exception& e) {
      // 1.0: throw new RuntimeException("Could not extract key from " + value, e)
      throw std::runtime_error(std::string("Could not extract key: ") +
                               e.what());
    }
    returnArray_[0] =
        flink::runtime::util::MathUtils::murmurHash(key.hashCode()) %
        numberOfOutputChannels;
    return returnArray_;
  }

  // 1.0: return this
  std::shared_ptr<StreamPartitioner> copy() override {
    return shared_from_this();
  }

  std::string toString() const override { return "HASH"; }

 private:
  std::vector<int> returnArray_;
  KeySelectorPtr keySelector_;
};

}  // namespace partitioner
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_PARTITIONER_HASHPARTITIONER_H
