/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper
 *
 * 1.0 CustomPartitionerWrapper<K, T> extends StreamPartitioner<T>：
 *   private int[] returnArray = new int[1];
 *   Partitioner<K> partitioner;
 *   KeySelector<T, K> keySelector;
 *   int[] selectChannels(record, n) {
 *       K key = null;
 *       try { key = keySelector.getKey(record.getInstance().getValue()); }
 *       catch (Exception e) {
 *           throw new RuntimeException(
 *               "Could not extract key from " + record.getInstance(), e);
 *       }
 *       returnArray[0] = partitioner.partition(key, n);
 *       return returnArray;
 *   }
 *   StreamPartitioner<T> copy() { return this; }
 *   String toString() { return "CUSTOM"; }
 *
 * 1.0 原注释：「Partitioner that selects the channel with a user defined
 * partitioner function on a key.」DataStream.partitionCustom() 生成的就是它。
 *
 * 注意 1.0 这里**不做 murmurHash、不取模**——分区号完全由用户的
 * Partitioner.partition 决定（所以用户返回越界值就会越界，这是 1.0 的行为）。
 *
 * 【偏差】1.0 的两个泛型参数 <K, T>：T 是数据类型、K 是 key 类型。
 * 本实现的 T 已被 Any 擦除（见 ChannelSelector.h 的偏差 1），只保留 K——
 * 因为 Partitioner<K>::partition 需要 K 的真实类型。KeySelector<T,K> 则走
 * 类型擦除版 AnyKeySelector，取出的 Any 在这里 as<K>() 还原。
 */
#ifndef FLINK_STREAMING_RUNTIME_PARTITIONER_CUSTOMPARTITIONERWRAPPER_H
#define FLINK_STREAMING_RUNTIME_PARTITIONER_CUSTOMPARTITIONERWRAPPER_H

#include <memory>
#include <stdexcept>

#include "flink/api/common/functions/Partitioner.h"
#include "flink/api/java/functions/AnyKeySelector.h"
#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"
#include "flink/streaming/runtime/streamrecord/StreamRecord.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace partitioner {

template <class K>
class CustomPartitionerWrapper : public StreamPartitioner {
 public:
  typedef std::shared_ptr<flink::api::common::functions::Partitioner<K> >
      PartitionerPtr;
  typedef std::shared_ptr<flink::api::java::functions::AnyKeySelector>
      KeySelectorPtr;

  CustomPartitionerWrapper(const PartitionerPtr& partitioner,
                           const KeySelectorPtr& keySelector)
      : returnArray_(1, 0),
        partitioner_(partitioner),
        keySelector_(keySelector) {}

  const std::vector<int>& selectChannels(
      const RecordPtr& record, int numberOfOutputChannels) override {
    flink::types::Any key;
    try {
      key = keySelector_->getKey(
          record->as<flink::streaming::runtime::streamrecord::StreamRecord>()
              .getValue());
    } catch (const std::exception& e) {
      // 1.0: throw new RuntimeException("Could not extract key from " + ..., e)
      throw std::runtime_error(std::string("Could not extract key: ") +
                               e.what());
    }
    // 1.0: returnArray[0] = partitioner.partition(key, n) —— 不取模
    returnArray_[0] =
        partitioner_->partition(key.template as<K>(), numberOfOutputChannels);
    return returnArray_;
  }

  // 1.0: return this
  std::shared_ptr<StreamPartitioner> copy() override {
    return shared_from_this();
  }

  std::string toString() const override { return "CUSTOM"; }

 private:
  std::vector<int> returnArray_;
  PartitionerPtr partitioner_;
  KeySelectorPtr keySelector_;
};

}  // namespace partitioner
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_PARTITIONER_CUSTOMPARTITIONERWRAPPER_H
